"""Clock-free recording for the Kalshi speed question. Places no orders.

    python kalshi_recorder.py

kalshi_ms.py found a real edge that dies within ~100 ms — but on BINANCE's
clock against KALSHI's clock, which are not synchronised, and from an exchange
a US bot hears ~150 ms late. This records what a US bot actually sees, with
every row stamped by ONE clock: this machine's, at receipt.

    spot     Coinbase (every match), Kraken and Bitstamp trades — US venues inside
             CF Benchmarks' BRTI, which KXBTC15M settles on
    book     Kalshi top of book for the live market, polled every 100 ms on a
             kept-alive connection; send and receive times both stored, so the
             replay knows the window in which each snapshot was true
    trades   Kalshi's public trade tape for the live market (exchange stamps kept
             beside ours, which measures the clock offset rather than assuming it)
    markets  ticker, window, strike, and result once settled

kalshi_live_replay.py then replays the trigger at our own measured latency.
Rate budget: book 10/s + tape 2/s + the paper bot's 2/s stays under Kalshi's
public read limit; a 429 backs off instead of hammering.
"""

import asyncio
import calendar
import json
import sqlite3
import sys
import time
from pathlib import Path

import kalshi_client as kc
from spot_feeds import Book

DB = Path(__file__).with_name("kalshi_live.sqlite")
BOOK_EVERY = 0.100
TAPE_EVERY = 0.500
# ETH is recorded, not traded: KXETH15M settles on the same structure (CF ETHUSDRTI, 60 s
# average at both ends) with ~6% of BTC's volume (2026-09-13). Recording it now means a later
# test does not wait weeks for data. ETH spot rows use src "coinbase:ETH" etc.; BTC rows keep
# their original names so every existing reader (replay, bot sigma) is unchanged.
SERIES = {"KXBTC15M": "market", "KXETH15M": "eth_market"}
# the EC2 server (28 GB disk) measured 2.8 GB/day before the leading feeds were added (1.8 GB in
# 15.4 h, mostly Kalshi book rows for BTC + ETH). 10 days would overflow the disk; 5 days fits.
RETAIN_DAYS = 5
# Leading BTC feeds (2026-09-14): perpetual futures, where price discovery happens first.
# live_latency_curve.py found the US spot books arrive too late to beat Kalshi's traders; the only
# lever left is earlier information. Stored as spot rows (src below), price = top-of-book mid,
# one row per mid change (~5/s per venue). NOTE: Binance's terms restrict US persons; this reads
# public market data only.

SCHEMA = """
CREATE TABLE IF NOT EXISTS spot (recv_ns INTEGER, src TEXT, exch_us INTEGER, price REAL, size REAL);
CREATE TABLE IF NOT EXISTS book (send_ns INTEGER, recv_ns INTEGER, ticker TEXT, yes_bid REAL, yes_ask REAL,
                                 yes_ask_sz REAL, no_bid REAL, no_ask REAL, no_ask_sz REAL);
CREATE TABLE IF NOT EXISTS trades (trade_id TEXT PRIMARY KEY, recv_ns INTEGER, ticker TEXT, created_us INTEGER,
                                   taker_side TEXT, yes_price REAL, count REAL);
CREATE TABLE IF NOT EXISTS markets (ticker TEXT PRIMARY KEY, open INTEGER, close INTEGER, strike REAL, result TEXT);
CREATE TABLE IF NOT EXISTS gaps (recv_ns INTEGER, src TEXT, error TEXT);
CREATE INDEX IF NOT EXISTS spot_recv ON spot (recv_ns);
CREATE INDEX IF NOT EXISTS book_ticker_send ON book (ticker, send_ns);
CREATE INDEX IF NOT EXISTS trades_ticker ON trades (ticker, created_us);
"""


def _iso_us(iso):
    """'2026-09-13T20:41:57.635818Z' -> epoch microseconds."""
    base, _, frac = iso.rstrip("Z").partition(".")
    return calendar.timegm(time.strptime(base, "%Y-%m-%dT%H:%M:%S")) * 1_000_000 + int((frac + "000000")[:6])


class Recorder:
    def __init__(self):
        self.db = sqlite3.connect(DB)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        self.rows = {"spot": [], "book": [], "trades": [], "gaps": []}
        self.counts = {}
        have = {r[1] for r in self.db.execute("PRAGMA table_info(book)")}
        if "src" not in have:
            self.db.execute("ALTER TABLE book ADD COLUMN src TEXT DEFAULT 'rest'")
            self.db.commit()
        self.rest = kc.Rest("prod")
        self.market = None       # BTC, as before
        self.eth_market = None
        try:
            kc.load_key("prod")
            self.ws_on = True
        except kc.KeyMissing:
            self.ws_on = False

    def add(self, table, row, label):
        self.rows[table].append(row)
        self.counts[label] = self.counts.get(label, 0) + 1

    def flush(self):
        sql = {"spot": "INSERT INTO spot VALUES (?,?,?,?,?)", "book": "INSERT INTO book VALUES (?,?,?,?,?,?,?,?,?,?)",
               "trades": "INSERT OR IGNORE INTO trades VALUES (?,?,?,?,?,?,?)", "gaps": "INSERT INTO gaps VALUES (?,?,?)"}
        for t, rows in self.rows.items():
            if rows:
                self.db.executemany(sql[t], rows)
                rows.clear()
        self.db.commit()

    # ---- US spot venues

    async def feed(self, name, url, sub, parse):
        import websockets
        while True:
            try:
                async with websockets.connect(url, open_timeout=10, max_size=None) as ws:
                    await ws.send(json.dumps(sub))
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                        recv = time.time_ns()
                        try:
                            for exch_us, px, sz in parse(json.loads(raw)):
                                self.add("spot", (recv, name, exch_us, px, sz), name)
                        except (ValueError, KeyError, TypeError):
                            pass
            except Exception as e:
                self.add("gaps", (time.time_ns(), name, repr(e)[:300]), "gaps")
                await asyncio.sleep(2)

    async def lead_feed(self, name, url, sub, parse):
        """Top-of-book mid from a perpetual-futures venue; stores every mid CHANGE, stamped when first seen.
        No throttle: Binance's ~650 book updates/s carry only ~5 mid changes/s (measured 2026-09-14), and
        a throttle both dropped changes and moved their timestamps later — fatal for a lead measurement."""
        import websockets
        last_mid = None
        while True:
            try:
                async with websockets.connect(url, open_timeout=10, max_size=None, ping_interval=15, ping_timeout=15) as ws:
                    if sub is not None:
                        await ws.send(json.dumps(sub))
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                        recv = time.time_ns()
                        try:
                            got = parse(json.loads(raw))
                        except (ValueError, KeyError, TypeError, IndexError):
                            got = None
                        if got is None:
                            continue
                        exch_us, mid = got
                        if mid != last_mid:
                            self.add("spot", (recv, name, exch_us, mid, 0.0), name)
                            last_mid = mid
            except Exception as e:
                self.add("gaps", (time.time_ns(), name, repr(e)[:300]), "gaps")
                await asyncio.sleep(2)

    @staticmethod
    def parse_binance_bbo(m):
        if "b" in m and "a" in m:
            return int(m.get("T") or m.get("E")) * 1000, (float(m["b"]) + float(m["a"])) / 2

    @staticmethod
    def parse_okx_bbo(m):
        d = (m.get("data") or [None])[0]
        if d and d.get("bids") and d.get("asks"):
            return int(d["ts"]) * 1000, (float(d["bids"][0][0]) + float(d["asks"][0][0])) / 2

    @staticmethod
    def parse_bybit_bbo(m):
        # orderbook.1 sends a snapshot then deltas; for depth 1 each message carries the new top when it changes
        d = m.get("data") or {}
        if d.get("b") and d.get("a"):
            return int(m["ts"]) * 1000, (float(d["b"][0][0]) + float(d["a"][0][0])) / 2

    @staticmethod
    def parse_deribit_ticker(m):
        d = (m.get("params") or {}).get("data")
        if d and d.get("best_bid_price") and d.get("best_ask_price"):
            return int(d["timestamp"]) * 1000, (d["best_bid_price"] + d["best_ask_price"]) / 2

    @staticmethod
    def parse_coinbase(m):
        if m.get("type") in ("match", "last_match"):
            yield _iso_us(m["time"]), float(m["price"]), float(m["size"])

    @staticmethod
    def parse_kraken(m):
        if m.get("channel") == "trade":
            for d in m.get("data", []):
                yield _iso_us(d["timestamp"]), float(d["price"]), float(d["qty"])

    @staticmethod
    def parse_bitstamp(m):
        if m.get("event") == "trade":
            d = m["data"]
            yield int(d["microtimestamp"]), float(d["price"]), float(d["amount"])

    # ---- Kalshi

    async def markets(self):
        while True:
            try:
                for series, attr in SERIES.items():
                    cur = getattr(self, attr)
                    if cur is not None and time.time() < cur["close"]:
                        continue
                    d, _ = await asyncio.to_thread(self.rest.request, "GET", f"/markets?series_ticker={series}&status=open&limit=5")
                    live = [m for m in d["markets"] if _iso_us(m["close_time"]) / 1e6 > time.time() + 1]
                    if live:
                        m = min(live, key=lambda x: x["close_time"])
                        setattr(self, attr, {"ticker": m["ticker"], "open": _iso_us(m["open_time"]) // 1_000_000,
                                             "close": _iso_us(m["close_time"]) // 1_000_000})
                        self.db.execute("INSERT OR IGNORE INTO markets VALUES (?,?,?,?,NULL)",
                                        (m["ticker"], _iso_us(m["open_time"]) // 1_000_000,
                                         _iso_us(m["close_time"]) // 1_000_000, m.get("floor_strike")))
                # fill in results for closed markets
                for (ticker,) in self.db.execute("SELECT ticker FROM markets WHERE result IS NULL AND close < ?",
                                                 (time.time() - 60,)).fetchall():
                    r, _ = await asyncio.to_thread(self.rest.request, "GET", f"/markets/{ticker}")
                    if r["market"].get("result") in ("yes", "no"):
                        self.db.execute("UPDATE markets SET result=? WHERE ticker=?", (r["market"]["result"], ticker))
            except kc.RateLimited:
                pass
            except Exception as e:
                self.add("gaps", (time.time_ns(), "kalshi-markets", repr(e)[:300]), "gaps")
            await asyncio.sleep(2)

    async def ws_book(self, attr="market"):
        """Top-of-book CHANGES from Kalshi's websocket (read-only key), stamped on arrival.
        ~600 deltas/s arrive; only rows where the top actually changed are stored.
        Verified 2026-09-13: rebuilt top matched REST 10/12, both misses were the
        websocket being newer, zero sequence gaps. One connection per series (BTC, ETH)."""
        import websockets
        key = kc.load_key("prod")
        while True:
            m = getattr(self, attr)
            if not m:
                await asyncio.sleep(1)
                continue
            try:
                async with websockets.connect(kc.HOSTS["prod"]["ws"], open_timeout=10, max_size=None,
                                              additional_headers=kc.auth_headers(key, "GET", kc.WS_PATH)) as ws:
                    await ws.send(json.dumps({"id": 1, "cmd": "subscribe",
                                              "params": {"channels": ["orderbook_delta"], "market_ticker": m["ticker"]}}))
                    levels, last_seq, last_top = {"yes": Book(), "no": Book()}, None, None
                    while getattr(self, attr) and getattr(self, attr)["ticker"] == m["ticker"]:
                        try:
                            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                        except asyncio.TimeoutError:
                            continue  # quiet book: re-check whether the market rolled over
                        recv = time.time_ns()
                        seq, body = msg.get("seq"), msg.get("msg", {})
                        if seq is not None:
                            if last_seq is not None and seq != last_seq + 1:
                                raise RuntimeError(f"sequence gap {last_seq}->{seq}")
                            last_seq = seq
                        if msg.get("type") == "orderbook_snapshot":
                            levels = {"yes": Book(), "no": Book()}   # heap-backed best bid: O(log n) per delta
                            levels["yes"].load(body.get("yes_dollars_fp") or [], [])
                            levels["no"].load(body.get("no_dollars_fp") or [], [])
                        elif msg.get("type") == "orderbook_delta":
                            side, p = body["side"], float(body["price_dollars"])
                            bk = levels[side]
                            bk.set("bid", p, bk.bids.get(p, 0.0) + float(body["delta_fp"]))
                        else:
                            continue
                        yb, nb = levels["yes"].best_bid, levels["no"].best_bid
                        row = (yb, round(1 - nb, 4) if nb is not None else None,
                               levels["no"].bids.get(nb, 0) if nb is not None else 0,
                               nb, round(1 - yb, 4) if yb is not None else None,
                               levels["yes"].bids.get(yb, 0) if yb is not None else 0)
                        if row != last_top:
                            last_top = row
                            # send_ns = recv_ns: a pushed update has no request leg
                            self.add("book", (recv, recv, m["ticker"]) + row + ("ws",), f"kalshi-book-ws:{attr}")
            except Exception as e:
                self.add("gaps", (time.time_ns(), f"kalshi-ws:{attr}", repr(e)[:300]), "gaps")
                await asyncio.sleep(1)

    async def book(self):
        """REST polls. With the websocket running these stay on, once a second, as the
        measurement of our request round trip — the latency an order would pay."""
        while True:
            t0 = time.perf_counter()
            m = self.market
            if m:
                send = time.time_ns()
                try:
                    ob, _ = await asyncio.to_thread(self.rest.request, "GET", f"/markets/{m['ticker']}/orderbook?depth=5")
                    top = kc.orderbook_top(ob["orderbook_fp"])
                    self.add("book", (send, time.time_ns(), m["ticker"], top["yes_bid"], top["yes_ask"], top["yes_ask_sz"],
                                      top["no_bid"], top["no_ask"], top["no_ask_sz"], "rest"), "kalshi-book")
                except kc.RateLimited as e:
                    self.add("gaps", (time.time_ns(), "kalshi-book", str(e)), "gaps")
                except Exception as e:
                    self.add("gaps", (time.time_ns(), "kalshi-book", repr(e)[:300]), "gaps")
            every = 1.0 if self.ws_on else BOOK_EVERY
            await asyncio.sleep(max(0.0, every - (time.perf_counter() - t0)))

    async def tape(self, attr="market", every=TAPE_EVERY):
        while True:
            m = getattr(self, attr)
            if m:
                try:
                    d, _ = await asyncio.to_thread(self.rest.request, "GET", f"/markets/trades?ticker={m['ticker']}&limit=100")
                    recv = time.time_ns()
                    for t in d.get("trades", []):
                        self.add("trades", (t["trade_id"], recv, t["ticker"], _iso_us(t["created_time"]), t["taker_side"],
                                            float(t["yes_price_dollars"]), float(t["count_fp"])), "kalshi-trades")
                except kc.RateLimited:
                    pass
                except Exception as e:
                    self.add("gaps", (time.time_ns(), "kalshi-trades", repr(e)[:300]), "gaps")
            await asyncio.sleep(every)

    def _first_kept_rowid(self, table, cutoff):
        """Smallest rowid whose recv_ns >= cutoff, by binary search on rowid (rows are inserted in
        time order). Point lookups only: a `WHERE recv_ns < ?` scan has no index on book/trades and
        would read millions of rows on this single-threaded writer, stalling every feed's timestamps."""
        lo, hi = self.db.execute(f"SELECT MIN(rowid), MAX(rowid) FROM {table}").fetchone()
        if lo is None:
            return None
        row_at = lambda r: self.db.execute(f"SELECT rowid, recv_ns FROM {table} WHERE rowid >= ? ORDER BY rowid LIMIT 1",
                                           (r,)).fetchone()
        if row_at(lo)[1] >= cutoff:
            return lo           # nothing old
        if row_at(hi)[1] < cutoff:
            return hi + 1       # everything old
        while hi - lo > 1:
            mid = (lo + hi) // 2
            rid, recv = row_at(mid)
            if recv < cutoff:
                lo = rid
            else:
                hi = mid
        return hi

    async def retention(self):
        """Delete rows older than RETAIN_DAYS, hourly, in 20k-row rowid ranges so the writer never stalls."""
        while True:
            try:
                cutoff = time.time_ns() - RETAIN_DAYS * 86400 * 10**9
                deleted = 0
                for table in ("spot", "book", "trades"):
                    keep_from = self._first_kept_rowid(table, cutoff)
                    start = self.db.execute(f"SELECT MIN(rowid) FROM {table}").fetchone()[0]
                    while keep_from is not None and start is not None and start < keep_from:
                        stop = min(start + 20000, keep_from)
                        cur = self.db.execute(f"DELETE FROM {table} WHERE rowid >= ? AND rowid < ?", (start, stop))
                        self.db.commit()
                        deleted += cur.rowcount
                        start = stop
                        await asyncio.sleep(0.02)  # let the feeds run between batches
                if deleted:
                    print(f"retention: deleted {deleted:,} rows older than {RETAIN_DAYS} days", flush=True)
            except sqlite3.Error as e:
                self.add("gaps", (time.time_ns(), "retention", repr(e)[:300]), "gaps")
            await asyncio.sleep(3600)

    async def run(self):
        if sys.platform == "win32":  # stay awake while recording (released on exit)
            import ctypes
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001)
        tasks = [
            self.feed("coinbase", "wss://ws-feed.exchange.coinbase.com",
                      {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches"]}, self.parse_coinbase),
            self.feed("kraken", "wss://ws.kraken.com/v2",
                      {"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD"]}}, self.parse_kraken),
            self.feed("bitstamp", "wss://ws.bitstamp.net",
                      {"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}}, self.parse_bitstamp),
            # ETH: same venues, separate connections and src names, so BTC rows are untouched
            self.feed("coinbase:ETH", "wss://ws-feed.exchange.coinbase.com",
                      {"type": "subscribe", "product_ids": ["ETH-USD"], "channels": ["matches"]}, self.parse_coinbase),
            self.feed("kraken:ETH", "wss://ws.kraken.com/v2",
                      {"method": "subscribe", "params": {"channel": "trade", "symbol": ["ETH/USD"]}}, self.parse_kraken),
            self.feed("bitstamp:ETH", "wss://ws.bitstamp.net",
                      {"event": "bts:subscribe", "data": {"channel": "live_trades_ethusd"}}, self.parse_bitstamp),
            # leading BTC feeds (perpetuals), for the does-earlier-information-help test
            self.lead_feed("binance_perp", "wss://fstream.binance.com/ws/btcusdt@bookTicker", None, self.parse_binance_bbo),
            self.lead_feed("okx_perp", "wss://ws.okx.com:8443/ws/v5/public",
                           {"op": "subscribe", "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"}]}, self.parse_okx_bbo),
            self.lead_feed("bybit_perp", "wss://stream.bybit.com/v5/public/linear",
                           {"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}, self.parse_bybit_bbo),
            self.lead_feed("deribit_perp", "wss://www.deribit.com/ws/api/v2",
                           {"jsonrpc": "2.0", "id": 1, "method": "public/subscribe",
                            "params": {"channels": ["ticker.BTC-PERPETUAL.100ms"]}}, self.parse_deribit_ticker),
            self.markets(), self.book(), self.tape(), self.tape("eth_market", 1.0), self.retention(),
        ] + ([self.ws_book(), self.ws_book("eth_market")] if self.ws_on else [])
        print(f"Kalshi book: {'websocket (top changes) + 1/s REST round-trip probes' if self.ws_on else 'REST polling every 100 ms'}", flush=True)
        runners = [asyncio.create_task(t) for t in tasks]
        last = time.time()
        while True:
            await asyncio.sleep(1)
            self.flush()
            if time.time() - last >= 60:
                last = time.time()
                print(time.strftime("%H:%M:%S"), dict(sorted(self.counts.items())), flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(Recorder().run())
    except KeyboardInterrupt:
        pass
