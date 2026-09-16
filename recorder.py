"""Record everything the Polymarket-vs-BTC dislocation study needs. Places no orders.

Five public feeds, each row stamped with local receive time (time.time_ns):

    chainlink      Polymarket's RTDS relay of the Chainlink BTC/USD stream the
                   5-minute Up/Down markets settle on. 1 print per second.
    coinbase       BTC-USD spot top of book.
    okx_perp       BTC-USDT-SWAP perpetual top of book (tick-by-tick).
    deribit_perp   BTC-PERPETUAL top of book, 100ms.
    deribit_index  Deribit's multi-exchange BTC index (arrives with the perp).

plus the raw Polymarket order-book stream for each 5-minute market (kept raw so
the book can be rebuilt exactly in analysis), Deribit DVOL once a minute, and
TCP connect time to Polymarket's matching endpoint once a minute.

    python recorder.py                 # until Ctrl+C
    python recorder.py --minutes 120

Uses the third-party `websockets` package, imported inside main() per CLAUDE.md:
data fetching may use a library, the math may not.
"""

import argparse
import asyncio
import json
import socket
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).with_name("pm_hf.sqlite")
WINDOW = 300  # seconds per market

SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (recv_ns INTEGER, src TEXT, exch_ms INTEGER,
                                  bid REAL, ask REAL, last REAL);
CREATE TABLE IF NOT EXISTS pm_msgs (recv_ns INTEGER, slug TEXT, payload TEXT);
CREATE TABLE IF NOT EXISTS windows (slug TEXT PRIMARY KEY, start INTEGER, end INTEGER,
                                    up_token TEXT, down_token TEXT, outcome TEXT);
CREATE TABLE IF NOT EXISTS dvol (recv_ns INTEGER, exch_ms INTEGER, value REAL);
CREATE TABLE IF NOT EXISTS rtt (recv_ns INTEGER, host TEXT, ms REAL);
CREATE TABLE IF NOT EXISTS gaps (recv_ns INTEGER, src TEXT, error TEXT);
CREATE INDEX IF NOT EXISTS ticks_src_recv ON ticks (src, recv_ns);
CREATE INDEX IF NOT EXISTS pm_slug_recv ON pm_msgs (slug, recv_ns);
"""


def _get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "pm-hf-recorder"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _iso_ms(s):
    """'2026-09-13T15:41:53.123456789Z' -> epoch ms. Coinbase sends nanoseconds."""
    base = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    frac = s[20:-1] if len(s) > 20 else "0"
    return int(base.timestamp() * 1000) + int((frac + "000")[:3])


class Store:
    """Single writer. Rows queue in memory and commit once a second."""

    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        self.pending = {"ticks": [], "pm_msgs": [], "dvol": [], "rtt": [], "gaps": []}
        self.counts = {}

    def add(self, table, row, src):
        self.pending[table].append(row)
        self.counts[src] = self.counts.get(src, 0) + 1

    def flush(self):
        sql = {
            "ticks": "INSERT INTO ticks VALUES (?,?,?,?,?,?)",
            "pm_msgs": "INSERT INTO pm_msgs VALUES (?,?,?)",
            "dvol": "INSERT INTO dvol VALUES (?,?,?)",
            "rtt": "INSERT INTO rtt VALUES (?,?,?)",
            "gaps": "INSERT INTO gaps VALUES (?,?,?)",
        }
        for table, rows in self.pending.items():
            if rows:
                self.db.executemany(sql[table], rows)
                rows.clear()
        self.db.commit()


# ---- message handlers: raw text -> rows. Unknown shapes are ignored, not guessed at.

def on_coinbase(store, recv, raw):
    m = json.loads(raw)
    if m.get("type") == "ticker":
        store.add("ticks", (recv, "coinbase", _iso_ms(m["time"]), float(m["best_bid"]),
                            float(m["best_ask"]), float(m["price"])), "coinbase")


def on_okx(store, recv, raw):
    if raw == "pong":
        return
    m = json.loads(raw)
    for d in m.get("data", []):
        store.add("ticks", (recv, "okx_perp", int(d["ts"]), float(d["bids"][0][0]),
                            float(d["asks"][0][0]), None), "okx_perp")


def on_deribit(store, recv, raw):
    m = json.loads(raw)
    d = m.get("params", {}).get("data")
    if not d:
        return
    store.add("ticks", (recv, "deribit_perp", d["timestamp"], d.get("best_bid_price"),
                        d.get("best_ask_price"), d.get("last_price")), "deribit_perp")
    store.add("ticks", (recv, "deribit_index", d["timestamp"], None, None,
                        d.get("index_price")), "deribit_index")


def on_chainlink(store, recv, raw):
    if not raw.strip() or raw.strip() == "PONG":
        return
    m = json.loads(raw)
    p = m.get("payload") or {}
    # The first message after subscribing is a backfill list; later ones are single prints.
    prints = p.get("data") if isinstance(p.get("data"), list) else [p]
    for x in prints:
        if "timestamp" in x and "value" in x:
            store.add("ticks", (recv, "chainlink", int(x["timestamp"]), None, None,
                                float(x["value"])), "chainlink")


# ---- connection loops

async def ws_feed(websockets, store, name, url, sub, handler, stop, text_ping=None, stale_after=30):
    """Reconnect forever until `stop`. Every disconnect is logged to `gaps`.

    `stale_after`: seconds of silence before the connection is treated as dead.
    On 2026-09-13 the Chainlink relay went quiet for 140-185s at a time without
    ever closing the socket, and a loop that only reconnects on close waited it out.
    """
    backoff = 1
    while not stop.is_set():
        try:
            async with websockets.connect(url, open_timeout=10, max_size=None,
                                          ping_interval=None if text_ping else 20) as ws:
                await ws.send(json.dumps(sub))
                backoff = 1
                pinger = None
                if text_ping:
                    async def ping_loop():
                        while True:
                            await asyncio.sleep(text_ping[1])
                            await ws.send(text_ping[0])
                    pinger = asyncio.create_task(ping_loop())
                try:
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=stale_after)
                        except asyncio.TimeoutError:
                            raise ConnectionError(f"silent for {stale_after}s")
                        recv = time.time_ns()
                        try:
                            handler(store, recv, raw if isinstance(raw, str) else raw.decode())
                        except (ValueError, KeyError, TypeError, IndexError):
                            pass  # a malformed message is skipped; counts show if it's systematic
                        if stop.is_set():
                            break
                finally:
                    if pinger:
                        pinger.cancel()
        except Exception as e:  # network errors of every kind: log and reconnect
            store.add("gaps", (time.time_ns(), name, repr(e)[:300]), "gaps")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


async def market_window(websockets, store, start, stop):
    """Subscribe to one 5-minute market from 60s before open to 30s after close."""
    slug = f"btc-updown-5m-{start}"
    for attempt in range(10):
        try:
            ev = await asyncio.to_thread(_get_json, f"https://gamma-api.polymarket.com/events?slug={slug}")
            m = ev[0]["markets"][0]
            outcomes = json.loads(m["outcomes"])
            tokens = json.loads(m["clobTokenIds"])
            tok = dict(zip(outcomes, tokens))
            break
        except Exception as e:
            store.add("gaps", (time.time_ns(), slug, "lookup: " + repr(e)[:250]), "gaps")
            await asyncio.sleep(5)
    else:
        return
    store.db.execute("INSERT OR IGNORE INTO windows VALUES (?,?,?,?,?,NULL)",
                     (slug, start, start + WINDOW, tok["Up"], tok["Down"]))

    def handler(st, recv, raw):
        if raw.strip() and raw.strip() != "PONG":
            st.add("pm_msgs", (recv, slug, raw), "polymarket")

    task = asyncio.create_task(ws_feed(
        websockets, store, slug, "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        {"assets_ids": [tok["Up"], tok["Down"]], "type": "market"}, handler, stop,
        text_ping=("PING", 10)))
    await asyncio.sleep(max(0, start + WINDOW + 30 - time.time()))
    task.cancel()


async def market_scheduler(websockets, store, stop):
    launched = set()
    while not stop.is_set():
        now = time.time()
        nxt = int(now) - int(now) % WINDOW
        for start in (nxt, nxt + WINDOW):
            if start not in launched and start - now <= 60:
                launched.add(start)
                asyncio.create_task(market_window(websockets, store, start, stop))
        await asyncio.sleep(1)


async def poll_dvol(store, stop):
    while not stop.is_set():
        try:
            now_ms = int(time.time() * 1000)
            url = ("https://www.deribit.com/api/v2/public/get_volatility_index_data?currency=BTC"
                   f"&start_timestamp={now_ms - 180_000}&end_timestamp={now_ms}&resolution=60")
            d = (await asyncio.to_thread(_get_json, url))["result"]["data"][-1]
            store.add("dvol", (time.time_ns(), d[0], d[4]), "dvol")
        except Exception as e:
            store.add("gaps", (time.time_ns(), "dvol", repr(e)[:300]), "gaps")
        await asyncio.sleep(60)


def _tcp_connect_ms(host):
    t0 = time.perf_counter()
    with socket.create_connection((host, 443), timeout=5):
        pass
    return (time.perf_counter() - t0) * 1000


async def poll_rtt(store, stop):
    """TCP connect time ~ one network round trip. An order also pays TLS-free
    request time plus matching, so this is a LOWER bound on our order latency."""
    while not stop.is_set():
        for host in ("clob.polymarket.com",):
            try:
                ms = await asyncio.to_thread(_tcp_connect_ms, host)
                store.add("rtt", (time.time_ns(), host, ms), "rtt")
            except OSError as e:
                store.add("gaps", (time.time_ns(), "rtt", repr(e)[:300]), "gaps")
        await asyncio.sleep(60)


def _keep_awake():
    """Ask Windows not to sleep while this process runs (released on exit).

    On 2026-09-13 the machine slept 38 minutes mid-recording. This is the
    per-process request media players make, not a change to power settings.
    """
    if sys.platform == "win32":
        import ctypes
        ES_CONTINUOUS, ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001
        ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)


async def run(minutes):
    import websockets  # presentation/data exception: imported where needed

    _keep_awake()
    store = Store(DB_PATH)
    stop = asyncio.Event()
    feeds = [
        ("coinbase", "wss://ws-feed.exchange.coinbase.com",
         {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}, on_coinbase, None),
        ("okx_perp", "wss://ws.okx.com:8443/ws/v5/public",
         {"op": "subscribe", "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"}]}, on_okx, ("ping", 20)),
        ("deribit", "wss://www.deribit.com/ws/api/v2",
         {"jsonrpc": "2.0", "id": 1, "method": "public/subscribe",
          "params": {"channels": ["ticker.BTC-PERPETUAL.100ms"]}}, on_deribit, None),
        ("chainlink", "wss://ws-live-data.polymarket.com",
         {"action": "subscribe", "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*",
                                                    "filters": "{\"symbol\":\"btc/usd\"}"}]},
         on_chainlink, ("PING", 5)),
    ]
    # Chainlink prints every second, so 5s of silence is a stall; the others can
    # legitimately pause (Coinbase ticker only sends on trades).
    silence = {"chainlink": 5, "okx_perp": 10, "deribit": 10, "coinbase": 30}
    tasks = [asyncio.create_task(ws_feed(websockets, store, n, u, s, h, stop, text_ping=p, stale_after=silence[n]))
             for n, u, s, h, p in feeds]
    tasks += [asyncio.create_task(market_scheduler(websockets, store, stop)),
              asyncio.create_task(poll_dvol(store, stop)),
              asyncio.create_task(poll_rtt(store, stop))]

    t_end = time.time() + minutes * 60 if minutes else float("inf")
    last_print = time.time()
    try:
        while time.time() < t_end:
            await asyncio.sleep(1)
            store.flush()
            if time.time() - last_print >= 60:
                last_print = time.time()
                print(time.strftime("%H:%M:%S"), dict(sorted(store.counts.items())), flush=True)
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        store.flush()
        print("final counts", dict(sorted(store.counts.items())), flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--minutes", type=float, default=0, help="stop after N minutes (0 = until Ctrl+C)")
    args = ap.parse_args()
    try:
        asyncio.run(run(args.minutes))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
