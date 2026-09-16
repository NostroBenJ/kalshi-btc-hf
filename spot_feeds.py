"""Composite BTC price from four US order books inside CF Benchmarks' BRTI.

Why: the bot's first BTC feed was Coinbase's `ticker`, which only speaks when a
trade prints — 3.3 updates/s measured 2026-09-13. The news trigger is a race to
notice a move before Kalshi reprices, so it must hear the BOOK move, not wait
for a trade. Measured the same minute, public, reachable from the US:

    Coinbase level2_batch   16.7 /s      Kraken v2 book   9.9 /s
    Gemini v2 l2            95.3 /s      Bitstamp book    9.3 /s

Each venue keeps its own top of book; the composite is the MEDIAN of the mids
that are fresh (< STALE_S old). A median of BRTI constituents both moves sooner
and tracks Kalshi's settlement index more closely than any one exchange, and one
venue printing a bad price cannot drag it.
"""

import asyncio
import heapq
import json
import statistics
import time

STALE_S = 3.0


class Book:
    """One venue's bids/asks. The best price comes from a heap with lazy deletion, so
    removing the top level costs O(log n) instead of a rescan.

    Why: Coinbase's level2 book holds ~42,000 levels and its top churns many times a
    second. The first version rescanned the whole side with max()/min() whenever the
    best level emptied; on a small burstable cloud instance, together with everything else,
    that starved the event loop until the Kalshi websocket missed its pings (2026-09-13).
    """

    def __init__(self):
        self.bids, self.asks = {}, {}
        self._bid_heap, self._ask_heap = [], []   # bids stored negated: heapq is a min-heap
        self.ts = 0.0

    def load(self, bids, asks):
        self.bids = {float(p): float(s) for p, s in bids if float(s) > 0}
        self.asks = {float(p): float(s) for p, s in asks if float(s) > 0}
        self._bid_heap = [-p for p in self.bids]
        self._ask_heap = list(self.asks)
        heapq.heapify(self._bid_heap)
        heapq.heapify(self._ask_heap)

    def set(self, side, price, size):
        book = self.bids if side == "bid" else self.asks
        price, size = float(price), float(size)
        # <= 1e-9, not <= 0: a Kalshi level emptied by float deltas (+2.00, -2.00...) ends at ~4e-13,
        # and treating that dust as a live level put dead prices at the top of the book (2026-09-13)
        if size <= 1e-9:
            book.pop(price, None)       # its heap entry goes stale and is skipped when it surfaces
            return
        if price not in book:
            heapq.heappush(self._bid_heap if side == "bid" else self._ask_heap, -price if side == "bid" else price)
        book[price] = size
        # heaps only grow with re-added prices; rebuild when stale entries dominate
        heap = self._bid_heap if side == "bid" else self._ask_heap
        if len(heap) > 4 * len(book) + 1000:
            fresh = [-p for p in book] if side == "bid" else list(book)
            heapq.heapify(fresh)
            if side == "bid":
                self._bid_heap = fresh
            else:
                self._ask_heap = fresh

    @property
    def best_bid(self):
        h = self._bid_heap
        while h and -h[0] not in self.bids:
            heapq.heappop(h)
        return -h[0] if h else None

    @property
    def best_ask(self):
        h = self._ask_heap
        while h and h[0] not in self.asks:
            heapq.heappop(h)
        return h[0] if h else None

    def mid(self):
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None or bid >= ask:
            return None  # empty or crossed: do not publish a price from it
        return (bid + ask) / 2


class SpotFeeds:
    """Runs the four venue connections; calls on_update(venue) after every book change."""

    VENUES = ("coinbase", "kraken", "bitstamp", "gemini")

    def __init__(self, on_update, on_trade=None, on_error=None):
        self.books = {v: Book() for v in self.VENUES}
        self.counts = {v: 0 for v in self.VENUES}
        self.on_update, self.on_trade, self.on_error = on_update, on_trade, on_error or (lambda v, e: None)
        self.loads = json.loads  # the child process swaps in orjson when installed

    def composite(self, now=None):
        now = now or time.time()
        mids = [b.mid() for b in self.books.values() if now - b.ts < STALE_S]
        mids = [m for m in mids if m is not None]
        return (statistics.median(mids), len(mids)) if mids else (None, 0)

    def _touch(self, venue):
        self.books[venue].ts = time.time()
        self.counts[venue] += 1
        self.on_update(venue)

    async def _run(self, venue, url, sub, handle):
        import websockets
        while True:
            try:
                async with websockets.connect(url, open_timeout=10, max_size=None, ping_interval=10, ping_timeout=10) as ws:
                    await ws.send(json.dumps(sub))
                    self.books[venue] = Book()
                    while True:
                        handle(self.loads(await asyncio.wait_for(ws.recv(), timeout=30)))
            except Exception as e:
                self.books[venue].ts = 0.0  # a dropped venue leaves the composite until it reconnects
                self.on_error(venue, e)
                await asyncio.sleep(2)

    # ---- venue parsers

    def _coinbase(self, m):
        t = m.get("type")
        if t == "snapshot":
            self.books["coinbase"].load(m["bids"], m["asks"])
            self._touch("coinbase")
        elif t == "l2update":
            b = self.books["coinbase"]
            for side, price, size in m["changes"]:
                b.set("bid" if side == "buy" else "ask", price, size)
            self._touch("coinbase")
        elif t == "ticker" and self.on_trade:
            self.on_trade("coinbase", m)

    def _kraken(self, m):
        if m.get("channel") != "book":
            return
        b = self.books["kraken"]
        for d in m.get("data", []):
            if m.get("type") == "snapshot":
                b.load([(x["price"], x["qty"]) for x in d["bids"]], [(x["price"], x["qty"]) for x in d["asks"]])
            else:
                for x in d.get("bids", []):
                    b.set("bid", x["price"], x["qty"])
                for x in d.get("asks", []):
                    b.set("ask", x["price"], x["qty"])
                # Kraken sends a depth-10 book: levels pushed past 10 are the client's to drop
                for side, book in (("bid", b.bids), ("ask", b.asks)):
                    if len(book) > 10:
                        keep = set(sorted(book, reverse=(side == "bid"))[:10])
                        for p in [p for p in book if p not in keep]:
                            book.pop(p)
        self._touch("kraken")

    def _bitstamp(self, m):
        if m.get("event") == "data":  # each message is the full top-100 book
            d = m["data"]
            self.books["bitstamp"].load(d["bids"][:5], d["asks"][:5])
            self._touch("bitstamp")

    def _gemini(self, m):
        if m.get("type") == "l2_updates":
            b = self.books["gemini"]
            for side, price, size in m.get("changes", []):
                b.set("bid" if side == "buy" else "ask", price, size)
            self._touch("gemini")

    def tasks(self):
        return [
            self._run("coinbase", "wss://ws-feed.exchange.coinbase.com",
                      {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["level2_batch", "ticker"]}, self._coinbase),
            self._run("kraken", "wss://ws.kraken.com/v2",
                      {"method": "subscribe", "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 10}}, self._kraken),
            self._run("bitstamp", "wss://ws.bitstamp.net",
                      {"event": "bts:subscribe", "data": {"channel": "order_book_btcusd"}}, self._bitstamp),
            self._run("gemini", "wss://api.gemini.com/v2/marketdata",
                      {"type": "subscribe", "subscriptions": [{"name": "l2", "symbols": ["BTCUSD"]}]}, self._gemini),
        ]


# ---------------------------------------------------------------- separate-process mode
#
# Why: on a 2-vCPU burstable instance the bot's single event loop parsed ~430 BTC book
# messages/s (80% from Gemini) AND ~760 Kalshi deltas/s AND ran the model. When BTC moved —
# exactly when the trigger fires — CPU spiked to 50-96% of a core, the loop woke 50+ ms late,
# and the overload guard blocked 22 signals (2026-09-13). This runs the four venues in their
# own process, on the other core, and publishes only the result into shared memory.
#
# Shared layout (doubles), written by the child under the Array's lock:
#   [0] composite mid  [1] receive time  [2] fresh venues  [3] total updates  [4] child CPU %
#   then per venue v: [5+3v] mid  [6+3v] last update time  [7+3v] update count

SLOT_MID, SLOT_TS, SLOT_N, SLOT_COUNT, SLOT_CPU = 0, 1, 2, 3, 4
SHARED_LEN = 5 + 3 * len(SpotFeeds.VENUES)


def _child(shared, trades, errors):
    """Process entry point: run the venues, write the composite on every book change."""
    def on_update(venue):
        now = time.time()
        mid, n = feeds.composite(now)
        i = SpotFeeds.VENUES.index(venue)
        vmid = feeds.books[venue].mid()
        with shared.get_lock():
            if mid is not None:
                shared[SLOT_MID], shared[SLOT_TS], shared[SLOT_N] = mid, now, n
            shared[SLOT_COUNT] += 1
            shared[5 + 3 * i] = vmid if vmid is not None else float("nan")
            shared[6 + 3 * i] = now
            shared[7 + 3 * i] = feeds.counts[venue]

    def on_trade(venue, m):
        try:  # for the dashboard tape and Coinbase latency only; never block the feed on it
            trades.put_nowait((time.time(), m.get("time"), m.get("side", ""), float(m.get("last_size") or 0), float(m["price"])))
        except Exception:
            pass

    def on_error(venue, e):
        try:
            errors.put_nowait((venue, repr(e)[:160]))
        except Exception:
            pass

    from kalshi_feed import fast_json, run_loop  # orjson / uvloop when installed, stdlib otherwise
    feeds = SpotFeeds(on_update, on_trade, on_error)
    feeds.loads = fast_json()[0]

    async def cpu_meter():
        last_c, last_w = time.process_time(), time.time()
        while True:
            await asyncio.sleep(5)
            c, w = time.process_time(), time.time()
            with shared.get_lock():
                shared[SLOT_CPU] = 100 * (c - last_c) / (w - last_w)
            last_c, last_w = c, w

    async def main():
        await asyncio.gather(*feeds.tasks(), cpu_meter())

    run_loop(main())


class SpotProcess:
    """The four venues in a child process. start() it once; read() is a cheap snapshot."""

    def __init__(self):
        import multiprocessing as mp
        ctx = mp.get_context("spawn")  # same behaviour on Windows and Linux; no inherited sockets
        self.shared = ctx.Array("d", [float("nan"), 0.0, 0.0, 0.0, float("nan")] + [float("nan"), 0.0, 0.0] * len(SpotFeeds.VENUES))
        self.trades = ctx.Queue(maxsize=500)
        self.errors = ctx.Queue(maxsize=100)
        self.proc = ctx.Process(target=_child, args=(self.shared, self.trades, self.errors), daemon=True, name="spot-feeds")

    def start(self):
        self.proc.start()

    def alive(self):
        return self.proc.is_alive()

    def read(self):
        with self.shared.get_lock():
            s = list(self.shared)
        venues = {}
        for i, v in enumerate(SpotFeeds.VENUES):
            mid, ts, count = s[5 + 3 * i], s[6 + 3 * i], s[7 + 3 * i]
            venues[v] = {"mid": None if mid != mid else mid, "ts": ts, "count": int(count)}
        return {"mid": None if s[SLOT_MID] != s[SLOT_MID] else s[SLOT_MID], "ts": s[SLOT_TS], "n": int(s[SLOT_N]),
                "count": int(s[SLOT_COUNT]), "cpu_pct": None if s[SLOT_CPU] != s[SLOT_CPU] else s[SLOT_CPU], "venues": venues}

    def drain(self, q, limit=200):
        out = []
        try:
            while len(out) < limit:
                out.append(q.get_nowait())
        except Exception:
            pass
        return out


def _verify():
    ok = True
    b = Book()
    b.load([["100", "1"], ["99", "2"]], [["101", "1"], ["102", "3"]])
    ok &= b.mid() == 100.5
    b.set("bid", 100, 0)          # best bid removed -> rescan finds 99
    ok &= b.best_bid == 99.0
    b.set("ask", 100.5, 1)        # new better ask
    ok &= b.best_ask == 100.5
    b.set("bid", 101, 1)          # crossed book publishes nothing
    ok &= b.mid() is None
    # heap vs brute force: 20,000 random updates on a 5,000-level book, best must always match
    import random
    rng = random.Random(3)
    h = Book()
    h.load([[100 - i * 0.01, 1] for i in range(2500)], [[100.01 + i * 0.01, 1] for i in range(2500)])
    agree = True
    for _ in range(20000):
        side = rng.choice(("bid", "ask"))
        px = round(rng.uniform(95, 99.99) if side == "bid" else rng.uniform(100.01, 105), 2)
        h.set(side, px, rng.choice((0, 0, 1, 2)))
        want_b = max(h.bids) if h.bids else None
        want_a = min(h.asks) if h.asks else None
        agree &= h.best_bid == want_b and h.best_ask == want_a
    ok &= agree
    print("  heap best == brute-force max/min over 20,000 random updates:", agree)
    # float dust from additive deltas must empty a level (the Kalshi websocket applies deltas)
    k = Book()
    k.load([["0.51", "0.3"], ["0.33", "5"]], [])
    size = 0.3
    for d in (0.1, 0.2):
        size -= d                      # 0.3 - 0.1 - 0.2 == 5.55e-17, not 0
        k.set("bid", 0.51, size)
    ok &= k.best_bid == 0.33
    print("  level emptied by float deltas (residue", f"{size:.1e})", "is removed:", k.best_bid == 0.33)
    f = SpotFeeds(lambda v: None)
    now = time.time()
    for v, (bid, ask) in zip(SpotFeeds.VENUES, ((10, 12), (11, 13), (12, 14), (100, 102))):
        f.books[v].load([[bid, 1]], [[ask, 1]])
        f.books[v].ts = now
    ok &= f.composite(now) == (12.5, 4)   # median of 11, 12, 13, 101: one wild venue can't drag it
    f.books["gemini"].ts = now - 10       # stale venue drops out
    ok &= f.composite(now) == (12.0, 3)
    print("spot_feeds verify:", "ALL PASS" if ok else "FAILED")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if _verify() else 1)
