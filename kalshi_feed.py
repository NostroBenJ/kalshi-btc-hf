"""Kalshi order-book websocket in its own process, publishing top of book into shared memory.

Why: on the first (burstable) server the bot's main process spent ~0.8 ms of CPU per Kalshi book delta —
39% of a core at 370 deltas/s, 78% at 976/s on a Sunday night (2026-09-13) — and a
single Python process cannot use more than one core. Weekday volume would saturate it.
This moves the parsing to its own process; the bot reads six numbers every 5 ms.

The bot sends the ticker to follow over a queue (it changes every 15 minutes). The
child owns reconnects, sequence-gap resyncs, and float-dust handling (spot_feeds.Book).

Shared layout (doubles):
  [0] yes_bid [1] yes_ask [2] yes_ask_sz [3] no_bid [4] no_ask [5] no_ask_sz
  [6] book time: last delta, or now while the socket is alive (silence = unchanged)
  [7] delta count  [8] connected 0/1  [9] exchange->receipt latency p50 ms  [10] child CPU %
  [11] ticker generation (which subscribe the numbers belong to)
"""

import asyncio
import time

FIELDS = ("yes_bid", "yes_ask", "yes_ask_sz", "no_bid", "no_ask", "no_ask_sz")
SLOT_TS, SLOT_COUNT, SLOT_CONNECTED, SLOT_LAT, SLOT_CPU, SLOT_GEN = 6, 7, 8, 9, 10, 11
LEN = 12


def fast_json():
    """orjson when installed (3-5x faster parsing); stdlib json otherwise. Data path only."""
    try:
        import orjson
        return orjson.loads, lambda o: orjson.dumps(o).decode()
    except ImportError:
        import json
        return json.loads, json.dumps


def run_loop(coro):
    """uvloop when installed (Linux); the standard loop otherwise."""
    try:
        import uvloop
        return uvloop.run(coro)
    except ImportError:
        return asyncio.run(coro)


def _child(shared, commands, errors, key_env):
    import kalshi_client as kc
    from spot_feeds import Book
    loads, dumps = fast_json()
    nan = float("nan")
    state = {"ticker": None, "gen": 0}

    def publish(levels, recv):
        yb, nb = levels["yes"].best_bid, levels["no"].best_bid
        vals = (yb if yb is not None else nan, round(1 - nb, 4) if nb is not None else nan,
                levels["no"].bids.get(nb, 0.0) if nb is not None else 0.0,
                nb if nb is not None else nan, round(1 - yb, 4) if yb is not None else nan,
                levels["yes"].bids.get(yb, 0.0) if yb is not None else 0.0)
        with shared.get_lock():
            shared[0:6] = vals
            shared[SLOT_TS] = recv
            shared[SLOT_COUNT] += 1

    async def take_commands():
        while True:
            try:
                while True:
                    ticker = commands.get_nowait()
                    if ticker != state["ticker"]:
                        state["ticker"], state["gen"] = ticker, state["gen"] + 1
            except Exception:
                pass
            await asyncio.sleep(0.2)

    async def meters():
        last_c, last_w = time.process_time(), time.time()
        while True:
            await asyncio.sleep(0.25)
            now = time.time()
            with shared.get_lock():
                if shared[SLOT_CONNECTED] == 1:
                    shared[SLOT_TS] = now  # socket alive: a quiet book is a current book
            if now - last_w >= 5:
                c = time.process_time()
                with shared.get_lock():
                    shared[SLOT_CPU] = 100 * (c - last_c) / (now - last_w)
                last_c, last_w = c, now

    async def book():
        import websockets
        key = kc.load_key("prod") if key_env else None
        lats = []
        while True:
            ticker, gen = state["ticker"], state["gen"]
            if not ticker or not key:
                await asyncio.sleep(0.2)
                continue
            try:
                async with websockets.connect(kc.HOSTS["prod"]["ws"], open_timeout=10, max_size=None, ping_interval=10,
                                              ping_timeout=15, additional_headers=kc.auth_headers(key, "GET", kc.WS_PATH)) as ws:
                    await ws.send(dumps({"id": 1, "cmd": "subscribe",
                                         "params": {"channels": ["orderbook_delta"], "market_ticker": ticker}}))
                    levels, last_seq = {"yes": Book(), "no": Book()}, None
                    with shared.get_lock():
                        shared[0:6] = (nan, nan, 0.0, nan, nan, 0.0)
                        shared[SLOT_GEN] = gen
                    while state["gen"] == gen:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1)
                        except asyncio.TimeoutError:
                            continue
                        msg = loads(raw)
                        seq, body = msg.get("seq"), msg.get("msg") or {}
                        if seq is not None:
                            if last_seq is not None and seq != last_seq + 1:
                                raise RuntimeError(f"sequence gap {last_seq}->{seq}")
                            last_seq = seq
                        kind = msg.get("type")
                        if kind == "orderbook_snapshot":
                            levels = {"yes": Book(), "no": Book()}
                            levels["yes"].load(body.get("yes_dollars_fp") or [], [])
                            levels["no"].load(body.get("no_dollars_fp") or [], [])
                            with shared.get_lock():
                                shared[SLOT_CONNECTED] = 1
                        elif kind == "orderbook_delta":
                            bk = levels[body["side"]]
                            p = float(body["price_dollars"])
                            bk.set("bid", p, bk.bids.get(p, 0.0) + float(body["delta_fp"]))
                        elif kind == "error":
                            raise RuntimeError(f"websocket error: {body}")
                        else:
                            continue
                        recv = time.time()
                        publish(levels, recv)
                        if body.get("ts_ms"):
                            lats.append(recv * 1000 - body["ts_ms"])
                            if len(lats) >= 200:
                                lats.sort()
                                with shared.get_lock():
                                    shared[SLOT_LAT] = lats[100]
                                lats.clear()
            except Exception as e:
                with shared.get_lock():
                    shared[SLOT_CONNECTED] = 0
                try:
                    errors.put_nowait(repr(e)[:180])
                except Exception:
                    pass
                await asyncio.sleep(0.5 if "sequence gap" in repr(e) else 2)

    async def main():
        await asyncio.gather(take_commands(), meters(), book())

    run_loop(main())


class KalshiBookProcess:
    def __init__(self, have_key):
        import multiprocessing as mp
        ctx = mp.get_context("spawn")
        nan = float("nan")
        self.shared = ctx.Array("d", [nan, nan, 0.0, nan, nan, 0.0, 0.0, 0.0, 0.0, nan, nan, 0.0])
        self.commands = ctx.Queue(maxsize=50)
        self.errors = ctx.Queue(maxsize=100)
        self.proc = ctx.Process(target=_child, args=(self.shared, self.commands, self.errors, have_key),
                                daemon=True, name="kalshi-book")
        self.ticker = None

    def start(self):
        self.proc.start()

    def alive(self):
        return self.proc.is_alive()

    def follow(self, ticker):
        if ticker and ticker != self.ticker:
            self.ticker = ticker
            self.commands.put_nowait(ticker)

    def read(self):
        with self.shared.get_lock():
            s = list(self.shared)
        clean = lambda v: None if v != v else v
        out = {f: clean(s[i]) for i, f in enumerate(FIELDS)}
        out.update(ts=s[SLOT_TS], count=int(s[SLOT_COUNT]), connected=s[SLOT_CONNECTED] == 1,
                   lat_p50=clean(s[SLOT_LAT]), cpu_pct=clean(s[SLOT_CPU]))
        return out

    def drain_errors(self, limit=20):
        out = []
        try:
            while len(out) < limit:
                out.append(self.errors.get_nowait())
        except Exception:
            pass
        return out
