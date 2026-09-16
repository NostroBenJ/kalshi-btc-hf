"""Paper-trading bot for Kalshi KXBTC15M, with a live dashboard.

    python paper_bot.py                  # paper (default): no real orders; open http://localhost:8765
    python paper_bot.py --broker live    # REAL MONEY, capped (see LiveBroker); refuses unless armed

What it does, every loop:
  1. Coinbase BTC-USD top of book over websocket -> a per-second price history.
  2. Kalshi's current 15-minute market and its order book, polled every 500 ms
     (public REST, ~110 ms per request measured 2026-09-13).
  3. Fair P(yes) from fair_value.fair_up — the model the backtests verified —
     with Kalshi's own floor_strike as K and Coinbase shifted by the measured
     basis (Coinbase's 60s average before the open minus floor_strike).
  4. If the ask is below fair by more than the fee plus THRESHOLD, a simulated
     order. It "arrives" SIM_LATENCY_MS later and fills only if the book still
     offers that price then — so a bot slower than the dislocation misses.
  5. At the close, positions settle against Kalshi's real result.

Why paper: kalshi_backtest.py has not passed, and live orders need the operator's
own API key. The Broker seam is where a live client would go, and nothing else
in the bot would change.
"""

import asyncio
import calendar
import json
import math
import socket
import statistics
import sqlite3
import threading
import time
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fair_value import fair_up, realized_vol, settle_times, taker_fee
from risk import RiskManager
from kalshi_feed import FIELDS as KFIELDS, KalshiBookProcess
from spot_feeds import SpotProcess

HERE = Path(__file__).parent
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
PORT = 8765

STRATEGY = "news"       # "news" (default) or "level" (the original disagreement rule)
# news trigger — the rule kalshi_ms.py tested on 74.6M fills: the model's fair value moved
# >= TRIGGER_DELTA in the last second, Kalshi's mid lagged by >= half of that, and the
# ask is still below fair after the fee. At 0 ms it paid +3.5c/contract (2c) and +6.0c (5c);
# from +100 ms on Binance's clock it lost. Whether THIS server is fast enough is what paper tells us.
TRIGGER_DELTA = 0.02
TRIGGER_COOLDOWN_S = 2  # one entry per side per 2 s, as in the replay and the rate count
THRESHOLD = 0.05        # level strategy only: edge after fee required, $/contract
SIM_LATENCY_MS = 250    # fallback until the order-path round trip has been measured
# Position sizing lives in risk.py (dollars at risk per market, sized by edge). The old fixed
# 50-contract orders let up to 400 contracts ride one market and buy both sides of it.
COOLDOWN_S = 5          # level strategy only: between orders on the same side
FEED_ID = "median4"     # BTC price definition; a stored basis is reused only for the same feed
TRIGGER_MAX_LOOKBACK_S = 1.5  # the "1 s ago" reference must really be ~1 s old, from the same market
OVERLOAD_LAG_MS = 50    # no new entries while the event loop's p90 wake-up lag exceeds this
BOOK_POLL_S = 0.5


def _get(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "pm-hf-paper", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _iso(iso):
    """UTC ISO -> epoch. calendar.timegm, not mktime: mktime would apply local DST."""
    return calendar.timegm(time.strptime(iso[:19], "%Y-%m-%dT%H:%M:%S"))


def _iso_frac(iso):
    """UTC ISO with fractional seconds -> float epoch."""
    frac = iso[19:].rstrip("Z").lstrip(".")
    return _iso(iso) + (float("0." + frac) if frac.isdigit() else 0.0)


class PaperBroker:
    """Simulated fills against the real production book."""

    name = "PAPER"
    venue = "sim vs prod book"

    def __init__(self, bot):
        self.bot = bot

    async def take(self, order):
        # Latency = this machine's MEASURED kept-alive round trip to Kalshi (full trip, so a
        # little conservative), not a guess. Falls back to SIM_LATENCY_MS until measured.
        latency_ms = self.bot.sim_latency_ms
        await asyncio.sleep(latency_ms / 1000)
        # Judge the fill on a book snapshot taken AFTER the order would have arrived.
        # The book refreshes every 500 ms, so without this the "arrival" check re-read
        # the very snapshot that produced the signal: 37 of 37 paper orders filled on
        # 2026-09-13 and the latency simulation never bit.
        arrive = order["signal_ts"] + latency_ms / 1000
        for _ in range(40):
            if self.bot.book["ts"] >= arrive:
                break
            await asyncio.sleep(0.05)
        else:
            order.update(status="missed", note="no fresh book after arrival", arrive_ts=time.time(), ack_ms=latency_ms)
            return order
        book = self.bot.book
        ask, size = (book["yes_ask"], book["yes_ask_sz"]) if order["side"] == "yes" else (book["no_ask"], book["no_ask_sz"])
        order["arrive_ts"] = time.time()
        order["ack_ms"] = latency_ms
        if ask is None or ask > order["limit"] + 1e-9 or not size:
            order["status"] = "missed"
            order["note"] = "price gone" if ask is not None else "no book"
            return order
        # whole contracts only: the ask can show a fractional size (e.g. 0.4), and filling that
        # recorded "qty 0" trades with a fee and ~zero P&L on 2026-09-13
        order["contracts"] = math.floor(min(order["contracts"], size))
        if order["contracts"] < 1:
            order["status"], order["note"] = "missed", "less than 1 contract at the ask"
            return order
        order["fill_px"] = ask
        order["fee"] = taker_fee(ask) * order["contracts"]
        order["status"] = "filled"
        return order

    async def result(self, ticker):
        return (await asyncio.to_thread(self.bot.rest.request, "GET", f"/markets/{ticker}"))[0]["market"].get("result")


class DemoBroker:
    """Real immediate-or-cancel orders on Kalshi's DEMO exchange (fake money).

    Signals still come from the production book and real BTC; the order goes to
    the same ticker on demo at the signal's limit price. What this measures is
    real: auth, order acknowledgement latency, the order lifecycle. What it does
    not measure is edge — demo liquidity is not production liquidity, so a
    demo fill price says nothing about what production would have filled at.
    """

    name = "DEMO"
    venue = "Kalshi demo exchange"

    def __init__(self, bot):
        import kalshi_client as kc
        self.kc = kc
        self.bot = bot
        self.rest = kc.Rest("demo", kc.load_key("demo"))  # raises KeyMissing with the env var names

    async def take(self, order):
        order["arrive_ts"] = time.time()
        try:
            resp, ack = await asyncio.to_thread(self.kc.place_ioc, self.rest, order["ticker"], order["side"],
                                                order["limit"], order["contracts"])
        except Exception as e:
            order.update(status="missed", note=f"demo error: {e}"[:200], ack_ms=None)
            return order
        order["ack_ms"] = ack
        filled = float(resp.get("fill_count") or 0)
        if filled <= 0:
            order.update(status="missed", note="IOC not filled on demo book")
            return order
        px = float(resp["average_fill_price"])
        px = px if order["side"] == "yes" else round(1 - px, 4)  # V2 reports the YES-book price
        fee = float(resp.get("average_fee_paid") or 0) * filled
        order.update(status="filled", contracts=filled, fill_px=px, fee=fee, note="demo fill (demo liquidity)")
        return order

    async def result(self, ticker):
        return (await asyncio.to_thread(self.rest.request, "GET", f"/markets/{ticker}"))[0]["market"].get("result")


LIVE_ORDER_USD = 1.00         # most one live order can cost
LIVE_MARKET_USD = 2.00        # most at risk in one 15-minute market
LIVE_DAILY_LOSS_USD = 5.00    # UTC day; breakers are enforced in live mode
LIVE_TOTAL_LOSS_USD = 10.00   # lifetime of live.sqlite: settled losses + money in open positions
LIVE_MAX_ORDERS_PER_DAY = 40
LIVE_MAX_ERRORS = 3           # consecutive order errors before live trading stops itself
ARM_FILE = HERE / "LIVE_ARMED"   # written by the arming script (not published) when the operator turns live on
STOP_FILE = HERE / "LIVE_STOP"   # kill switch: dashboard button, the loss stop, or a reconcile mismatch


class LiveBroker:
    """REAL-MONEY immediate-or-cancel orders on Kalshi production, with hard caps.

    Built 2026-09-15 by decision, to run with $10. Nothing here claims an edge: the live
    latency test measured +0.75c +/-3.6 per contract. The caps exist so the most this can ever cost
    is the $10 budget set for the live test, whatever the bot does:

      * refuses to start unless LIVE_ARMED exists (the operator's switch) and the TRADING key loads
      * $1 an order, $2 a market, $5 a UTC day, 40 orders a day (risk.py enforces the first three)
      * lifetime stop: settled losses + cost of open positions reach $10 -> LIVE_STOP, for good
      * kill switch: LIVE_STOP (dashboard button) stops every new order within one evaluation
      * every order re-prices its edge with Kalshi's per-order fee ROUNDING before it is sent
      * three order errors in a row -> LIVE_STOP (an order that timed out may still have filled)
      * once a minute, positions from Kalshi must match live.sqlite; any mismatch -> LIVE_STOP
    """

    name = "LIVE"
    venue = "Kalshi production · REAL MONEY"

    def __init__(self, bot, rest=None):
        import kalshi_client as kc
        self.kc, self.bot = kc, bot
        if rest is None:
            if not ARM_FILE.exists():
                raise RuntimeError("live mode is not armed (no LIVE_ARMED file); use the arming script (not published)")
            rest = kc.Rest("prod", kc.load_key("trade"), live_orders=True)  # raises KeyMissing with var names
        self.rest = rest
        self.errors = 0
        self.balance = None
        self.reconcile_note = None

    # -- state

    def stopped(self):
        return STOP_FILE.read_text().strip() if STOP_FILE.exists() else None

    def stop(self, reason):
        if not STOP_FILE.exists():
            STOP_FILE.write_text(f"{reason} ({time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())})\n")
            self.bot.say("warn", f"LIVE TRADING STOPPED: {reason}")

    def lifetime_loss(self):
        """Settled P&L plus the full cost of positions not yet settled (they can go to zero)."""
        settled = self.bot.db.execute("SELECT COALESCE(SUM(pnl),0) FROM orders WHERE status='settled'").fetchone()[0]
        open_cost = self.bot.db.execute("SELECT COALESCE(SUM(contracts*fill_px + COALESCE(fee,0)),0) FROM orders "
                                        "WHERE status='filled'").fetchone()[0]
        return settled - open_cost

    def gate(self, now):
        """None if a new order may be sent, else the reason it may not."""
        reason = self.stopped()
        if reason:
            return f"stopped: {reason}"
        if self.lifetime_loss() <= -LIVE_TOTAL_LOSS_USD:
            self.stop(f"lifetime loss reached ${LIVE_TOTAL_LOSS_USD:.0f}")
            return "stopped: lifetime loss limit"
        day0 = now - now % 86400
        n = self.bot.db.execute("SELECT COUNT(*) FROM orders WHERE signal_ts >= ? AND status IN ('filled','settled','missed','error')",
                                (day0,)).fetchone()[0]
        if n >= LIVE_MAX_ORDERS_PER_DAY:
            return f"{LIVE_MAX_ORDERS_PER_DAY} live orders today"
        if self.balance is not None and self.balance < 0.05:
            return f"Kalshi balance ${self.balance:.2f}"
        return None

    def status(self):
        return {"balance": self.balance, "lifetime_pnl": self.lifetime_loss(), "stopped": self.stopped(),
                "reconcile": self.reconcile_note,
                "caps": {"order": LIVE_ORDER_USD, "market": LIVE_MARKET_USD, "day": LIVE_DAILY_LOSS_USD,
                         "lifetime": LIVE_TOTAL_LOSS_USD, "orders_per_day": LIVE_MAX_ORDERS_PER_DAY}}

    # -- orders

    async def take(self, order):
        order["arrive_ts"] = time.time()
        reason = self.gate(order["signal_ts"])
        if reason:
            order.update(status="missed", note=f"live gate: {reason}", ack_ms=None)
            return order
        n, limit = int(order["contracts"]), order["limit"]
        fee = self.kc.order_fee(limit, n)
        if order["fair"] - limit - fee / n < 0:
            order.update(status="missed", note=f"edge gone after fee rounding ({fee:.2f} on {n})", ack_ms=None)
            return order
        try:
            resp, ack = await asyncio.to_thread(self.kc.place_ioc, self.rest, order["ticker"], order["side"], limit, n)
        except Exception as e:
            self.errors += 1
            order.update(status="error", note=f"live order error: {e}"[:200], ack_ms=None)
            if self.errors >= LIVE_MAX_ERRORS:
                self.stop(f"{self.errors} order errors in a row")
            return order
        self.errors = 0
        order["ack_ms"] = ack
        filled = int(float(resp.get("fill_count") or 0))
        if filled <= 0:
            order.update(status="missed", note="IOC not filled")
            return order
        px = float(resp["average_fill_price"])
        px = px if order["side"] == "yes" else round(1 - px, 4)  # V2 reports the YES-book price
        paid = float(resp.get("average_fee_paid") or 0) * filled
        order.update(status="filled", contracts=filled, fill_px=px, fee=paid, note=f"live fill {resp.get('order_id', '')}"[:200])
        return order

    async def result(self, ticker):
        return (await asyncio.to_thread(self.rest.request, "GET", f"/markets/{ticker}"))[0]["market"].get("result")

    # -- reconciliation

    def fetch_account(self):
        """(balance $, {ticker: position}) from Kalshi. YES positive, NO negative. Network only."""
        bal, _ = self.rest.request("GET", "/portfolio/balance", signed=True)
        pos, _ = self.rest.request("GET", "/portfolio/positions?count_filter=position&limit=200", signed=True)
        return bal.get("balance", 0) / 100, {p["ticker"]: float(p.get("position_fp") or p.get("position") or 0)
                                             for p in pos.get("market_positions", [])}

    def mismatches(self, theirs, current_ticker):
        """Compare Kalshi's positions with live.sqlite's unsettled fills. A position Kalshi has already
        settled to zero in a CLOSED market is fine (settle() catches up within minutes); anything else
        that differs is an order the bot doesn't know about — a timeout that filled, a trade placed by
        hand in the Kalshi app, or a parsing bug."""
        mine = {}
        for ticker, side, n in self.bot.db.execute("SELECT ticker, side, SUM(contracts) FROM orders WHERE status='filled' "
                                                   "GROUP BY ticker, side"):
            mine[ticker] = mine.get(ticker, 0.0) + (n if side == "yes" else -n)
        bad = []
        for t in sorted(set(theirs) | set(mine)):
            k, b = theirs.get(t, 0.0), mine.get(t, 0.0)
            if abs(k - b) > 1e-6 and not (k == 0 and t != current_ticker):
                bad.append(f"{t}: Kalshi {k:+.0f}, bot {b:+.0f}")
        return bad

    async def reconcile(self):
        """Once a minute. A mismatch must show on two checks in a row before it stops trading: a fill
        can land on Kalshi a moment before the bot has written it down."""
        strikes = 0
        while True:
            try:
                self.balance, theirs = await asyncio.to_thread(self.fetch_account)
                bad = self.mismatches(theirs, (self.bot.market or {}).get("ticker"))
                strikes = strikes + 1 if bad else 0
                self.reconcile_note = ("; ".join(bad) if bad else f"positions match Kalshi ({time.strftime('%H:%M:%S')} UTC)")
                if strikes >= 2:
                    self.stop(f"positions do not match Kalshi: {'; '.join(bad)}"[:300])
            except Exception as e:
                self.reconcile_note = f"reconcile failed: {e}"[:160]
            await asyncio.sleep(60)


class Bot:
    def __init__(self, broker="paper"):
        import kalshi_client as kc
        self.kc = kc
        # live orders get their own database: real money never mixes with the paper record, and the
        # live risk budget and lifetime loss stop count only live fills
        self.db = sqlite3.connect(HERE / ("live.sqlite" if broker == "live" else "paper.sqlite"), check_same_thread=False)
        self.db.execute("""CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, ticker TEXT, side TEXT,
            signal_ts REAL, arrive_ts REAL, limit_px REAL, fill_px REAL, contracts REAL, fee REAL,
            fair REAL, edge REAL, status TEXT, note TEXT, pnl REAL, settled_ts REAL)""")
        # the basis is measured once, in the minute before an open; persist it so a
        # restart mid-window can keep pricing instead of going blind for ~15 minutes
        self.db.execute("CREATE TABLE IF NOT EXISTS basis (ticker TEXT PRIMARY KEY, basis REAL, measured_ts REAL)")
        if "feed" not in {r[1] for r in self.db.execute("PRAGMA table_info(basis)")}:
            self.db.execute("ALTER TABLE basis ADD COLUMN feed TEXT")
        have = {r[1] for r in self.db.execute("PRAGMA table_info(orders)")}
        # ack/broker/strategy, then the autopsy fields: the conditions each order was placed under
        for col, typ in (("ack_ms", "REAL"), ("broker", "TEXT"), ("strategy", "TEXT"), ("d_fair", "REAL"),
                         ("lag", "REAL"), ("secs_left", "REAL"), ("spread", "REAL"), ("depth", "REAL"),
                         ("sigma", "REAL"), ("latency_ms", "REAL"), ("venues", "INTEGER"),
                         ("x5", "REAL"), ("x10", "REAL"), ("x30", "REAL")):  # shadow exit P&L at the real bid
            if col not in have:
                self.db.execute(f"ALTER TABLE orders ADD COLUMN {col} {typ}")
        self.db.commit()
        self.rest = kc.Rest("prod")  # public market data, kept-alive; GET only by construction
        self.broker = {"demo": DemoBroker, "live": LiveBroker}.get(broker, PaperBroker)(self)
        try:
            self.prod_key = kc.load_key("prod")  # read-only websocket book, if configured
        except kc.KeyMissing:
            self.prod_key = None
        self.book_source = "REST poll (kept-alive)"
        self.rate_limited = None
        self.settling = set()
        self.coin = {"bid": None, "ask": None, "mid": None, "ts": 0.0}
        self.sec_px = {}                  # epoch second -> Coinbase mid as of the start of that second
        self.market = None
        self.book = {"yes_bid": None, "yes_ask": None, "no_bid": None, "no_ask": None,
                     "yes_ask_sz": 0, "no_ask_sz": 0, "ts": 0.0, "rtt_ms": None}
        self.model = {"fair": None, "sigma": None, "sigma_src": None, "basis": None, "S": None,
                      "edge_yes": None, "edge_no": None}
        self.series = deque(maxlen=1800)  # (t, btc_adj, fair, kalshi_mid)
        self.log = deque(maxlen=300)
        self.last_order = {"yes": 0.0, "no": 0.0}
        self.snapshot = {}
        # instrumentation for the pipeline view: every stage counts what passes through it
        self.started = time.time()
        self.host = socket.gethostname()
        self.stats = {"coin_msgs": deque(maxlen=5000), "book_msgs": deque(maxlen=20000),
                      "evals": deque(maxlen=20000), "eval_us": deque(maxlen=500),
                      "coin_lat_ms": deque(maxlen=500), "book_lat_ms": deque(maxlen=500),
                      "totals": {"coin": 0, "book": 0, "evals": 0}}
        self.tape = deque(maxlen=80)           # live events: btc trades, book top changes, signals
        self.fair_hist = deque(maxlen=4000)    # (t, fair, kalshi_mid) for the 1 s trigger view
        self.last_top = None
        self.recorder_stats = {}
        self.recorder_checked = 0.0
        self.order_rtt = deque(maxlen=30)
        self.sim_latency_ms = SIM_LATENCY_MS
        self.loop_lag = deque(maxlen=600)
        self.cpu_pct = None
        self.spot = SpotProcess()          # BTC venues parse in their own process, on the other core
        self.spot_snapshot = self.spot.read()
        self.kbook = KalshiBookProcess(bool(self.prod_key))
        self.kbook_snapshot = self.kbook.read()
        self.proc_cpu = {}                 # Linux: CPU % per pm_hf process + host steal, from /proc
        self.last_btc_eval = 0.0
        self.btc_warned = {}
        # paper: breakers report "would pause" but don't stop the test; any real-order broker enforces them
        self.risk = (RiskManager(self.db, enforce_breakers=True, order_usd=LIVE_ORDER_USD, market_usd=LIVE_MARKET_USD,
                                 daily_loss_usd=LIVE_DAILY_LOSS_USD, scale_by_edge=False) if broker == "live"
                     else RiskManager(self.db, enforce_breakers=(broker != "paper")))
        self.block_logged = {}
        self.autopsy = {}
        self.autopsy_at = 0.0

    def say(self, level, msg):
        self.log.append({"t": time.time(), "level": level, "msg": msg})
        if level in ("trade", "fill", "miss", "settle"):
            self.tape.append({"t": time.time(), "kind": level, "text": msg})

    # ---- feeds

    async def btc_poll(self):
        """Read the composite BTC price from the spot-feeds process every 5 ms. The child does all
        parsing on its own core; this loop only copies a few doubles, then re-checks the trigger when
        the price changed. 5 ms polling is far inside the ~100 ms the edge survives."""
        last_count, last_err_check = 0, 0.0
        while True:
            await asyncio.sleep(0.005)
            snap = self.spot.read()
            now = time.time()
            if snap["count"] != last_count:
                for _ in range(min(snap["count"] - last_count, 200)):  # keep the dashboard's rate honest
                    self.stats["coin_msgs"].append(now)
                self.stats["totals"]["coin"] = snap["count"]
                last_count = snap["count"]
                if snap["mid"] is not None:
                    self.coin.update(mid=snap["mid"], ts=snap["ts"], venues=snap["n"])
                    self.maybe_evaluate(now)
            self.spot_snapshot = snap
            for recv, stamp, side, size, price in self.spot.drain(self.spot.trades):
                self.on_btc_trade(recv, stamp, side, size, price)
            if now - last_err_check > 1:
                last_err_check = now
                for venue, err in self.spot.drain(self.spot.errors, 20):
                    self.on_btc_error(venue, err)
                if not self.spot.alive() and now - self.btc_warned.get("process", 0) > 30:
                    # without BTC the stale-input guard stops all trading; say why, loudly
                    self.btc_warned["process"] = now
                    self.say("warn", "BTC feed process died: restarting it")
                    self.spot = SpotProcess()
                    self.spot.start()

    def maybe_evaluate(self, now):
        """One shared throttle for BTC- and Kalshi-driven evaluations: at most one every 10 ms.
        Kalshi alone sends ~760 book updates/s; evaluating on each (~120 us of model + bookkeeping)
        on top of 600+ BTC updates/s was part of what overloaded the server. 10 ms is far inside the
        ~100 ms the edge survives."""
        if now - self.last_btc_eval >= 0.010:
            self.last_btc_eval = now
            self.evaluate()

    def on_btc_trade(self, recv, stamp, side, size, price):
        if stamp:
            # exchange stamp -> receipt in the child process. Meaningful only with a synced clock (server).
            self.stats["coin_lat_ms"].append((recv - _iso_frac(stamp)) * 1000)
        self.tape.append({"t": recv, "kind": "btc", "text": f"BTC {side.upper():<4} {size:.4f} @ {price:,.2f}"})

    def on_btc_error(self, venue, err):
        now = time.time()
        if now - self.btc_warned.get(venue, 0) > 30:
            self.btc_warned[venue] = now
            self.say("warn", f"{venue} book dropped, reconnecting: {err}"[:160])

    async def sampler(self):
        """Record the price as of the start of every second — the grid the model and settlement use."""
        while True:
            now = time.time()
            await asyncio.sleep(math.ceil(now) - now)
            s = int(round(time.time()))
            if self.coin["mid"] and time.time() - self.coin["ts"] < 5:
                self.sec_px[s] = self.coin["mid"]
            for old in [k for k in self.sec_px if k < s - 4000]:
                del self.sec_px[old]

    async def bootstrap_sigma(self):
        """Sigma at startup. Every deploy restarts the bot, and its own price history starts empty,
        so for ~55 minutes sigma used to come from 1-minute candles — not the backtest's estimator.
        The recorder on the same machine already holds the last hour of Coinbase trades: build the
        backtest's 10-second realized vol from those first, and fall back to candles only if absent."""
        path = HERE / "kalshi_live.sqlite"
        if path.exists():
            try:
                db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
                since = int((time.time() - 3600) * 1e9)
                rows = db.execute("SELECT recv_ns, price FROM spot WHERE src='coinbase' AND recv_ns > ? ORDER BY recv_ns",
                                  (since,)).fetchall()
                db.close()
                grid, j, last = [], 0, None
                for g in range(since, time.time_ns(), 10 * 10**9):
                    while j < len(rows) and rows[j][0] <= g:
                        last = rows[j][1]
                        j += 1
                    if last is not None:
                        grid.append(last)
                if len(grid) >= 300:
                    self.model["sigma"] = realized_vol(grid, dt_seconds=10)
                    self.model["sigma_src"] = "10s realized, 1h, from recorder"
                    return
            except sqlite3.Error as e:
                self.say("warn", f"recorder sigma unavailable: {e}"[:160])
        try:
            c = await asyncio.to_thread(_get, "https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=60")
            closes = [row[4] for row in sorted(c)[-61:]]
            self.model["sigma"] = realized_vol(closes, dt_seconds=60)
            self.model["sigma_src"] = "1m candles (bootstrap)"
        except Exception as e:
            self.say("warn", f"sigma bootstrap failed: {e!r}"[:160])

    def update_sigma(self):
        now = int(time.time())
        grid = [self.sec_px.get(s) for s in range(now - 3600, now + 1, 10)]
        grid = [p for p in grid if p]
        if len(grid) >= 330:  # ~55 minutes of our own data: switch to the backtest's estimator
            self.model["sigma"] = realized_vol(grid, dt_seconds=10)
            self.model["sigma_src"] = "10s realized, 1h (backtest's)"

    async def markets(self):
        while True:
            try:
                if self.market is None or time.time() >= self.market["close"]:
                    if self.market and self.market["ticker"] not in self.settling:
                        self.settling.add(self.market["ticker"])
                        asyncio.create_task(self.settle(dict(self.market)))
                    d = await asyncio.to_thread(_get, f"{KALSHI}/markets?series_ticker=KXBTC15M&status=open&limit=5")
                    # Kalshi keeps a just-closed market in status=open for ~15s (seen 2026-09-13):
                    # filter on close_time, or the bot re-selects it and re-settles it every 2s
                    ms = sorted((m for m in d["markets"] if _iso(m["close_time"]) > time.time() + 1),
                                key=lambda m: m["close_time"])
                    if ms:
                        m = ms[0]
                        self.market = {"ticker": m["ticker"], "open": int(_iso(m["open_time"])),
                                       "close": int(_iso(m["close_time"])), "strike": m.get("floor_strike")}
                        self.model["basis"] = None
                        self.say("info", f"now trading {m['ticker']}  strike {m.get('floor_strike')}")
            except Exception as e:
                self.say("warn", f"market lookup failed: {e!r}"[:160])
            await asyncio.sleep(2)

    async def orderbook(self):
        """Kalshi book. With a production key: the websocket runs in its own process
        (kalshi_feed.KalshiBookProcess) and this loop reads its top of book every 5 ms.
        Main-process websocket parsing cost 39-78% of a core on the first (burstable) server at 370-976 deltas/s
        (2026-09-13), too close to one core for weekday volume. Without a key: REST polling."""
        if not self.prod_key:
            await self.rest_book(until=float("inf"))
            return
        last_count, disconnected_since, last_warn = 0, None, 0.0
        while True:
            await asyncio.sleep(0.005)
            now = time.time()
            if self.market:
                self.kbook.follow(self.market["ticker"])
            if not self.kbook.alive():
                self.say("warn", "Kalshi book process died: restarting it")
                self.kbook = KalshiBookProcess(True)
                self.kbook.start()
                continue
            snap = self.kbook.read()
            self.kbook_snapshot = snap
            if now - last_warn > 30:
                errs = self.kbook.drain_errors()
                if errs:
                    last_warn = now
                    self.say("warn", f"Kalshi websocket: {errs[-1]} ({len(errs)} recent)"[:200])
            if not snap["connected"]:
                disconnected_since = disconnected_since or now
                if now - disconnected_since > 5:
                    # websocket down for 5 s: poll REST until it comes back, never trade blind
                    self.say("warn", "Kalshi websocket down 5 s: polling REST until it reconnects")
                    await self.rest_book(until=now + 15)
                continue
            disconnected_since = None
            self.book_source = "Kalshi websocket"
            if snap["count"] != last_count:
                n = snap["count"] - last_count
                last_count = snap["count"]
                for _ in range(min(n, 400)):
                    self.stats["book_msgs"].append(now)
                self.stats["totals"]["book"] = snap["count"]
                if snap["lat_p50"] is not None:
                    self.stats["book_lat_ms"].append(snap["lat_p50"])
                self.book.update(**{f: snap[f] for f in KFIELDS}, ts=snap["ts"], rtt_ms=None)
                self.note_top()
                self.maybe_evaluate(now)
            else:
                self.book["ts"] = snap["ts"]  # socket alive and quiet: the book is current

    async def rest_book(self, until):
        self.book_source = "REST poll (kept-alive)"
        last_warn = 0.0
        while time.time() < until:
            m = self.market
            if m:
                try:
                    ob, rtt = await asyncio.to_thread(self.rest.request, "GET", f"/markets/{m['ticker']}/orderbook?depth=10")
                    self.book.update(**self.kc.orderbook_top(ob["orderbook_fp"]), ts=time.time(), rtt_ms=rtt)
                    self.stats["book_msgs"].append(time.time())
                    self.stats["totals"]["book"] += 1
                    self.stats["book_lat_ms"].append(rtt)
                    self.note_top()
                    self.rate_limited = None
                    self.evaluate()
                except self.kc.RateLimited as e:
                    self.rate_limited = str(e)
                    if time.time() - last_warn > 10:  # one line, not one per poll
                        self.say("warn", f"Kalshi rate limit: {e}")
                        last_warn = time.time()
                except Exception as e:
                    self.say("warn", f"orderbook failed: {e!r}"[:160])
            await asyncio.sleep(BOOK_POLL_S)

    def note_top(self):
        """Put top-of-book CHANGES on the live tape (the raw deltas are ~600/s: too many to show)."""
        b = self.book
        top = (b["yes_bid"], b["yes_ask"])
        if top != self.last_top and None not in top:
            if self.last_top and None not in self.last_top:
                arrow = "▲" if top[0] > self.last_top[0] else "▼" if top[0] < self.last_top[0] else "•"
            else:
                arrow = "•"
            self.tape.append({"t": time.time(), "kind": "book",
                              "text": f"KALSHI {arrow} YES {top[0]:.3f} / {top[1]:.3f}"})
            self.last_top = top

    # ---- model and decisions

    def evaluate(self):
        t0 = time.perf_counter()
        try:
            self._evaluate()
        finally:
            self.stats["evals"].append(time.time())
            self.stats["eval_us"].append((time.perf_counter() - t0) * 1e6)
            self.stats["totals"]["evals"] += 1

    def _chart_point(self, now, S, fair, mid):
        """2 points/s for the dashboard charts. Recorded even when the model can't price yet
        (no basis after a restart mid-market), so the BTC and Kalshi lines never go blank —
        on 2026-09-13 the charts sat empty for up to 15 minutes after every deploy."""
        if not self.series or now - self.series[-1][0] >= 0.5:
            self.series.append((now, S, fair, mid))

    def _evaluate(self):
        m, now = self.market, time.time()
        if not m or m["strike"] is None or not self.coin["mid"] or not self.model["sigma"]:
            return
        b0 = self.book
        kalshi_mid = (b0["yes_bid"] + b0["yes_ask"]) / 2 if b0["yes_bid"] is not None and b0["yes_ask"] is not None else None
        if self.model["basis"] is None:
            pre = [self.sec_px.get(s) for s in range(m["open"] - 59, m["open"] + 1)]
            # A basis only means something for the price feed it was measured on. On 2026-09-13 a
            # Coinbase-only basis (-11.84) was restored after the switch to the 4-venue median, whose
            # basis for the next market measured -0.94: the model ran ~$11 off for a whole market.
            saved = self.db.execute("SELECT basis FROM basis WHERE ticker=? AND feed=?", (m["ticker"], FEED_ID)).fetchone()
            if all(pre):
                self.model["basis"] = sum(pre) / 60 - m["strike"]
                self.db.execute("INSERT OR REPLACE INTO basis (ticker, basis, measured_ts, feed) VALUES (?,?,?,?)",
                                (m["ticker"], self.model["basis"], now, FEED_ID))
                self.db.commit()
                self.say("info", f"basis measured: BTC median - BRTI strike = {self.model['basis']:+.2f}")
            elif saved:
                self.model["basis"] = saved[0]
                self.say("info", f"basis restored from this market's pre-open measurement: {saved[0]:+.2f}")
            else:
                self.model["fair"] = None
                # started mid-window: no measured basis, so no price. Never guess it — but keep
                # charting the raw BTC median and Kalshi's mid so the dashboard isn't blank
                self._chart_point(now, self.coin["mid"], None, kalshi_mid)
                return
        basis = self.model["basis"]
        times = settle_times(m["open"], m["close"] - m["open"], 60)
        known = {s: self.sec_px[s] - basis for s in times if s in self.sec_px and s <= now}
        S = self.coin["mid"] - basis
        fair = fair_up(now, times, known, S, m["strike"], self.model["sigma"])[0]
        b = self.book
        ey = fair - b["yes_ask"] - taker_fee(b["yes_ask"]) if b["yes_ask"] is not None else None
        en = (1 - fair) - b["no_ask"] - taker_fee(b["no_ask"]) if b["no_ask"] is not None else None
        self.model.update(fair=fair, S=S, edge_yes=ey, edge_no=en)
        mid = (b["yes_bid"] + b["yes_ask"]) / 2 if b["yes_bid"] is not None and b["yes_ask"] is not None else None
        # evaluate runs on every websocket delta (~600/s); the charts keep 2 points/s so
        # 1,800 points still span 15 minutes, and the trigger history keeps 20/s
        self._chart_point(now, S, fair, mid)
        if not self.fair_hist or now - self.fair_hist[-1][0] >= 0.05:
            self.fair_hist.append((now, fair, mid, m["ticker"]))
        if now - self.coin["ts"] > 3 or now - b["ts"] > 2:
            return  # stale inputs never trade
        if not (m["open"] < now < m["close"] - 1):
            return  # never trade a market outside its own window
        if self.overloaded():
            # a late-waking loop means every input is older than it looks: don't enter
            if now - self.block_logged.get("overloaded", 0) > 30:
                self.block_logged["overloaded"] = now
                self.risk.blocks["bot overloaded (loop lag)"] = self.risk.blocks.get("bot overloaded (loop lag)", 0) + 1
                self.say("warn", f"skipping signals: bot overloaded, loop lag p90 > {OVERLOAD_LAG_MS} ms, CPU {self.cpu_pct or 0:.0f}%")
            return
        if STRATEGY == "news":
            past = next((x for x in reversed(self.fair_hist) if now - x[0] >= 1.0), None)
            # The reference must be ~1 s old AND from this market. Without both checks, the first
            # evaluation after a rollover or a data gap compared against a previous market's price or
            # a minutes-old one, and a stale jump would read as fresh "news".
            if (past is None or past[1] is None or past[2] is None or mid is None
                    or past[3] != m["ticker"] or now - past[0] > TRIGGER_MAX_LOOKBACK_S):
                return
            d_fair = fair - past[1]
            lag = d_fair - (mid - past[2])
            self.model.update(d_fair=d_fair, lag=lag)
            wants = {"yes": d_fair >= TRIGGER_DELTA and lag >= TRIGGER_DELTA / 2 and ey is not None and ey >= 0,
                     "no": d_fair <= -TRIGGER_DELTA and lag <= -TRIGGER_DELTA / 2 and en is not None and en >= 0}
            cooldown = TRIGGER_COOLDOWN_S
        else:
            wants = {"yes": ey is not None and ey >= THRESHOLD, "no": en is not None and en >= THRESHOLD}
            cooldown = COOLDOWN_S
        live_block = self.broker.gate(now) if self.broker.name == "LIVE" and any(wants.values()) else None
        for side, edge, ask in (("yes", ey, b["yes_ask"]), ("no", en, b["no_ask"])):
            if not wants[side] or now - self.last_order[side] < cooldown:
                continue
            if live_block:
                self.last_order[side] = now
                if now - self.block_logged.get("live", 0) > 30:
                    self.block_logged["live"] = now
                    self.say("info", f"live signal {side.upper()} @ {ask:.2f} not sent: {live_block}")
                continue
            allowed, contracts, reason = self.risk.check(now, m["ticker"], side, ask, edge)
            if not allowed:
                self.last_order[side] = now  # a blocked signal also waits out the cooldown
                if now - self.block_logged.get(reason, 0) > 30:  # one log line per reason per 30 s
                    self.block_logged[reason] = now
                    self.say("info", f"risk blocked {side.upper()} @ {ask:.2f}: {reason}")
                # Shadow trade: what would this blocked signal have made? Recorded at the ask it saw
                # (no latency haircut, so if anything flattering to the blocked side), sized like a
                # real order, settled at the close, and kept out of every real P&L and risk figure.
                # Answers "are the rules costing us money?" on live data, not only the backtest.
                size = max(1, math.floor(10.0 * max(0.25, min(1.0, edge / 0.05)) / max(ask, 0.01)))
                self.db.execute("INSERT INTO orders (ticker, side, signal_ts, limit_px, fill_px, contracts, fee, fair, edge, "
                                "status, note, broker, strategy) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (m["ticker"], side, now, ask, ask, size, taker_fee(ask) * size,
                                 fair if side == "yes" else 1 - fair, edge, "blocked", reason, "SHADOW", STRATEGY))
                self.db.commit()
                continue
            self.last_order[side] = now
            spread = (b["yes_ask"] - b["yes_bid"]) if b["yes_ask"] is not None and b["yes_bid"] is not None else None
            order = {"ticker": m["ticker"], "side": side, "signal_ts": now, "limit": ask,
                     "contracts": contracts, "fair": fair if side == "yes" else 1 - fair,
                     "edge": edge, "status": "pending", "strategy": STRATEGY,
                     # autopsy: the conditions this order was placed under
                     "d_fair": self.model.get("d_fair"), "lag": self.model.get("lag"), "secs_left": m["close"] - now,
                     "spread": spread, "depth": b["yes_ask_sz"] if side == "yes" else b["no_ask_sz"],
                     "sigma": self.model["sigma"], "latency_ms": self.sim_latency_ms, "venues": self.coin.get("venues")}
            why = (f"model moved {100 * self.model['d_fair']:+.1f}c in 1s, Kalshi lagged {100 * self.model['lag']:+.1f}c"
                   if STRATEGY == "news" else "edge over threshold")
            self.say("trade", f"SIGNAL buy {side.upper()} @ {ask:.2f}  fair {order['fair']:.3f}  edge {100 * edge:+.1f}c  ({why})")
            asyncio.get_running_loop().create_task(self.execute(order))

    async def execute(self, order):
        order = await self.broker.take(order)
        self.db.execute("INSERT INTO orders (ticker, side, signal_ts, arrive_ts, limit_px, fill_px, contracts, fee, fair, "
                        "edge, status, note, ack_ms, broker, strategy, d_fair, lag, secs_left, spread, depth, sigma, "
                        "latency_ms, venues) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (order["ticker"], order["side"], order["signal_ts"], order.get("arrive_ts"), order["limit"],
                         order.get("fill_px"), order["contracts"] if order["status"] == "filled" else 0,
                         order.get("fee"), order["fair"], order["edge"], order["status"], order.get("note"),
                         order.get("ack_ms"), self.broker.name, order.get("strategy"), order.get("d_fair"),
                         order.get("lag"), order.get("secs_left"), order.get("spread"), order.get("depth"),
                         order.get("sigma"), order.get("latency_ms"), order.get("venues")))
        order_id = self.db.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.db.commit()
        ack = f"{order['ack_ms']:.0f} ms" if order.get("ack_ms") is not None else "no ack"
        if order["status"] == "filled":
            self.say("fill", f"FILLED {order['contracts']:.0f} {order['side'].upper()} @ {order['fill_px']:.2f}  [{self.broker.name}, {ack}]")
            asyncio.get_running_loop().create_task(self.shadow_exits(order_id, order))
        else:
            self.say("miss", f"MISSED {order['side'].upper()} @ {order['limit']:.2f}: {order['note']}  [{self.broker.name}, {ack}]")

    async def shadow_exits(self, order_id, order):
        """What would SELLING this position have paid, N seconds after the signal, at the real bid?
        Recorded next to the hold-to-settlement result; the bot's behaviour does not change.

        kalshi_exit.py (30 days, 2,805 markets) found exits cut the P&L swing ~4x but give up about
        half the edge to the second spread + fee (at 0 ms: hold +3.51c +/-0.69, sell at 10 s +1.47c
        +/-0.18). Its exit prices were last trades minus half a cent; this measures the real bid."""
        start = order["signal_ts"]
        for n, col in ((5, "x5"), (10, "x10"), (30, "x30")):
            await asyncio.sleep(max(0.0, start + n - time.time()))
            m, b = self.market, self.book
            if not m or m["ticker"] != order["ticker"] or time.time() - b["ts"] > 2:
                return  # market rolled over (a hold would settle) or no live book: record nothing
            bid = b["yes_bid"] if order["side"] == "yes" else b["no_bid"]
            if bid is None:
                continue
            pnl = order["contracts"] * (bid - order["fill_px"]) - order["fee"] - taker_fee(bid) * order["contracts"]
            self.db.execute(f"UPDATE orders SET {col}=? WHERE id=?", (pnl, order_id))
            self.db.commit()

    def exits_view(self):
        """Hold vs shadow exits over the SAME settled orders (only those with all three exits recorded)."""
        rows = self.db.execute("SELECT contracts, pnl, x5, x10, x30, ticker FROM orders WHERE status='settled' AND strategy=? "
                               "AND x5 IS NOT NULL AND x10 IS NOT NULL AND x30 IS NOT NULL", (STRATEGY,)).fetchall()
        n = sum(r[0] for r in rows)
        if not n:
            return {"orders": 0}
        out = {"orders": len(rows), "markets": len({r[5] for r in rows}), "contracts": n}
        for i, name in ((1, "hold"), (2, "sell 5s"), (3, "sell 10s"), (4, "sell 30s")):
            out[name] = 100 * sum(r[i] for r in rows) / n
        return out

    async def settle(self, m):
        for _ in range(60):
            await asyncio.sleep(20)
            try:
                r = await self.broker.result(m["ticker"])  # paper: production result; demo: demo's
            except Exception:
                continue
            if r in ("yes", "no"):
                rows = self.db.execute("SELECT id, side, fill_px, contracts, fee FROM orders WHERE ticker=? AND status='filled'",
                                       (m["ticker"],)).fetchall()
                total = 0.0
                for oid, side, px, n, fee in rows:
                    pnl = n * ((1.0 if side == r else 0.0) - px) - fee
                    total += pnl
                    self.db.execute("UPDATE orders SET status='settled', pnl=?, settled_ts=? WHERE id=?", (pnl, time.time(), oid))
                shadow = self.db.execute("SELECT id, side, fill_px, contracts, fee FROM orders WHERE ticker=? AND status='blocked'",
                                         (m["ticker"],)).fetchall()
                for oid, side, px, n, fee in shadow:
                    pnl = n * ((1.0 if side == r else 0.0) - px) - fee
                    self.db.execute("UPDATE orders SET status='blocked_settled', pnl=?, settled_ts=? WHERE id=?", (pnl, time.time(), oid))
                self.db.commit()
                self.say("settle", f"{m['ticker']} settled {r.upper()}: {len(rows)} fills, P&L ${total:+.2f}")
                return
        self.say("warn", f"{m['ticker']}: no result after 20 minutes")

    async def settle_orphans(self):
        """Settle filled orders in any market that is not the one being traded. settle() normally runs
        when the bot rolls to the next market; a restart across a close skipped that, and those
        positions would have stayed 'filled' forever, missing from P&L."""
        while True:
            try:
                current = (self.market or {}).get("ticker")
                for (ticker,) in self.db.execute("SELECT DISTINCT ticker FROM orders WHERE status IN ('filled','blocked')").fetchall():
                    if ticker != current and ticker not in self.settling:
                        self.settling.add(ticker)
                        asyncio.create_task(self.settle({"ticker": ticker}))
            except sqlite3.Error:
                pass
            await asyncio.sleep(60)

    async def loop_monitor(self):
        """Is the bot keeping up? Every 100 ms, how late did a 100 ms sleep wake (event-loop lag),
        and what share of a CPU core is this process using. Missed trades caused by an overloaded
        bot are indistinguishable from being outrun unless this is measured."""
        last_cpu, last_wall = time.process_time(), time.time()
        while True:
            t0 = time.perf_counter()
            await asyncio.sleep(0.1)
            self.loop_lag.append((time.perf_counter() - t0 - 0.1) * 1000)
            now = time.time()
            if now - last_wall >= 5:
                self.cpu_pct = 100 * (time.process_time() - last_cpu) / (now - last_wall)
                last_cpu, last_wall = time.process_time(), now

    async def proc_monitor(self):
        """Linux only (server): CPU % per pm_hf process and host-wide steal, from /proc, every 5 s.
        Steal = time the hypervisor ran someone else while we wanted the CPU. On a burstable
        Lightsail plan that has used up its credits, steal is where the throttling shows up —
        it separates "our code is too slow" from "the server is being capped"."""
        import os
        if not os.path.exists("/proc/stat"):
            return
        tick = os.sysconf("SC_CLK_TCK")
        ncpu = os.cpu_count() or 1

        def host_times():
            f = open("/proc/stat").readline().split()[1:]
            v = [int(x) for x in f]
            return v[0] + v[1] + v[2], v[3] + v[4], v[7] if len(v) > 7 else 0, sum(v)  # busy, idle+iowait, steal, total

        def pids():
            named = {"bot": os.getpid(), "btc feeds": self.spot.proc.pid, "kalshi book": getattr(self.kbook.proc, "pid", None)}
            for d in os.listdir("/proc"):
                if d.isdigit():
                    try:
                        if b"kalshi_recorder.py" in open(f"/proc/{d}/cmdline", "rb").read():
                            named["recorder"] = int(d)
                    except OSError:
                        pass
            return {k: v for k, v in named.items() if v}

        def cpu_ticks(pid):
            try:
                parts = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
                return int(parts[11]) + int(parts[12])  # utime + stime
            except (OSError, IndexError, ValueError):
                return None

        last_host, last_proc, last_t = host_times(), {}, time.time()
        while True:
            await asyncio.sleep(5)
            now, host = time.time(), host_times()
            dt, total = now - last_t, host[3] - last_host[3]
            procs = {}
            for name, pid in pids().items():
                t = cpu_ticks(pid)
                if t is not None and (name, pid) in last_proc:
                    procs[name] = 100 * (t - last_proc[(name, pid)]) / tick / dt
                if t is not None:
                    last_proc[(name, pid)] = t
            self.proc_cpu = {"procs": procs, "cores": ncpu,
                             "host_busy_pct": 100 * (host[0] - last_host[0]) / total if total else None,
                             "steal_pct": 100 * (host[2] - last_host[2]) / total if total else None,
                             "loadavg": os.getloadavg()[0]}
            last_host, last_t = host, now

    def overloaded(self):
        """True when the loop has been waking late: decisions and fill checks would be stale."""
        if len(self.loop_lag) < 20:
            return False
        recent = sorted(list(self.loop_lag)[-50:])
        return recent[int(0.9 * (len(recent) - 1))] > OVERLOAD_LAG_MS

    async def latency_probe(self):
        """Kept-alive request to Kalshi every 10 s: the round trip an order would pay from here."""
        while True:
            try:
                _, rtt = await asyncio.to_thread(self.rest.request, "GET", "/exchange/status")
                self.order_rtt.append(rtt)
                self.sim_latency_ms = max(1.0, statistics.median(self.order_rtt))
            except Exception:
                pass
            await asyncio.sleep(10)

    # ---- dashboard state

    async def publish(self):
        while True:
            self.update_sigma() if int(time.time()) % 30 == 0 else None
            now = time.time()
            rows = self.db.execute("SELECT id, ticker, side, signal_ts, arrive_ts, limit_px, fill_px, contracts, fee, fair, "
                                   "edge, status, note, pnl, ack_ms, broker, strategy FROM orders "
                                   "WHERE status NOT IN ('blocked','blocked_settled') ORDER BY id DESC LIMIT 100").fetchall()
            blocked_by = self.db.execute("SELECT note, COUNT(*), COUNT(pnl), COALESCE(SUM(pnl),0), COALESCE(SUM(contracts),0), "
                                         "COUNT(DISTINCT ticker) FROM orders WHERE status IN ('blocked','blocked_settled') "
                                         "AND strategy=? GROUP BY note ORDER BY COUNT(*) DESC", (STRATEGY,)).fetchall()
            cols = ["id", "ticker", "side", "signal_ts", "arrive_ts", "limit", "fill_px", "contracts", "fee", "fair",
                    "edge", "status", "note", "pnl", "ack_ms", "broker", "strategy"]
            # P&L, curve and counts show ONLY the running strategy: the old level-rule trades were
            # mixed into the news trigger's numbers, which made neither readable. (Risk limits still
            # count every paper trade — safety should not depend on how results are labelled.)
            agg = self.db.execute("SELECT COUNT(*), COALESCE(SUM(pnl),0), COALESCE(SUM(pnl>0),0), "
                                  "COALESCE(SUM(contracts*fill_px),0) FROM orders WHERE status='settled' AND strategy=?",
                                  (STRATEGY,)).fetchone()
            curve = self.db.execute("SELECT settled_ts, SUM(pnl) OVER (ORDER BY settled_ts) FROM orders "
                                    "WHERE status='settled' AND strategy=? ORDER BY settled_ts", (STRATEGY,)).fetchall()
            counts = dict(self.db.execute("SELECT status, COUNT(*) FROM orders WHERE strategy=? GROUP BY status",
                                          (STRATEGY,)).fetchall())
            self.snapshot = {
                "now": now, "broker": self.broker.name, "venue": self.broker.venue,
                "book_source": self.book_source, "rate_limited": self.rate_limited,
                "config": {"threshold": THRESHOLD, "sim_latency_ms": self.sim_latency_ms,
                           "strategy": STRATEGY, "trigger_delta": TRIGGER_DELTA,
                           "latency_measured": bool(self.order_rtt)},
                "coin": self.coin, "market": self.market, "book": self.book, "model": self.model,
                "series": list(self.series)[-900:], "orders": [dict(zip(cols, r)) for r in rows],
                "stats": {"settled": agg[0], "pnl": agg[1], "wins": agg[2], "notional": agg[3], "counts": counts},
                "curve": curve, "log": list(self.log)[-120:],
                "backtest": (HERE / "kalshi_backtest_report.txt").exists(),
                "pipeline": self.pipeline(now), "tape": list(self.tape)[-60:],
                "host": {"name": self.host, "uptime_s": now - self.started, "cpu_pct": self.cpu_pct,
                         "feed_cpu_pct": self.spot_snapshot.get("cpu_pct"),
                         "kalshi_cpu_pct": self.kbook_snapshot.get("cpu_pct"), "procs": self.proc_cpu,
                         "loop_lag_p50": sorted(self.loop_lag)[len(self.loop_lag) // 2] if self.loop_lag else None,
                         "loop_lag_p90": sorted(self.loop_lag)[int(0.9 * (len(self.loop_lag) - 1))] if self.loop_lag else None,
                         "overloaded": self.overloaded()},
                "recorder": self.recorder_view(now),
                "risk": self.risk.view(now, (self.market or {}).get("ticker")),
                "live": self.broker.status() if self.broker.name == "LIVE" else None,
                "blocked_shadow": [{"reason": r_, "signals": n, "settled": ns, "pnl": pnl, "contracts": nc, "markets": nm}
                                   for r_, n, ns, pnl, nc, nm in blocked_by],
                "autopsy": self.autopsy_view(now),
                "exits": self.exits_view(),
                "venues": {v: {"count": d["count"], "mid": d["mid"], "age": now - d["ts"] if d["ts"] else None}
                           for v, d in self.spot_snapshot["venues"].items()},
            }
            await asyncio.sleep(0.5)

    def pipeline(self, now):
        """Per-stage throughput and latency over the last 5 seconds, plus the 1 s trigger quantities."""
        def rate(dq):
            return sum(1 for t in reversed(dq) if now - t <= 5) / 5 if dq else 0.0

        def pct(dq, q):
            xs = sorted(dq)
            return xs[int(q * (len(xs) - 1))] if xs else None

        d_fair = d_mid = None
        if self.fair_hist:
            t1, f1, m1, tk1 = self.fair_hist[-1]
            past = next((x for x in reversed(self.fair_hist) if t1 - x[0] >= 1.0), None)
            if past and past[3] == tk1 and t1 - past[0] <= TRIGGER_MAX_LOOKBACK_S and f1 is not None and past[1] is not None:
                d_fair = f1 - past[1]
                if m1 is not None and past[2] is not None:
                    d_mid = m1 - past[2]
        return {
            "coin": {"rate": rate(self.stats["coin_msgs"]), "total": self.stats["totals"]["coin"],
                     "lat_p50": pct(self.stats["coin_lat_ms"], .5), "lat_p90": pct(self.stats["coin_lat_ms"], .9)},
            "book": {"rate": rate(self.stats["book_msgs"]), "total": self.stats["totals"]["book"],
                     "lat_p50": pct(self.stats["book_lat_ms"], .5), "lat_p90": pct(self.stats["book_lat_ms"], .9)},
            "model": {"rate": rate(self.stats["evals"]), "total": self.stats["totals"]["evals"],
                      "us_p50": pct(self.stats["eval_us"], .5), "us_p90": pct(self.stats["eval_us"], .9)},
            "trigger": {"d_fair_1s": d_fair, "d_mid_1s": d_mid,
                        "lag": (d_fair - d_mid) if d_fair is not None and d_mid is not None else None},
        }

    def autopsy_view(self, now):
        """Which conditions do settled orders win or lose under? P&L per contract by bucket.
        Recomputed once a minute. Buckets are few on purpose: with a handful of markets,
        every extra split is another chance to find a pattern that is only noise."""
        if now - self.autopsy_at < 60:
            return self.autopsy
        self.autopsy_at = now
        rows = self.db.execute("SELECT ticker, side, fill_px, contracts, pnl, d_fair, lag, secs_left, spread, latency_ms "
                               "FROM orders WHERE status='settled' AND contracts > 0 AND strategy='news'").fetchall()
        dims = {
            "entry price": lambda r: "<0.35" if r[2] < 0.35 else "0.35-0.65" if r[2] <= 0.65 else ">0.65",
            "time left": lambda r: None if r[7] is None else "first 5 min" if r[7] > 600 else "middle" if r[7] > 120 else "last 2 min",
            "model move": lambda r: None if r[5] is None else "2-3c" if abs(r[5]) < 0.03 else "3-5c" if abs(r[5]) < 0.05 else "5c+",
            "Kalshi spread": lambda r: None if r[8] is None else "1c" if r[8] <= 0.011 else "2c+",
        }
        out = {}
        for name, fn in dims.items():
            buckets = {}
            for r in rows:
                key = fn(r)
                if key is None:
                    continue
                b = buckets.setdefault(key, {"pnl": 0.0, "contracts": 0.0, "orders": 0, "markets": set(), "wins": 0})
                b["pnl"] += r[4]
                b["contracts"] += r[3]
                b["orders"] += 1
                b["wins"] += r[4] > 0
                b["markets"].add(r[0])
            out[name] = [{"bucket": k, "orders": v["orders"], "markets": len(v["markets"]),
                          "cents_per_contract": 100 * v["pnl"] / v["contracts"] if v["contracts"] else None,
                          "win_rate": v["wins"] / v["orders"]} for k, v in sorted(buckets.items())]
        out["_n"] = {"orders": len(rows), "markets": len({r[0] for r in rows})}
        self.autopsy = out
        return out

    def recorder_view(self, now):
        """What kalshi_recorder.py (same folder) wrote in the last minute. Re-read every 5 s."""
        if now - self.recorder_checked < 5:
            return self.recorder_stats
        self.recorder_checked = now
        path = HERE / "kalshi_live.sqlite"
        if not path.exists():
            self.recorder_stats = {"running": False}
            return self.recorder_stats
        try:
            db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1)
            since = int((now - 60) * 1e9)
            spot = dict(db.execute("SELECT src, COUNT(*) FROM spot WHERE rowid > (SELECT MAX(rowid) FROM spot) - 20000 "
                                   "AND recv_ns > ? GROUP BY src", (since,)).fetchall())
            book = dict(db.execute("SELECT COALESCE(src,'rest'), COUNT(*) FROM book WHERE rowid > (SELECT MAX(rowid) FROM book) - 50000 "
                                   "AND recv_ns > ? GROUP BY 1", (since,)).fetchall())
            trades = db.execute("SELECT COUNT(*) FROM trades WHERE recv_ns > ?", (since,)).fetchone()[0]
            markets = db.execute("SELECT COUNT(*), SUM(result IS NOT NULL) FROM markets").fetchone()
            db.close()
            size_mb = sum(p.stat().st_size for p in HERE.glob("kalshi_live.sqlite*")) / 1e6
            self.recorder_stats = {"running": bool(spot or book), "per_min": {**{f"spot:{k}": v for k, v in spot.items()},
                                   **{f"book:{k}": v for k, v in book.items()}, "kalshi trades": trades},
                                   "markets": markets[0], "settled": markets[1] or 0, "size_mb": size_mb}
        except sqlite3.Error as e:
            self.recorder_stats = {"running": None, "error": str(e)[:100]}
        return self.recorder_stats


def serve(bot):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/api/state"):
                body, ctype = json.dumps(bot.snapshot).encode(), "application/json"
            elif self.path in ("/", "/index.html"):
                body, ctype = (HERE / "dash" / "index.html").read_bytes(), "text/html; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            # the kill switch. Stopping is the only write the dashboard can make: there is no
            # endpoint that starts live trading (that is a separate arming script run by the operator, not published)
            if self.path == "/api/live/stop" and bot.broker.name == "LIVE":
                bot.broker.stop("stop button on the dashboard")
                body = b'{"stopped": true}'
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()


async def main():
    global STRATEGY, TRIGGER_DELTA
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", choices=["paper", "demo", "live"], default="paper",
                    help="paper = simulated fills; demo = real orders on Kalshi's demo exchange (fake money); "
                         "live = REAL MONEY on production, capped, only when armed by the arming script (not published)")
    ap.add_argument("--strategy", choices=["news", "level"], default=STRATEGY,
                    help="news = trade only fresh BTC moves Kalshi hasn't priced; level = the original disagreement rule")
    ap.add_argument("--delta", type=float, default=TRIGGER_DELTA, help="news trigger: model move in 1 s, in $ (0.02 = 2c)")
    args = ap.parse_args()
    STRATEGY, TRIGGER_DELTA = args.strategy, args.delta
    try:
        bot = Bot(args.broker)
    except Exception as e:  # KeyMissing for demo: say which env vars, then stop
        raise SystemExit(f"cannot start {args.broker} broker: {e}")
    threading.Thread(target=serve, args=(bot,), daemon=True).start()
    rule = (f"NEWS trigger: model moves >= {100 * TRIGGER_DELTA:.0f}c in 1 s and Kalshi lags" if STRATEGY == "news"
            else f"LEVEL rule: edge >= {100 * THRESHOLD:.0f}c")
    if bot.broker.name == "LIVE":
        bot.say("warn", f"LIVE MODE — real money. {rule}. Caps: ${LIVE_ORDER_USD:.0f}/order, ${LIVE_MARKET_USD:.0f}/market, "
                        f"${LIVE_DAILY_LOSS_USD:.0f}/day, ${LIVE_TOTAL_LOSS_USD:.0f} lifetime stop.")
    else:
        bot.say("info", f"{bot.broker.name} mode ({bot.broker.venue}). {rule}. No real orders in this mode.")
    bot.say("info", f"book feed: {'websocket (prod key found, read-only)' if bot.prod_key else 'REST polling (no prod key set)'}")
    await bot.bootstrap_sigma()
    bot.spot.start()
    if bot.prod_key:
        bot.kbook.start()
    tasks = [bot.btc_poll(), bot.sampler(), bot.markets(), bot.orderbook(), bot.publish(),
             bot.latency_probe(), bot.settle_orphans(), bot.loop_monitor(), bot.proc_monitor()]
    if bot.broker.name == "LIVE":
        tasks.append(bot.broker.reconcile())
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    print(f"dashboard: http://localhost:{PORT}")
    asyncio.run(main())
