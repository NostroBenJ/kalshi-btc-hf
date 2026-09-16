"""Offline checks for paper_bot.LiveBroker: no network, no real key, no real orders.

    python test_live_broker.py

A fake Kalshi REST client answers the order and account calls; a stub bot supplies the database.
Every cap and stop is driven to the point where it must fire, because a guard that is only ever
tested in the state where it does nothing proves nothing.
"""

import asyncio
import sqlite3
import tempfile
import time
from pathlib import Path

import paper_bot as pb

ok = True


def expect(label, got, want):
    global ok
    good = got == want
    ok &= good
    print(f"  {'PASS' if good else 'FAIL'}  {label:<64} {got!r}")


class FakeRest:
    def __init__(self):
        self.orders, self.reply, self.fail = [], {}, None
        self.balance_cents, self.positions = 1000, []

    def request(self, method, path, body=None, signed=False):
        if method == "POST":
            self.orders.append(body)
            if self.fail:
                raise RuntimeError(self.fail)
            return dict(self.reply), 12.0
        if path.startswith("/portfolio/balance"):
            return {"balance": self.balance_cents}, 10.0
        if path.startswith("/portfolio/positions"):
            return {"market_positions": self.positions}, 10.0
        if path.startswith("/markets/"):
            return {"market": {"result": "yes"}}, 10.0
        raise AssertionError(path)


class StubBot:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("""CREATE TABLE orders (id INTEGER PRIMARY KEY, ticker TEXT, side TEXT, signal_ts REAL,
            contracts REAL, fill_px REAL, fee REAL, status TEXT, pnl REAL)""")
        self.market = {"ticker": "NOW"}
        self.logs = []

    def say(self, level, msg):
        self.logs.append((level, msg))


def order(side="yes", limit=0.40, fair=0.46, n=2):
    return {"ticker": "NOW", "side": side, "signal_ts": time.time(), "limit": limit, "fair": fair, "contracts": n}


def main():
    tmp = Path(tempfile.mkdtemp())
    pb.STOP_FILE, pb.ARM_FILE = tmp / "LIVE_STOP", tmp / "LIVE_ARMED"
    run = asyncio.run

    bot = StubBot()
    try:
        pb.LiveBroker(bot)
        expect("refuses to start when live mode is not armed", "started", "refused")
    except RuntimeError:
        expect("refuses to start when live mode is not armed", "refused", "refused")

    rest = FakeRest()
    br = pb.LiveBroker(bot, rest=rest)

    rest.reply = {"order_id": "o1", "fill_count": "2.00", "average_fill_price": "0.4000", "average_fee_paid": "0.010000"}
    o = run(br.take(order()))
    expect("YES fill: status, contracts, price, fee", (o["status"], o["contracts"], o["fill_px"], o["fee"]),
           ("filled", 2, 0.40, 0.02))
    expect("YES order sent as bid @ 0.4000 x 2.00", (rest.orders[-1]["side"], rest.orders[-1]["price"], rest.orders[-1]["count"]),
           ("bid", "0.4000", "2.00"))

    rest.reply = {"order_id": "o2", "fill_count": "1.00", "average_fill_price": "0.6300", "average_fee_paid": "0.020000"}
    o = run(br.take(order(side="no", limit=0.37, fair=0.45, n=1)))
    expect("NO fill: YES-book 0.63 recorded as NO price 0.37", (o["status"], o["fill_px"]), ("filled", 0.37))

    rest.reply = {"order_id": "o3", "fill_count": "0.00", "remaining_count": "2.00"}
    o = run(br.take(order()))
    expect("IOC with no fill -> missed", (o["status"], o["note"]), ("missed", "IOC not filled"))

    before = len(rest.orders)
    o = run(br.take(order(limit=0.40, fair=0.41, n=1)))
    expect("1c edge vs 2c rounded fee on 1 contract -> not sent", (o["status"], len(rest.orders) - before), ("missed", 0))

    rest.fail = "timeout"
    for _ in range(3):
        o = run(br.take(order()))
    expect("three order errors in a row write LIVE_STOP", (o["status"], pb.STOP_FILE.exists()), ("error", True))
    before = len(rest.orders)
    o = run(br.take(order()))
    expect("after LIVE_STOP nothing is sent", (o["status"], len(rest.orders) - before), ("missed", 0))
    expect("gate names the stop reason", br.gate(time.time()).startswith("stopped: 3 order errors"), True)
    pb.STOP_FILE.unlink()
    rest.fail = None

    bot2 = StubBot()
    br2 = pb.LiveBroker(bot2, rest=FakeRest())
    now = time.time()
    bot2.db.execute("INSERT INTO orders (ticker, side, signal_ts, contracts, fill_px, fee, status, pnl) VALUES "
                    "('A','yes',?,10,0.5,0.1,'settled',-7.60)", (now,))
    expect("lifetime -$7.60: still allowed", br2.gate(now), None)
    bot2.db.execute("INSERT INTO orders (ticker, side, signal_ts, contracts, fill_px, fee, status) VALUES "
                    "('NOW','yes',?,5,0.45,0.04,'filled')", (now,))
    expect("lifetime -$7.60 settled - $2.29 open = -$9.89: allowed", br2.gate(now), None)
    bot2.db.execute("INSERT INTO orders (ticker, side, signal_ts, contracts, fill_px, fee, status) VALUES "
                    "('NOW','yes',?,1,0.10,0.01,'filled')", (now,))
    expect("one more 11c open -> -$10.00 -> lifetime stop", br2.gate(now), "stopped: lifetime loss limit")
    expect("the lifetime stop is written to LIVE_STOP", pb.STOP_FILE.exists(), True)
    pb.STOP_FILE.unlink()

    bot3 = StubBot()
    br3 = pb.LiveBroker(bot3, rest=FakeRest())
    for i in range(pb.LIVE_MAX_ORDERS_PER_DAY):
        bot3.db.execute("INSERT INTO orders (ticker, side, signal_ts, contracts, status) VALUES ('X','yes',?,0,'missed')", (now,))
    expect(f"{pb.LIVE_MAX_ORDERS_PER_DAY} orders today -> blocked", br3.gate(now), f"{pb.LIVE_MAX_ORDERS_PER_DAY} live orders today")

    bot4 = StubBot()
    br4 = pb.LiveBroker(bot4, rest=FakeRest())
    bot4.db.execute("INSERT INTO orders (ticker, side, contracts, status) VALUES ('NOW','yes',2,'filled'), ('NOW','no',1,'filled'), "
                    "('OLD','yes',3,'filled')")
    expect("positions match: current +1 net, closed market already settled to 0",
           br4.mismatches({"NOW": 1.0}, "NOW"), [])
    expect("current market differs -> flagged", br4.mismatches({"NOW": 3.0}, "NOW"), ["NOW: Kalshi +3, bot +1"])
    expect("position the bot never placed -> flagged", br4.mismatches({"NOW": 1.0, "HAND": -4.0}, "NOW"),
           ["HAND: Kalshi -4, bot +0"])

    rest5 = FakeRest()
    rest5.positions = [{"ticker": "NOW", "position_fp": "5.00"}]
    br4.rest = rest5
    bot4.market = {"ticker": "NOW"}

    async def two_checks():
        pb_sleep = asyncio.sleep
        calls = {"n": 0}

        async def fast_sleep(s):
            calls["n"] += 1
            if calls["n"] >= 3:
                raise asyncio.CancelledError
            await pb_sleep(0)
        asyncio.sleep = fast_sleep
        try:
            await br4.reconcile()
        except asyncio.CancelledError:
            pass
        finally:
            asyncio.sleep = pb_sleep
    run(two_checks())
    expect("reconcile: mismatch on two checks in a row -> LIVE_STOP", pb.STOP_FILE.exists(), True)
    expect("reconcile reads the balance ($10.00)", br4.balance, 10.0)

    print("live broker verify:", "ALL PASS" if ok else "FAILED")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
