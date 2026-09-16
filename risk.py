"""Risk rules for the Kalshi bot. Every rule says why it exists and what it blocked.

Written 2026-09-13 after the first six news-trigger markets on the first (burstable) server: four of six
bought BOTH sides of the same market (one side is a certain loss, and both pay the
fee), up to 8 orders / 400 contracts rode one coin flip, and single markets swung
-$39 to +$60. The rules, checked in this order:

  1. circuit breakers  daily loss limit; pause after a losing streak of markets;
                       pause when most recent orders MISS (we are being outrun)
  2. price band        effectively off since 2026-09-15: 0.01-0.99 (see PRICE_BAND)
  3. one direction     never hold YES and NO in the same market
  4. risk budget       dollars at risk per market capped; each order sized by edge

Paper-money settings. The numbers are knobs, not findings.
"""

import math
import time
from datetime import datetime, timezone

RISK_PER_MARKET_USD = 20.0   # most that can be lost in one 15-minute market
ORDER_USD = 10.0             # dollars at risk in a full-size order
EDGE_FULL_SIZE = 0.05        # an edge this big gets a full-size order; smaller edges scale down
MIN_SIZE_FRACTION = 0.25
# Tested, not assumed (kalshi_band.py, 30 days, 2,805 markets). The first band (0.15-0.85) was
# backwards: at 100-250 ms delay, buying the side priced 0.50-0.75 lost 5c/contract (+/-1.5,
# about -8% on money risked), while the cheap side (0.15-0.25) made +3-4c (+15-20% on risk,
# noisy) and below 0.15 was positive but very noisy. Upper bound 0.50 cuts the proven loser.
# Lower bound 0.05: Kalshi rounds each order's fee up to a whole cent, which the backtest cannot
# model and which swamps a 1-4c contract.
#
# Reopened 2026-09-15 by decision. Shadow trades blocked by the 0.05-0.50 band made
# +$304 over ~20 h live (810 orders, 76 markets), but +/-$359 clustered by market, so it is
# indistinguishable from zero: $224 of it came from 14 orders below 0.05, and 0.50-0.75 made +$8
# +/-$200. Paper money only; the band now lets everything but 0 and 1 through so the live
# record, not the backtest, decides. Revisit before any real money.
PRICE_BAND = (0.01, 0.99)
DAILY_LOSS_LIMIT_USD = 40.0  # UTC day, settled P&L
# 6, not 3: with entries at 0.05-0.50 the side bought wins only ~15-45% of the time (kalshi_band.py),
# so 3 losing markets in a row happens by chance roughly a third of the time and would pause a
# working strategy constantly. 6 in a row is rarer and still stops a genuinely broken day early.
STREAK_MARKETS = 6           # this many losing markets in a row ...
STREAK_PAUSE_S = 30 * 60     # ... pauses entries this long
MISS_WINDOW = 20             # of the last N orders ...
MISS_RATE = 0.60             # ... if more than this share missed ...
MISS_MIN_ORDERS = 10         # (and at least this many exist) ...
MISS_PAUSE_S = 15 * 60       # ... pause this long


class RiskManager:
    def __init__(self, db, enforce_breakers=True, order_usd=ORDER_USD, market_usd=RISK_PER_MARKET_USD,
                 daily_loss_usd=DAILY_LOSS_LIMIT_USD, scale_by_edge=True):
        """enforce_breakers=False (paper mode): the daily-loss, losing-streak and miss-rate breakers
        still compute and are reported, but record 'would have paused' instead of blocking.
        On 2026-09-13 three losing paper markets (-$42.71) tripped the $40 daily limit at 11:30 pm ET;
        the UTC day made the pause last until 8 pm the next day and it blocked 284 signals — a
        whole day of evidence lost to a rule that only protects real money. The band, one-direction
        and per-market budget stay enforced in every mode: they define the strategy being tested."""
        self.enforce_breakers = enforce_breakers
        # live mode passes $1 orders / $2 per market / $5 a day. Edge scaling is off there: 25% of $1
        # cannot buy one contract above 25c, so every small-edge signal would read "budget used".
        self.order_usd, self.market_usd, self.daily_loss_usd = order_usd, market_usd, daily_loss_usd
        self.scale_by_edge = scale_by_edge
        self.shadow_pauses = []   # (start_ts, reason) each time a breaker would have paused
        self.db = db
        self.paused_until = 0.0
        self.pause_reason = None
        # The miss-rate breaker only counts orders placed after its last pause ended. Without this,
        # a pause freezes the very window it is judged on (no orders while paused), so the moment
        # it expired the same 60%+ miss rate re-triggered it: a permanent halt.
        self.miss_since = 0.0
        self.blocks = {}          # rule -> count of signals it stopped
        self.last_refresh = 0.0
        self.view_cache = {}

    def _block(self, rule):
        self.blocks[rule] = self.blocks.get(rule, 0) + 1
        return False, 0, rule

    def refresh(self, now):
        """Recompute breakers from the orders table. Cheap; called about once a second."""
        if now - self.last_refresh < 1.0:
            return
        self.last_refresh = now
        day0 = datetime.fromtimestamp(now, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        day_pnl = self.db.execute("SELECT COALESCE(SUM(pnl),0) FROM orders WHERE status='settled' AND settled_ts >= ?",
                                  (day0,)).fetchone()[0]
        markets = self.db.execute("SELECT ticker, SUM(pnl), MAX(settled_ts) FROM orders WHERE status='settled' "
                                  "GROUP BY ticker ORDER BY MAX(settled_ts) DESC LIMIT ?", (STREAK_MARKETS,)).fetchall()
        # real orders only: the bot also records blocked signals as shadow rows, which must not dilute the miss rate
        recent = [r[0] for r in self.db.execute("SELECT status FROM orders WHERE COALESCE(signal_ts, 0) > ? "
                                                "AND status IN ('filled','settled','missed') "
                                                "ORDER BY id DESC LIMIT ?", (self.miss_since, MISS_WINDOW))]
        miss_rate = sum(s == "missed" for s in recent) / len(recent) if recent else 0.0
        streak = 0
        for _, pnl, _ in markets:
            if pnl < 0:
                streak += 1
            else:
                break

        if day_pnl <= -self.daily_loss_usd:
            tomorrow = day0 + 86400
            if self.paused_until < tomorrow:
                self.paused_until, self.pause_reason = tomorrow, f"daily loss limit (${day_pnl:.2f} today)"
        elif streak >= STREAK_MARKETS and markets and now - markets[0][2] < STREAK_PAUSE_S and self.paused_until < now:
            self.paused_until, self.pause_reason = markets[0][2] + STREAK_PAUSE_S, f"{streak} losing markets in a row"
        elif len(recent) >= MISS_MIN_ORDERS and miss_rate > MISS_RATE and self.paused_until < now:
            self.paused_until, self.pause_reason = now + MISS_PAUSE_S, f"{miss_rate:.0%} of the last {len(recent)} orders missed"
            self.miss_since = self.paused_until

        if self.pause_reason and self.paused_until > now and (not self.shadow_pauses or self.shadow_pauses[-1][1] != self.pause_reason):
            self.shadow_pauses.append((now, self.pause_reason))
        self.view_cache = {"day_pnl": day_pnl, "daily_limit": self.daily_loss_usd, "streak": streak,
                           "miss_rate": miss_rate, "miss_window": len(recent)}

    def check(self, now, ticker, side, price, edge):
        """-> (allowed, contracts, reason). contracts is sized by risk budget and edge."""
        self.refresh(now)
        if now < self.paused_until:
            if self.enforce_breakers:
                return self._block(f"paused: {self.pause_reason}")
            # counted separately, NOT as a block: mixing these into the blocks table made ~300
            # allowed signals look blocked on the dashboard (2026-09-14)
            self.would_pause_count = getattr(self, "would_pause_count", 0) + 1
        if not PRICE_BAND[0] <= price <= PRICE_BAND[1]:
            return self._block("price outside band")
        opposite = "no" if side == "yes" else "yes"
        held_opposite = self.db.execute("SELECT COALESCE(SUM(contracts),0) FROM orders WHERE ticker=? AND side=? "
                                        "AND status IN ('filled','settled')", (ticker, opposite)).fetchone()[0]
        if held_opposite > 0:
            return self._block("already holding the other side")
        at_risk = self.db.execute("SELECT COALESCE(SUM(contracts*fill_px + COALESCE(fee,0)),0) FROM orders "
                                  "WHERE ticker=? AND status IN ('filled','settled')", (ticker,)).fetchone()[0]
        room = self.market_usd - at_risk
        scale = max(MIN_SIZE_FRACTION, min(1.0, edge / EDGE_FULL_SIZE)) if self.scale_by_edge else 1.0
        budget = min(room, self.order_usd * scale)
        # buying at p risks p per contract; the epsilon stops 0.02/0.05 = 0.3999... from flooring 8 to 7
        contracts = math.floor(budget / price + 1e-9)
        if contracts < 1:
            return self._block("market risk budget used")
        return True, contracts, "ok"

    def exposure(self, ticker):
        rows = self.db.execute("SELECT side, SUM(contracts), SUM(contracts*fill_px), SUM(COALESCE(fee,0)) FROM orders "
                               "WHERE ticker=? AND status='filled' GROUP BY side", (ticker,)).fetchall()
        return [{"side": s, "contracts": c, "cost": cost, "fees": fee, "max_loss": cost + fee, "max_gain": c - cost - fee}
                for s, c, cost, fee in rows]

    def view(self, now, ticker=None):
        self.refresh(now)
        return {**self.view_cache, "paused": now < self.paused_until and self.enforce_breakers,
                "would_pause": now < self.paused_until and not self.enforce_breakers,
                "enforce_breakers": self.enforce_breakers, "shadow_pauses": self.shadow_pauses[-10:],
                "would_pause_count": getattr(self, "would_pause_count", 0),
                "pause_reason": self.pause_reason,
                "paused_for_s": max(0.0, self.paused_until - now), "blocks": dict(self.blocks),
                "exposure": self.exposure(ticker) if ticker else [],
                "rules": {"risk_per_market": self.market_usd, "order_usd": self.order_usd, "price_band": PRICE_BAND,
                          "daily_loss_limit": self.daily_loss_usd, "streak_markets": STREAK_MARKETS,
                          "miss_rate": MISS_RATE}}


def _verify():
    import sqlite3
    db = sqlite3.connect(":memory:")
    db.execute("""CREATE TABLE orders (id INTEGER PRIMARY KEY, ticker TEXT, side TEXT, contracts REAL, fill_px REAL,
                  fee REAL, status TEXT, pnl REAL, settled_ts REAL, signal_ts REAL)""")
    r = RiskManager(db)
    now = time.time()
    ok = True

    def expect(label, got, allowed, contracts=None):
        nonlocal ok
        good = got[0] == allowed and (contracts is None or got[1] == contracts)
        ok &= good
        print(f"  {'PASS' if good else 'FAIL'}  {label:<56} {got}")

    expect("clean market, 5c edge at 0.40 -> $10 -> 25 contracts", r.check(now, "M1", "yes", 0.40, 0.05), True, 25)
    expect("2c edge scales the order to 40% -> $4 -> 10 contracts", r.check(now, "M1", "yes", 0.40, 0.02), True, 10)
    expect("price 0.995 is outside the band (0.01-0.99)", r.check(now, "M1", "yes", 0.995, 0.05), False)
    expect("price 0.60 is inside the reopened band -> 16", r.check(now, "M9", "yes", 0.60, 0.05), True, 16)
    db.execute("INSERT INTO orders VALUES (1,'M1','yes',20,0.5,0.35,'filled',NULL,NULL,NULL)")
    expect("holding YES blocks buying NO in the same market", r.check(now, "M1", "no", 0.50, 0.05), False)
    expect("second YES order gets the remaining ~$9.65 -> 19", r.check(now, "M1", "yes", 0.50, 0.05), True, 19)
    db.execute("INSERT INTO orders VALUES (2,'M1','yes',20,0.5,0.35,'filled',NULL,NULL,NULL)")
    expect("budget used up: $20.70 at risk", r.check(now, "M1", "yes", 0.50, 0.05), False)
    for i, t in enumerate(("A", "B", "C", "D1", "E", "F")):
        db.execute("INSERT INTO orders VALUES (?,?, 'yes', 10, 0.5, 0.1, 'settled', -5.1, ?, ?)", (10 + i, t, now - 60 + i, now - 70))
    r.last_refresh = 0
    expect("6 losing markets in a row pauses new entries", r.check(now, "M2", "yes", 0.50, 0.05), False)
    r2 = RiskManager(db)
    db.execute("INSERT INTO orders VALUES (20,'D','yes',100,0.5,1,'settled',-30,?,?)", (now, now - 5))
    expect("daily loss beyond $40 halts for the day", r2.check(now, "M3", "yes", 0.50, 0.05), False)
    paper = RiskManager(db, enforce_breakers=False)
    expect("paper: same loss -> trade still allowed", paper.check(now, "M3", "yes", 0.40, 0.05), True)
    ok &= paper.view(now)["would_pause"] and paper.view(now)["would_pause_count"] >= 1 and not paper.blocks
    print(f"  {'PASS' if paper.view(now)['would_pause_count'] >= 1 and not paper.blocks else 'FAIL'}  paper: would-be pause counted separately, not as a block")
    expect("paper: price band still enforced", paper.check(now, "M3", "yes", 0.995, 0.05), False)

    # miss-rate breaker: pauses, then must NOT re-pause on the same frozen window when it expires
    db2 = sqlite3.connect(":memory:")
    db2.execute("""CREATE TABLE orders (id INTEGER PRIMARY KEY, ticker TEXT, side TEXT, contracts REAL, fill_px REAL,
                   fee REAL, status TEXT, pnl REAL, settled_ts REAL, signal_ts REAL)""")
    for i in range(12):
        db2.execute("INSERT INTO orders VALUES (?, 'X', 'yes', 0, NULL, NULL, 'missed', NULL, NULL, ?)", (i, now - 100 + i))
    r3 = RiskManager(db2)
    expect("12 of 12 recent orders missed -> pause", r3.check(now, "X", "yes", 0.50, 0.05), False)
    later = r3.paused_until + 1
    r3.last_refresh = 0
    expect("after the pause, the old misses no longer count", r3.check(later, "X", "yes", 0.50, 0.05), True)
    # live caps: $1 an order, $2 a market, no edge scaling, $5 a day, breakers enforced
    db3 = sqlite3.connect(":memory:")
    db3.execute("""CREATE TABLE orders (id INTEGER PRIMARY KEY, ticker TEXT, side TEXT, contracts REAL, fill_px REAL,
                   fee REAL, status TEXT, pnl REAL, settled_ts REAL, signal_ts REAL)""")
    live = RiskManager(db3, enforce_breakers=True, order_usd=1.0, market_usd=2.0, daily_loss_usd=5.0, scale_by_edge=False)
    expect("live: 1c edge at 0.40 still buys $1 -> 2 contracts", live.check(now, "L1", "yes", 0.40, 0.01), True, 2)
    expect("live: at 0.60, $1 buys 1 contract", live.check(now, "L1", "yes", 0.60, 0.05), True, 1)
    db3.execute("INSERT INTO orders VALUES (1,'L1','yes',2,0.40,0.02,'filled',NULL,NULL,NULL)")
    db3.execute("INSERT INTO orders VALUES (2,'L1','yes',1,0.60,0.02,'filled',NULL,NULL,NULL)")
    expect("live: $1.44 at risk leaves $0.56 -> 1 at 0.40", live.check(now, "L1", "yes", 0.40, 0.05), True, 1)
    expect("live: $0.56 cannot buy one at 0.60", live.check(now, "L1", "yes", 0.60, 0.05), False)
    db3.execute("INSERT INTO orders VALUES (3,'L0','no',10,0.55,0.1,'settled',-5.6,?,?)", (now, now - 5))
    live.last_refresh = 0
    expect("live: -$5.60 today halts (limit $5)", live.check(now, "L2", "yes", 0.40, 0.05), False)
    print("risk verify:", "ALL PASS" if ok else "FAILED")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if _verify() else 1)
