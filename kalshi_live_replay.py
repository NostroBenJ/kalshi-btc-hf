"""Replay the news trigger at OUR latency, on one clock. The go/no-go for the fast bot.

    python kalshi_live_replay.py

Input: kalshi_live.sqlite from kalshi_recorder.py. Every event carries this
machine's receive time, so "Coinbase moved, then Kalshi's book still showed
the old price" is measured on a single clock — no Binance/Kalshi clock skew.

For every Coinbase trade we RECEIVED (throttled to one decision per 20 ms):
  1. fair(t) from fair_value.fair_up: Kalshi's own floor_strike as K, Coinbase
     shifted by the basis measured in the minute before the open, sigma from
     the hour before (>= 20 min of recording required).
  2. trigger: fair moved >= delta over the last 1,000 ms, the Kalshi book mid
     we had seen moved by less than half of that, and the ask we had seen is
     below fair by more than the fee.
  3. execution: the order reaches Kalshi at t + 1 ms + half our measured book
     round trip + EXTRA. It fills only against the first recorded book whose
     midpoint time (send+recv)/2 is at or after that arrival, and only if that
     ask is still <= the limit. Settled on Kalshi's result.
  4. one entry per side per market per 2 s, so one move is not counted 40 times.

Scenarios: EXTRA = 0 (this PC as it is), +100, +250, +1000 ms; and a LEAK that
reads Coinbase 1,000 ms into the future (must be the best row, or it is blind).

Limits, stated: the book is sampled every 100 ms, so quotes that live and die
between polls are invisible — in both directions. A websocket book (needs the
user's read-only production key) removes that. Errors clustered by market.
"""

import bisect
import math
import sqlite3
import statistics
from pathlib import Path

from backtest import clustered
from fair_value import fair_up, realized_vol, settle_times, taker_fee

DB = Path(__file__).with_name("kalshi_live.sqlite")
DELTAS = (0.02, 0.05)
SCENARIOS = (("LEAK: Coinbase 1000 ms early", -1000, True), ("this PC, as measured", 0, False),
             ("+100 ms", 100, False), ("+250 ms", 250, False), ("+1000 ms", 1000, False))
COOLDOWN_NS = 2_000_000_000
MAX_CONTRACTS = 50


def load(db):
    # BTC only: the recorder also stores KXETH15M markets, which BTC spot prices cannot replay
    markets = db.execute("SELECT ticker, open, close, strike, result FROM markets "
                         "WHERE result IN ('yes','no') AND strike IS NOT NULL AND ticker LIKE 'KXBTC15M-%' "
                         "ORDER BY open").fetchall()
    spot = db.execute("SELECT recv_ns, exch_us, price FROM spot WHERE src='coinbase' ORDER BY recv_ns").fetchall()
    # order latency comes from REQUEST round trips only; websocket rows have no request leg
    rtt = [r[0] for r in db.execute("SELECT (recv_ns - send_ns) / 1e6 FROM book WHERE COALESCE(src,'rest')='rest' "
                                    "ORDER BY rowid DESC LIMIT 5000")]
    return markets, spot, statistics.median(rtt) if rtt else None


def replay_market(db, market, spot, rtt_ms, extra_ms, leak):
    ticker, open_, close, K, result = market
    yes_won = result == "yes"
    s_recv = [r[0] for r in spot]
    # local clock -> UTC: median (receive - Coinbase stamp) over the hour before the open.
    # It folds network delay (~25 ms) into the offset; that only nudges which second
    # is "the open", never the latency path, which stays on the local clock.
    lo = bisect.bisect_left(s_recv, (open_ - 3600) * 10**9 + 0)
    pre = [spot[i][0] / 1e3 - spot[i][1] for i in range(max(0, lo - 2000), min(len(spot), lo + 2000))]
    if len(pre) < 200:
        return None
    offset_us = statistics.median(pre)  # local_us - utc_us
    shift_ns = -1_000_000_000 if leak else 0

    def price_at_local(t_ns):
        i = bisect.bisect_right(s_recv, t_ns - shift_ns) - 1
        return spot[i][2] if i >= 0 else None

    def price_at_utc_second(s):
        return price_at_local(int(s * 1e9 + offset_us * 1e3))

    pre_open = [price_at_utc_second(s) for s in range(open_ - 59, open_ + 1)]
    if any(p is None for p in pre_open):
        return None
    basis = sum(pre_open) / 60 - K
    grid = [price_at_utc_second(s) for s in range(open_ - 3600, open_ + 1, 10)]
    grid = [p for p in grid if p]
    if len(grid) < 120:  # < 20 minutes of history
        return None
    sigma = realized_vol(grid, dt_seconds=10)
    times = settle_times(open_, close - open_, 60)

    cols = "send_ns, recv_ns, yes_bid, yes_ask, yes_ask_sz, no_bid, no_ask, no_ask_sz"
    book = db.execute(f"SELECT {cols} FROM book WHERE ticker=? AND src='ws' ORDER BY recv_ns", (ticker,)).fetchall()
    if len(book) < 1000:  # no websocket book for this market: fall back to the 100 ms polls
        book = db.execute(f"SELECT {cols} FROM book WHERE ticker=? AND COALESCE(src,'rest')='rest' ORDER BY recv_ns",
                          (ticker,)).fetchall()
    if len(book) < 1000:
        return None
    b_recv = [b[1] for b in book]
    b_mid_time = sorted(((b[0] + b[1]) // 2, i) for i, b in enumerate(book))
    b_mid_keys = [x[0] for x in b_mid_time]

    cache = {}

    def fair_local(t_ns):
        slot = t_ns // 50_000_000
        if slot in cache:
            return cache[slot]
        now = (slot * 50_000_000 - offset_us * 1e3) / 1e9
        val = None
        S = price_at_local(slot * 50_000_000)
        if S is not None and open_ + 1 <= now < close - 1:
            known = {s: price_at_utc_second(s) - basis for s in times if s <= now}
            if all(v is not None for v in known.values()):
                val = fair_up(now, times, {s: v for s, v in known.items()}, S - basis, K, sigma)[0]
        cache[slot] = val
        return val

    def book_seen(t_ns):
        i = bisect.bisect_right(b_recv, t_ns) - 1
        return book[i] if i >= 0 else None

    start_ns = int(open_ * 1e9 + offset_us * 1e3)
    end_ns = int(close * 1e9 + offset_us * 1e3)
    i0 = bisect.bisect_left(s_recv, start_ns)
    i1 = bisect.bisect_left(s_recv, end_ns)
    cells = {}
    last_entry = {"yes": -10**20, "no": -10**20}
    last_eval = -10**20
    for i in range(i0, i1):
        t = s_recv[i]
        if t - last_eval < 20_000_000:
            continue
        last_eval = t
        f0, f1 = fair_local(t), fair_local(t - 1_000_000_000)
        seen, seen1 = book_seen(t), book_seen(t - 1_000_000_000)
        if None in (f0, f1, seen, seen1) or None in (seen[2], seen[3], seen1[2], seen1[3]):
            continue
        mid, mid1 = (seen[2] + seen[3]) / 2, (seen1[2] + seen1[3]) / 2
        d_fair = f0 - f1
        lag = d_fair - (mid - mid1)
        last60 = (t - start_ns) / 1e9 >= (close - open_) - 60
        for side, sign, ask, sz_i, ask_i in (("yes", 1, seen[3], 4, 3), ("no", -1, seen[6], 7, 6)):
            if ask is None or t - last_entry[side] < COOLDOWN_NS:
                continue
            fair_side = f0 if side == "yes" else 1 - f0
            if fair_side - ask - taker_fee(ask) < 0:
                continue
            for delta in DELTAS:
                if sign * d_fair < delta or sign * lag < delta / 2:
                    continue
                last_entry[side] = t
                arrive = t + 1_000_000 + int(rtt_ms / 2 * 1e6) + extra_ms * 1_000_000
                j = bisect.bisect_left(b_mid_keys, arrive)
                spans = ("all", "last60") if last60 else ("all",)
                for span in spans:
                    key = (delta, span)
                    c = cells.setdefault(key, {"signals": 0, "fills": 0, "pnl": 0.0, "n": 0.0})
                    c["signals"] += 1
                    if j >= len(b_mid_keys):
                        continue
                    later = book[b_mid_time[j][1]]
                    fill_ask, fill_sz = later[ask_i], later[sz_i]
                    if fill_ask is None or fill_ask > ask + 1e-9 or not fill_sz:
                        continue
                    n = min(MAX_CONTRACTS, fill_sz)
                    won = 1.0 if (side == "yes") == yes_won else 0.0
                    c["fills"] += 1
                    c["pnl"] += n * (won - fill_ask - taker_fee(fill_ask))
                    c["n"] += n
    return cells


def main():
    db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    markets, spot, rtt_ms = load(db)
    print(f"settled markets recorded: {len(markets)}   Coinbase trades: {len(spot):,}   "
          f"median Kalshi book round trip: {rtt_ms:.1f} ms")
    if not markets:
        print("nothing settled yet — let kalshi_recorder.py run")
        return
    for name, extra, leak in SCENARIOS:
        results = [r for m in markets if (r := replay_market(db, m, spot, rtt_ms, extra, leak)) is not None]
        print(f"\n=== {name}   ({len(results)} usable markets)")
        for span in ("all", "last60"):
            for delta in DELTAS:
                groups = [(r[(delta, span)]["pnl"], r[(delta, span)]["n"]) if (delta, span) in r else (0.0, 0.0)
                          for r in results]
                sig = sum(r[(delta, span)]["signals"] for r in results if (delta, span) in r)
                fil = sum(r[(delta, span)]["fills"] for r in results if (delta, span) in r)
                mean, se, _ = clustered(groups)
                pnl = f"{100 * mean:+.2f}c +/- {100 * se:.2f}" if fil and not math.isnan(mean) else "n/a"
                where = "whole window" if span == "all" else "final minute"
                print(f"  {where:<13} moved {100 * delta:.0f}c/1s: signals {sig:>5}  filled {fil:>5} "
                      f"({100 * fil / max(sig, 1):3.0f}%)   P&L/contract {pnl}")
    print("\nNeeds days, not hours: per market the signal count is small and outcomes cluster.")


if __name__ == "__main__":
    main()
