"""How fast does the bot need to be? Measured on the server's own recordings.

    python live_latency_curve.py            # on the server, next to kalshi_live.sqlite

Everything here is on ONE clock (the server's), so there is no exchange-clock skew:
  * BTC path: Coinbase trades as received by the server
  * Kalshi: top-of-book changes as received over the websocket

For every moment the bot's news trigger would have fired (fair value moved >= 2c in 1 s,
Kalshi's mid lagged by >= 1c, ask below fair after the fee), this measures:

  1. QUOTE LIFETIME: how long the underpriced ask we saw stayed on Kalshi's book before it was
     taken or pulled. If quotes die in ~5 ms, only co-located bots can trade this; if they live
     50+ ms, our ~15-30 ms is fast enough and speed is not the problem.
  2. THE LATENCY CURVE: an order arriving L ms after the signal fills only if the book at that
     moment still offers the price. P&L per contract after fees, held to settlement, for
     L = 0, 5, 10, 15, 20, 30, 50, 100, 250 ms. Errors clustered by market.
  3. CONTROL: the same test with the BTC feed read 1 s into the future. It must be clearly
     positive, or the test cannot see an edge even when one exists.

Uses the 60 s-average settlement rule, Kalshi's floor_strike, basis from the minute before the
open, and trailing-hour 10 s realized volatility — the same inputs the bot uses.
"""

import bisect
import math
import sqlite3
import statistics
import sys
from pathlib import Path

from backtest import clustered
from fair_value import fair_up, realized_vol, settle_times, taker_fee

DB = Path(__file__).with_name("kalshi_live.sqlite")
DELTA = 0.02
COOLDOWN_NS = 2_000_000_000
LATENCIES_MS = (0, 5, 10, 15, 20, 30, 50, 100, 250)
BAND = (0.05, 0.50)


def load(db, feed="coinbase"):
    markets = db.execute("SELECT ticker, open, close, strike, result FROM markets WHERE result IN ('yes','no') "
                         "AND strike IS NOT NULL AND ticker LIKE 'KXBTC15M-%' ORDER BY open").fetchall()
    spot = db.execute("SELECT recv_ns, exch_us, price FROM spot WHERE src=? ORDER BY recv_ns", (feed,)).fetchall()
    rtt = [r[0] for r in db.execute("SELECT (recv_ns - send_ns) / 1e6 FROM book WHERE COALESCE(src,'rest')='rest' "
                                    "ORDER BY rowid DESC LIMIT 5000")]
    return markets, spot, (statistics.median(rtt) if rtt else None)


def signals_for_market(db, market, spot, s_recv, shift_ns):
    ticker, open_, close, K, result = market
    yes_won = result == "yes"
    lo = bisect.bisect_left(s_recv, (open_ - 3600) * 10**9)
    near = [spot[i][0] / 1e3 - spot[i][1] for i in range(max(0, lo - 2000), min(len(spot), lo + 2000))]
    if len(near) < 200:
        return None
    offset_us = statistics.median(near)  # local - exchange; ~network delay on a synced server clock

    def px_local(t_ns):
        i = bisect.bisect_right(s_recv, t_ns - shift_ns) - 1
        return spot[i][2] if i >= 0 else None

    def px_utc_sec(sec):
        return px_local(int(sec * 1e9 + offset_us * 1e3))

    pre = [px_utc_sec(s) for s in range(open_ - 59, open_ + 1)]
    grid = [p for p in (px_utc_sec(s) for s in range(open_ - 3600, open_ + 1, 10)) if p]
    if any(p is None for p in pre) or len(grid) < 120:
        return None
    basis, sigma = sum(pre) / 60 - K, realized_vol(grid, dt_seconds=10)
    times = settle_times(open_, close - open_, 60)

    book = db.execute("SELECT recv_ns, yes_bid, yes_ask, yes_ask_sz, no_bid, no_ask, no_ask_sz FROM book "
                      "WHERE ticker=? AND src='ws' ORDER BY recv_ns", (ticker,)).fetchall()
    if len(book) < 500:
        return None
    b_recv = [b[0] for b in book]

    cache = {}

    def fair(t_ns):
        slot = t_ns // 50_000_000
        if slot not in cache:
            now = (slot * 50_000_000 - offset_us * 1e3) / 1e9
            S = px_local(slot * 50_000_000)
            val = None
            if S is not None and open_ + 1 <= now < close - 1:
                known = {s: px_utc_sec(s) for s in times if s <= now}
                if all(v is not None for v in known.values()):
                    val = fair_up(now, times, {s: v - basis for s, v in known.items()}, S - basis, K, sigma)[0]
            cache[slot] = val
        return cache[slot]

    def book_at(t_ns):
        i = bisect.bisect_right(b_recv, t_ns) - 1
        return (i, book[i]) if i >= 0 else (None, None)

    start_ns, end_ns = int(open_ * 1e9 + offset_us * 1e3), int(close * 1e9 + offset_us * 1e3)
    out, last_entry, last_eval = [], {"yes": -10**20, "no": -10**20}, -10**20
    for k in range(bisect.bisect_left(s_recv, start_ns), bisect.bisect_left(s_recv, end_ns)):
        t = s_recv[k]
        if t - last_eval < 20_000_000:
            continue
        last_eval = t
        f0, f1 = fair(t), fair(t - 1_000_000_000)
        i0, b0 = book_at(t)
        _, b1 = book_at(t - 1_000_000_000)
        if None in (f0, f1, b0, b1) or None in (b0[1], b0[2], b1[1], b1[2]):
            continue
        d_fair = f0 - f1
        lag = d_fair - ((b0[1] + b0[2]) / 2 - (b1[1] + b1[2]) / 2)
        for side, sign, ask, sz, ask_col, sz_col in (("yes", 1, b0[2], b0[3], 2, 3), ("no", -1, b0[5], b0[6], 5, 6)):
            if ask is None or not sz or t - last_entry[side] < COOLDOWN_NS:
                continue
            if sign * d_fair < DELTA or sign * lag < DELTA / 2:
                continue
            if (f0 if side == "yes" else 1 - f0) - ask - taker_fee(ask) < 0:
                continue
            last_entry[side] = t
            # quote lifetime: first later book row where that price is no longer offered
            life_ms = None
            for j in range(i0 + 1, len(book)):
                a2, s2 = book[j][ask_col], book[j][sz_col]
                if a2 is None or a2 > ask + 1e-9 or not s2:
                    life_ms = (book[j][0] - t) / 1e6
                    break
            won = 1.0 if (side == "yes") == yes_won else 0.0
            fills = {}
            for L in LATENCIES_MS:
                _, bl = book_at(t + L * 1_000_000)
                a2, s2 = bl[ask_col], bl[sz_col]
                if a2 is not None and s2 and a2 <= ask + 1e-9:
                    fills[L] = won - a2 - taker_fee(a2)
            out.append({"ticker": ticker, "side": side, "ask": ask, "life_ms": life_ms, "fills": fills,
                        "in_band": BAND[0] <= ask <= BAND[1]})
    return out


def report(label, sigs, rtt_ms):
    markets = sorted({s["ticker"] for s in sigs})
    print(f"\n=== {label}: {len(sigs)} signals in {len(markets)} settled markets")
    if not sigs:
        return
    lives = sorted(s["life_ms"] for s in sigs if s["life_ms"] is not None)
    if lives:
        q = lambda p: lives[int(p * (len(lives) - 1))]
        print(f"  quote lifetime (ms): p10 {q(.1):.0f} | p25 {q(.25):.0f} | median {q(.5):.0f} | p75 {q(.75):.0f} | p90 {q(.9):.0f}"
              f"   ({sum(1 for s in sigs if s['life_ms'] is None)} never taken before close)")
        for cut in (5, 10, 20, 50, 100):
            print(f"    gone within {cut:>3} ms: {100 * sum(1 for x in lives if x <= cut) / len(sigs):4.0f}% of signals")
    print(f"  {'arrive after':>13} {'filled':>8} {'P&L / contract (all)':>26} {'P&L / contract (in band)':>28}")
    for L in LATENCIES_MS:
        for band_only in (False, True):
            pool = [s for s in sigs if s["in_band"] or not band_only]
            groups = {}
            for s in pool:
                if L in s["fills"]:
                    g = groups.setdefault(s["ticker"], [0.0, 0])
                    g[0] += s["fills"][L]
                    g[1] += 1
            mean, se, _ = clustered(list(groups.values()) + [(0.0, 0)] * 0)
            n_f = sum(g[1] for g in groups.values())
            cell = f"{100 * mean:+6.2f}c +/- {100 * se:4.2f}" if n_f and not math.isnan(mean) else "n/a"
            if not band_only:
                row = f"  {L:>10} ms {100 * n_f / max(len(pool), 1):7.0f}% {cell:>26}"
            else:
                row += f" {cell:>28}"
        mark = "   <- ~bot's order round trip" if rtt_ms and L <= rtt_ms < next((x for x in LATENCIES_MS if x > L), 10**9) else ""
        print(row + mark)


def lead_lag(db, feed, ref="coinbase", step_ms=50, max_lag_ms=1500):
    """Does `feed` move before `ref`? Correlate 50 ms price changes of ref with feed shifted by each lag
    (both on the server's receive clock). The lag with the highest correlation is how far ahead the feed is."""
    def series(src):
        rows = db.execute("SELECT recv_ns, price FROM spot WHERE src=? ORDER BY recv_ns", (src,)).fetchall()
        return [r[0] for r in rows], [r[1] for r in rows]
    ft, fp = series(feed)
    rt, rp = series(ref)
    if len(ft) < 1000 or len(rt) < 1000:
        return None
    t0, t1 = max(ft[0], rt[0]), min(ft[-1], rt[-1])
    step = step_ms * 1_000_000

    def grid(ts, ps):
        out, j, last = [], 0, None
        for g in range(t0, t1, step):
            while j < len(ts) and ts[j] <= g:
                last = ps[j]
                j += 1
            out.append(last)
        return out

    fg, rg = grid(ft, fp), grid(rt, rp)
    fr = [math.log(b / a) if a and b else 0.0 for a, b in zip(fg, fg[1:])]
    rr = [math.log(b / a) if a and b else 0.0 for a, b in zip(rg, rg[1:])]
    best = None
    lines = []
    for lag in range(-max_lag_ms // step_ms, max_lag_ms // step_ms + 1):
        # lag > 0: the feed's move at t lines up with ref's move at t + lag (feed leads)
        pairs = [(fr[i], rr[i + lag]) for i in range(max(0, -lag), min(len(fr), len(rr) - lag))]
        if len(pairs) < 1000:
            continue
        xs, ys = zip(*pairs)
        mx, my = statistics.fmean(xs), statistics.fmean(ys)
        cov = sum((x - mx) * (y - my) for x, y in pairs)
        den = math.sqrt(sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys))
        corr = cov / den if den else 0.0
        lines.append((lag * step_ms, corr))
        if best is None or corr > best[1]:
            best = (lag * step_ms, corr)
    return best, lines


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--feeds", default="coinbase", help="comma list of spot srcs, e.g. coinbase,binance_perp,okx_perp")
    ap.add_argument("--control", action="store_true", help="also run the 1 s future-leak control for each feed")
    args = ap.parse_args()
    db = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    for feed in args.feeds.split(","):
        markets, spot, rtt = load(db, feed)
        s_recv = [r[0] for r in spot]
        print(f"\n################ FEED: {feed} | settled BTC markets recorded: {len(markets)} | {feed} rows: {len(spot):,} | "
              f"order round trip: {rtt:.1f} ms" if rtt else f"\n################ FEED: {feed}")
        if feed != "coinbase":
            ll = lead_lag(db, feed)
            if ll:
                (lag, corr), lines = ll
                at0 = dict(lines).get(0)
                print(f"  lead vs Coinbase: strongest at {lag:+d} ms (corr {corr:.3f}; at 0 ms {at0:.3f}) "
                      f"{'-> feed LEADS' if lag > 0 else '-> no lead'}")
        runs = [("LIVE (bot reads this feed)", 0)] + ([("CONTROL: this feed read 1 s in the future", -1_000_000_000)] if args.control else [])
        for label, shift in runs:
            sigs = []
            for i, mkt in enumerate(markets, 1):
                r = signals_for_market(db, mkt, spot, s_recv, shift)
                if r:
                    sigs.extend(r)
                print(f"  [{feed} {label[:4]}] {i}/{len(markets)} markets", file=sys.stderr, flush=True)
            report(f"{feed}: {label}", sigs, rtt)


if __name__ == "__main__":
    main()
