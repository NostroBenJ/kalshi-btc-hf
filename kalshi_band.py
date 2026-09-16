"""Does the risk band (no entries below 0.15 or above 0.85) cost the news trigger money?

    python kalshi_band.py

The band was added on judgement after early paper losses, never tested. Same trigger and
50 ms grid as kalshi_ms.py (2c model move in 1 s, Kalshi lagging >= 1c, edge after fee >= 0),
held to settlement, bucketed by the price actually paid for the side bought.

Reports per bucket, at D = 0 / 100 / 250 ms:
  c/contract   P&L per contract after fees (market-clustered SE)
  % on risk    P&L per dollar paid: what the $20-per-market budget actually earns,
               since a 0.10 contract ties up 10c and a 0.90 one ties up 90c
  win rate     how often the side bought won
"""

import gzip
import json
import math
from multiprocessing import Pool
from pathlib import Path

from backtest import Prices, clustered, market_sigma, strike
from fair_value import fair_up, settle_times, taker_fee
from kalshi_ms import Grid

DATA = Path(__file__).with_name("data") / "kalshi"
DELTA = 0.02
DELAYS_MS = (0, 100, 250)
EDGES = (0.0, 0.05, 0.10, 0.15, 0.25, 0.50, 0.75, 0.85, 0.90, 0.95, 1.0)
SLOT = 50


def bucket(p):
    for lo, hi in zip(EDGES, EDGES[1:]):
        if lo <= p < hi:
            return f"{lo:.2f}-{hi:.2f}"
    return None


def process(path_str):
    try:
        with gzip.open(path_str, "rt") as f:
            m = json.load(f)
    except (OSError, EOFError, ValueError):
        return None
    if m["result"] not in ("yes", "no") or m["close"] - m["open"] != 900:
        return None
    sec, grid = Prices(), Grid()
    start, end = m["open"], m["close"]
    K, sigma = strike(sec, start), market_sigma(sec, start)
    if K is None or sigma is None:
        return None
    yes_won = m["result"] == "yes"
    times = settle_times(start, 900, 60)
    cache = {}

    def fair_at(ms):
        slot = ms - ms % SLOT
        if slot not in cache:
            now = slot / 1000
            S = grid(slot)
            known = {s: sec(s) for s in times if s <= now}
            cache[slot] = (fair_up(now, times, known, S, K, sigma)[0]
                           if S is not None and start + 1 <= now < end and all(v is not None for v in known.values()) else None)
        return cache[slot]

    import bisect
    trades = sorted(m["trades"])
    t_ms = [int(t * 1000) for t, *_ in trades]
    y_px = [yp for _, _, yp, _ in trades]
    cells = {}
    prev_yes = None
    for t, side, yes_price, count in trades:
        if start + 1 <= t < end - 1:
            ms = int(t * 1000)
            p = yes_price if side == "yes" else 1.0 - yes_price
            b = bucket(p)
            if b is not None:
                won = 1.0 if (side == "yes") == yes_won else 0.0
                pnl = won - p - taker_fee(p)
                for D in DELAYS_MS:
                    f0, f1 = fair_at(ms - D), fair_at(ms - D - 1000)
                    i1 = bisect.bisect_left(t_ms, ms - D - 1000) - 1
                    if None in (f0, f1, prev_yes) or i1 < 0:
                        continue
                    d_fair = f0 - f1
                    lag = d_fair - (prev_yes - y_px[i1])
                    sign = 1 if side == "yes" else -1
                    if sign * d_fair < DELTA or sign * lag < DELTA / 2:
                        continue
                    if (f0 if side == "yes" else 1 - f0) - p - taker_fee(p) < 0:
                        continue
                    c = cells.setdefault((D, b), [0.0, 0.0, 0.0, 0.0, 0])  # pnl*n, n, paid*n, wins*n, fills
                    c[0] += pnl * count
                    c[1] += count
                    c[2] += (p + taker_fee(p)) * count
                    c[3] += won * count
                    c[4] += 1
        prev_yes = yes_price
    return cells


def main():
    import sys
    paths = sorted(str(p) for p in DATA.glob("KXBTC15M-*.json.gz"))
    print(f"processing {len(paths)} markets ...", flush=True)
    results = []
    with Pool(12) as pool:
        for i, r in enumerate(pool.imap_unordered(process, paths, chunksize=1), 1):
            if r is not None:
                results.append(r)
            if i % 500 == 0:
                print(f"  {i}/{len(paths)}", file=sys.stderr, flush=True)
    print(f"markets used {len(results)}\n")
    buckets = [f"{lo:.2f}-{hi:.2f}" for lo, hi in zip(EDGES, EDGES[1:])]
    for D in DELAYS_MS:
        print(f"=== D = {D} ms   (band blocks the rows marked x)")
        print(f"  {'price paid':<12}{'c/contract':>20}{'% on risk':>18}{'win rate':>10}{'fills':>10}")
        for b in buckets:
            key = (D, b)
            groups = [(r[key][0], r[key][1]) if key in r else (0.0, 0.0) for r in results]
            risk_groups = [(r[key][0], r[key][2]) if key in r else (0.0, 0.0) for r in results]
            fills = sum(r[key][4] for r in results if key in r)
            if not fills:
                continue
            mean, se, _ = clustered(groups)
            rmean, rse, _ = clustered(risk_groups)
            n = sum(g[1] for g in groups)
            wins = sum(r[key][3] for r in results if key in r) / n
            lo = float(b.split("-")[0]); hi = float(b.split("-")[1])
            blocked = "x" if hi <= 0.15 or lo >= 0.85 else " "
            print(f"{blocked} {b:<12}{100 * mean:+8.2f} +/-{100 * se:5.2f}{100 * rmean:+8.1f}% +/-{100 * rse:4.1f}{100 * wins:9.0f}%{fills / 1e3:9.0f}k")
        for label, sel in (("inside band", lambda lo, hi: lo >= 0.15 and hi <= 0.85), ("outside band", lambda lo, hi: hi <= 0.15 or lo >= 0.85)):
            keys = [(D, b) for b in buckets if sel(float(b.split("-")[0]), float(b.split("-")[1]))]
            groups = [(sum(r[k][0] for k in keys if k in r), sum(r[k][1] for k in keys if k in r)) for r in results]
            risk = [(sum(r[k][0] for k in keys if k in r), sum(r[k][2] for k in keys if k in r)) for r in results]
            mean, se, _ = clustered(groups)
            rmean, rse, _ = clustered(risk)
            print(f"  {label:<12}{100 * mean:+8.2f} +/-{100 * se:5.2f}{100 * rmean:+8.1f}% +/-{100 * rse:4.1f}")
        print()


if __name__ == "__main__":
    main()
