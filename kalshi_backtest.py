"""The same question on the venue a US trader can legally use: Kalshi KXBTC15M.

    python kalshi_backtest.py

Rule (from each market's rules_primary, and expiration_value >= floor_strike
matched the result on every market checked): YES iff the 60-second average of
CF Benchmarks' BRTI before the close >= the 60-second average before the open.
That is the Polymarket rule on a 900-second window, so the model is
backtest.fair_path(window=900) unchanged.

BRTI history is not free. Binance BTCUSDT perp stands in for it at BOTH ends,
so a slow basis cancels in A - K; [A] measures how far the proxy strike sits
from Kalshi's real one.

Timing. Kalshi stamps the match itself, to the microsecond. So unlike the
Polymarket run, D=0 is honest: the model sees trades strictly before the
fill's own second (0-1s old). D=-3 hands it three seconds of the future and is
the positive control.

Fee: 0.07 * P * (1-P) per contract (Kalshi's general trading fee). Kalshi
rounds each ORDER's fee up to the next cent; the public tape shows fills, not
orders, so the rounding is not modelled and real costs on small orders are
somewhat higher than shown here.
"""

import gzip
import json
import math
from multiprocessing import Pool
from pathlib import Path

from backtest import Prices, clustered, fair_path, market_sigma, strike
from fair_value import settle_times, taker_fee

DATA = Path(__file__).with_name("data") / "kalshi"
WINDOW = 900
DELAYS = (-3, 0, 1, 2, 5, 10)
THETAS = (None, 0.0, 0.02, 0.05, 0.10)
BUCKETS = ((1, 120), (120, 600), (600, 840), (840, 885), (885, 900))


def process(path_str):
    try:
        with gzip.open(path_str, "rt") as f:
            m = json.load(f)
    except (OSError, EOFError, ValueError):
        return {"skip": "unreadable file"}
    if m["result"] not in ("yes", "no"):
        return {"skip": "no result"}
    price = Prices()
    start, end = m["open"], m["close"]
    if end - start != WINDOW:
        return {"skip": f"window {end - start}s"}
    K, sigma = strike(price, start), market_sigma(price, start)
    if K is None or sigma is None:
        return {"skip": "no binance"}
    yes_won = m["result"] == "yes"

    # settlement: the exchange's own numbers, then the Binance proxy
    has_strike = m.get("floor_strike") is not None and m.get("expiration_value") is not None
    # some markets publish expiration_value with a thousands separator ("79,604.96")
    ev = float(str(m["expiration_value"]).replace(",", "")) if has_strike else None
    fs = float(str(m["floor_strike"]).replace(",", "")) if has_strike else None
    A = [price(s) for s in settle_times(start, WINDOW, 60)]
    proxy = None
    if all(A):
        a = sum(A) / 60
        proxy = ((a >= K) == yes_won, abs(a - K))
    out = {"exchange_rule": ((ev >= fs) == yes_won) if has_strike else None, "proxy": proxy,
           "strike_basis": (K - fs) if has_strike else None}

    fair = fair_path(price, start, 60, sigma, window=WINDOW)

    # accumulators: cells[(theta_i, delay_i)] = [pnl*count, count, fills]
    cells = {(i, j): [0.0, 0.0, 0] for i in range(len(THETAS)) for j in range(len(DELAYS))}
    buckets = {b: [0.0, 0.0] for b in range(len(BUCKETS))}
    j0 = DELAYS.index(0)
    for t, side, yes_price, count in m["trades"]:
        if not (start + 1 <= t < end):
            continue
        p = yes_price if side == "yes" else 1.0 - yes_price
        won = 1.0 if (side == "yes") == yes_won else 0.0
        pnl = won - p - taker_fee(p)
        for j, D in enumerate(DELAYS):
            f_yes = fair.get(math.floor(t - D))
            if f_yes is None:
                continue
            f = f_yes if side == "yes" else 1.0 - f_yes
            edge = f - p - taker_fee(p)
            for i, theta in enumerate(THETAS):
                if theta is None or edge >= theta:
                    c = cells[(i, j)]
                    c[0] += pnl * count
                    c[1] += count
                    c[2] += 1
            if j == j0 and edge >= 0.02:
                for b, (lo, hi) in enumerate(BUCKETS):
                    if lo <= t - start < hi:
                        buckets[b][0] += pnl * count
                        buckets[b][1] += count

    # calibration samples: model vs the last traded YES price strictly before the moment
    trades = sorted((t, yp) for t, _s, yp, _c in m["trades"])
    samples, k, last = [], 0, None
    for into in (60, 300, 600, 780, 870, 895):
        while k < len(trades) and trades[k][0] < start + into:
            last = trades[k][1]
            k += 1
        samples.append((fair.get(start + into), last, yes_won))
    out.update(cells=cells, buckets=buckets, samples=samples, sigma=sigma)
    return out


def report(results):
    used = [r for r in results if "cells" in r]
    skips = {}
    for r in results:
        if "skip" in r:
            skips[r["skip"]] = skips.get(r["skip"], 0) + 1
    print(f"markets used {len(used)}   skipped {skips}")

    print("\n[A] settlement")
    ex = [r["exchange_rule"] for r in used if r["exchange_rule"] is not None]
    print(f"  exchange: expiration_value >= floor_strike <-> result   {sum(ex)}/{len(ex)}")
    px = [r["proxy"] for r in used if r["proxy"]]
    far = [ok for ok, margin in px if margin > 15]
    print(f"  Binance proxy, 60s avg vs 60s avg                         {sum(ok for ok, _ in px)}/{len(px)} "
          f"({sum(ok for ok, _ in px) / max(len(px), 1):.1%});  |margin| > $15: {sum(far)}/{len(far)}")
    basis = sorted(r["strike_basis"] for r in used if r["strike_basis"] is not None)
    q = lambda f: basis[int(f * (len(basis) - 1))]
    print(f"  proxy strike - Kalshi strike ($): p10 {q(.1):+.2f}  median {q(.5):+.2f}  p90 {q(.9):+.2f}")

    print("\n[A2] calibration: model P(yes) buckets vs outcome, and Brier vs Kalshi's last trade")
    smp = [(f, px_, won) for r in used for f, px_, won in r["samples"] if f is not None]
    print(f"  {'model P(yes)':>12} {'samples':>8} {'yes won':>8} {'+/- 2SE':>8}")
    for lo in [i / 10 for i in range(10)]:
        b = [won for f, _, won in smp if lo <= f < lo + 0.1 or (lo == 0.9 and f == 1.0)]
        if b:
            w = sum(b) / len(b)
            print(f"  {lo:>5.1f}-{lo + 0.1:.1f} {len(b):>8} {w:>8.3f} {2 * math.sqrt(max(w * (1 - w), 1e-9) / len(b)):>8.3f}")
    both = [(f, px_, won) for f, px_, won in smp if px_ is not None]
    if both:
        bm = sum((f - won) ** 2 for f, _, won in both) / len(both)
        bk = sum((px_ - won) ** 2 for _, px_, won in both) / len(both)
        print(f"  Brier on {len(both)} samples: model {bm:.4f}   Kalshi last trade {bk:.4f}   (lower wins)")

    print("\n[B] copy every taker fill with model edge >= theta: realised P&L per contract, clustered by market")
    head = "".join(f"{('D=' + str(D) + 's' + (' LEAK' if D < 0 else '')):>28}" for D in DELAYS)
    print(f"  {'':>12}{head}")
    for i, theta in enumerate(THETAS):
        row = []
        for j in range(len(DELAYS)):
            groups = [(r["cells"][(i, j)][0], r["cells"][(i, j)][1]) for r in used]
            n_fills = sum(r["cells"][(i, j)][2] for r in used)
            mean, se, _ = clustered(groups)
            row.append(f"{100 * mean:+6.2f}c +/-{100 * se:4.2f} n={n_fills / 1e6:5.2f}M")
        label = "all takers" if theta is None else f">= {100 * theta:.0f}c"
        print(f"  {label:>12}" + "".join(f"{c:>28}" for c in row))
    print("  cents per contract after fees; +/- one market-clustered SE; n = fills.")

    print("\n[C] D=0, edge >= 2c, by seconds into the 15-minute window")
    for b, (lo, hi) in enumerate(BUCKETS):
        mean, se, M = clustered([tuple(r["buckets"][b]) for r in used])
        contracts = sum(r["buckets"][b][1] for r in used)
        print(f"  {lo:>4}-{hi:<4}s  {100 * mean:+6.2f}c +/- {100 * se:4.2f}   contracts {contracts:>14,.0f}   markets {M}")


def main():
    paths = sorted(str(p) for p in DATA.glob("KXBTC15M-*.json.gz"))
    print(f"processing {len(paths)} Kalshi markets ...")
    with Pool() as pool:
        results = pool.map(process, paths, chunksize=4)
    # the model's own tests live in fair_value.py and backtest.py; run both first
    report(results)


if __name__ == "__main__":
    main()
