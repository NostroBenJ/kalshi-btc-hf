"""News trigger: does taking Kalshi's book pay when BTC has JUST moved and the
market has not caught up — as opposed to whenever the model merely disagrees?

    python kalshi_trigger.py

kalshi_backtest.py copied every fill the model called cheap. Most of those were
the model's own error (Kalshi's last trade out-forecasts it). A speed edge is
narrower: in the last k seconds the model's fair value moved by at least delta,
Kalshi's traded price moved by less, and the fill bought in the direction of
the move with positive model edge after fees.

For a fill at time t, information as of second s0 = floor(t - D):

    d_fair = fair[s0] - fair[s0 - k]                 (model, from Binance prices)
    d_mkt  = last YES trade before this fill - last YES trade before s0 - k
    lag    = d_fair - d_mkt                          (how far Kalshi is behind)

    YES buy qualifies:  d_fair >=  delta  and  lag >=  delta/2  and edge >= 0
    NO  buy qualifies:  d_fair <= -delta  and  lag <= -delta/2  and edge >= 0

D=-3 (sees the future) is the positive control, D=0 the honest fastest case.
"Last minute" = the final 60s, while the settlement average is being formed.
Errors are clustered by market, as everywhere else in this project.
"""

import gzip
import json
import math
from multiprocessing import Pool
from pathlib import Path

from backtest import Prices, clustered, fair_path, market_sigma, strike
from fair_value import taker_fee

DATA = Path(__file__).with_name("data") / "kalshi"
WINDOW = 900
DELAYS = (-3, 0, 1, 2)
LOOKBACKS = (1, 3)
DELTAS = (0.01, 0.02, 0.05)
SPANS = ("all", "last60")


def process(path_str):
    try:
        with gzip.open(path_str, "rt") as f:
            m = json.load(f)
    except (OSError, EOFError, ValueError):
        return None
    if m["result"] not in ("yes", "no") or m["close"] - m["open"] != WINDOW:
        return None
    price = Prices()
    start, end = m["open"], m["close"]
    K, sigma = strike(price, start), market_sigma(price, start)
    if K is None or sigma is None:
        return None
    fair = fair_path(price, start, 60, sigma, window=WINDOW)
    yes_won = m["result"] == "yes"

    trades = sorted(m["trades"])
    # last YES trade price strictly before each whole second of the window
    last_yes, j, cur = {}, 0, None
    for s in range(start - 5, end + 1):
        while j < len(trades) and trades[j][0] < s:
            cur = trades[j][2]
            j += 1
        last_yes[s] = cur

    # cells[(k, delta_i, D_i, span)] = [pnl*count, count, fills]; plus the untriggered baseline
    cells = {}
    prev_yes = None
    for t, side, yes_price, count in trades:
        if start + 1 <= t < end:
            p = yes_price if side == "yes" else 1.0 - yes_price
            pnl = (1.0 if (side == "yes") == yes_won else 0.0) - p - taker_fee(p)
            spans = ("all", "last60") if t >= end - 60 else ("all",)
            for di, D in enumerate(DELAYS):
                s0 = math.floor(t - D)
                f0 = fair.get(s0)
                if f0 is None:
                    continue
                f_side = f0 if side == "yes" else 1.0 - f0
                edge = f_side - p - taker_fee(p)
                if edge < 0:
                    continue
                for span in spans:
                    key = ("base", 0, di, span)
                    c = cells.setdefault(key, [0.0, 0.0, 0])
                    c[0] += pnl * count; c[1] += count; c[2] += 1
                for k in LOOKBACKS:
                    f1, m1 = fair.get(s0 - k), last_yes.get(s0 - k)
                    if f1 is None or m1 is None or prev_yes is None:
                        continue
                    d_fair = f0 - f1
                    lag = d_fair - (prev_yes - m1)
                    for xi, delta in enumerate(DELTAS):
                        ok = (side == "yes" and d_fair >= delta and lag >= delta / 2) or \
                             (side == "no" and d_fair <= -delta and lag <= -delta / 2)
                        if not ok:
                            continue
                        for span in spans:
                            c = cells.setdefault((k, xi, di, span), [0.0, 0.0, 0])
                            c[0] += pnl * count; c[1] += count; c[2] += 1
        prev_yes = yes_price
    return cells


def cell(results, key):
    groups = [tuple(r[key][:2]) if key in r else (0.0, 0.0) for r in results]
    fills = sum(r[key][2] for r in results if key in r)
    mean, se, _ = clustered(groups)
    return mean, se, fills, sum(n for _, n in groups)


def main():
    paths = sorted(str(p) for p in DATA.glob("KXBTC15M-*.json.gz"))
    print(f"processing {len(paths)} Kalshi markets ...")
    with Pool() as pool:
        results = [r for r in pool.map(process, paths, chunksize=4) if r is not None]
    print(f"markets used {len(results)}\n")

    def row(label, key_fn):
        out = []
        for di, D in enumerate(DELAYS):
            mean, se, fills, contracts = cell(results, key_fn(di))
            out.append(f"{100 * mean:+6.2f}c +/-{100 * se:4.2f} n={fills / 1e3:7.1f}k" if fills else f"{'none':>26}")
        print(f"  {label:<26}" + "".join(f"{c:>28}" for c in out))

    for span in SPANS:
        title = "whole 15-minute window" if span == "all" else "LAST MINUTE ONLY (settlement average forming)"
        print(f"=== {title}: realised P&L per contract after fees, clustered by market")
        print(f"  {'':<26}" + "".join(f"{('D=' + str(D) + 's' + (' LEAK' if D < 0 else '')):>28}" for D in DELAYS))
        row("no trigger (edge >= 0)", lambda di: ("base", 0, di, span))
        for k in LOOKBACKS:
            for xi, delta in enumerate(DELTAS):
                row(f"moved {100 * delta:.0f}c in last {k}s", lambda di, k=k, xi=xi: (k, xi, di, span))
        print()
    print("Read it as: a real speed edge shows up as trigger rows beating the no-trigger row at D=0,")
    print("growing with delta, and fading as D grows. The LEAK column must be the best, or the test is blind.")


if __name__ == "__main__":
    main()
