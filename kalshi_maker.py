"""Plan B: is there money on the MAKER side of Kalshi KXBTC15M, and can a small bot keep it?

    python kalshi_maker.py

Every trade on the tape has a taker (crossed the spread) and a maker (resting order hit). The
maker SOLD the side the taker bought, at the same price. kalshi_backtest.py found takers lose
~1.2c/contract after fees; this measures who keeps that and when.

For each fill, from the maker's side (short `side` at p, i.e. long the other side at 1 - p):

  settle      P&L held to settlement: p - won(side), minus a maker fee
  markout Ns  p - price of `side` N seconds later (last traded price): negative means the fill
              was followed by the price moving against the maker = adverse selection
  BTC state   the model's fair value change over the 1 s before the fill (Binance, D = 0):
                calm    |move| < 0.5c
                mild    0.5-2c
                toward  >= 2c in the direction the TAKER bet (taker had news: maker picked off)
                against >= 2c against the taker (taker trading into the move)

Maker fee: Kalshi's per-series schedule is not confirmed here, so both 0 and
0.0175 * p * (1 - p) are shown. Queue position is unknowable from trades, so this says whether
maker fills are worth having, not how many a new bot would get.
"""

import bisect
import gzip
import json
import math
from multiprocessing import Pool
from pathlib import Path

from backtest import Prices, clustered, fair_path, market_sigma, strike

DATA = Path(__file__).with_name("data") / "kalshi"
MARKOUTS = (5, 30, 60)
BUCKETS = ((0.0, 0.10), (0.10, 0.25), (0.25, 0.50), (0.50, 0.75), (0.75, 0.90), (0.90, 1.0))
TIMES = ((0, 120), (120, 600), (600, 840), (840, 900))


def maker_fee(p):
    return 0.0175 * p * (1 - p)


def process(path_str):
    try:
        with gzip.open(path_str, "rt") as f:
            m = json.load(f)
    except (OSError, EOFError, ValueError):
        return None
    if m["result"] not in ("yes", "no") or m["close"] - m["open"] != 900:
        return None
    price = Prices()
    start, end = m["open"], m["close"]
    K, sigma = strike(price, start), market_sigma(price, start)
    if K is None or sigma is None:
        return None
    fair = fair_path(price, start, 60, sigma, window=900)
    yes_won = m["result"] == "yes"
    trades = sorted(m["trades"])
    ts = [t for t, *_ in trades]
    yp = [y for _, _, y, _ in trades]

    def yes_at(t):
        i = bisect.bisect_right(ts, t) - 1
        return yp[i] if i >= 0 else None

    cells = {}

    def add(key, value, count):
        c = cells.setdefault(key, [0.0, 0.0])
        c[0] += value * count
        c[1] += count

    for t, taker_side, yes_price, count in trades:
        if not (start + 1 <= t < end - 1):
            continue
        p = yes_price if taker_side == "yes" else 1.0 - yes_price  # price of the side the maker sold
        won = 1.0 if (taker_side == "yes") == yes_won else 0.0      # did the side the maker sold win?
        settle = p - won
        f0, f1 = fair.get(math.floor(t)), fair.get(math.floor(t) - 1)
        if f0 is None or f1 is None:
            state = "unknown"
        else:
            move = (f0 - f1) * (1 if taker_side == "yes" else -1)  # + means BTC moved the taker's way
            state = "calm" if abs(move) < 0.005 else "mild" if abs(move) < 0.02 else "toward" if move > 0 else "against"
        bucket = next(f"{lo:.2f}-{hi:.2f}" for lo, hi in BUCKETS if lo <= p < hi or (hi == 1.0 and p == 1.0))
        into = t - start
        tbin = next(f"{lo}-{hi}s" for lo, hi in TIMES if lo <= into < hi)
        for fee_label, fee in (("fee0", 0.0), ("fee1.75", maker_fee(p))):
            v = settle - fee
            add(("all", fee_label), v, count)
            add(("state", state, fee_label), v, count)
            add(("price", bucket, fee_label), v, count)
            add(("time", tbin, fee_label), v, count)
            add(("state_price", state, bucket, fee_label), v, count)
        add(("taker_check",), -settle - 0.07 * p * (1 - p), count)
        for n in MARKOUTS:
            later = yes_at(t + n) if t + n < end else None
            if later is None:
                continue
            later_side = later if taker_side == "yes" else 1.0 - later
            add(("markout", n, state), p - later_side, count)
            add(("markout", n, "all"), p - later_side, count)
    return cells


def stat(results, key):
    groups = [tuple(r[key]) if key in r else (0.0, 0.0) for r in results]
    mean, se, _ = clustered(groups)
    return mean, se, sum(g[1] for g in groups)


def main():
    import sys
    paths = sorted(str(p) for p in DATA.glob("KXBTC15M-*.json.gz"))
    print(f"processing {len(paths)} markets ...", flush=True)
    results = []
    with Pool(12) as pool:
        for i, r in enumerate(pool.imap_unordered(process, paths, chunksize=2), 1):
            if r is not None:
                results.append(r)
            if i % 500 == 0:
                print(f"  {i}/{len(paths)}", file=sys.stderr, flush=True)
    print(f"markets used {len(results)}\n")

    row = lambda label, key: (lambda m, s, n: print(f"  {label:<34}{100 * m:+7.2f}c +/-{100 * s:5.2f}{n / 1e6:9.2f}M contracts"))(*stat(results, key))
    print("=== sanity: taker side after 7% fee (kalshi_backtest.py found -1.20c)")
    row("taker P&L per contract", ("taker_check",))

    for fee in ("fee0", "fee1.75"):
        print(f"\n=== MAKER P&L held to settlement, per contract ({'no maker fee' if fee == 'fee0' else 'maker fee 0.0175*p*(1-p)'})")
        row("all maker fills", ("all", fee))
        print("  -- by BTC state in the second before the fill")
        for st in ("calm", "mild", "against", "toward", "unknown"):
            row(st, ("state", st, fee))
        print("  -- by price of the side the maker sold")
        for lo, hi in BUCKETS:
            row(f"{lo:.2f}-{hi:.2f}", ("price", f"{lo:.2f}-{hi:.2f}", fee))
        print("  -- by time in the window")
        for lo, hi in TIMES:
            row(f"{lo}-{hi}s", ("time", f"{lo}-{hi}s", fee))

    print("\n=== CALM fills only, by price (maker fee 0.0175) — the quote-when-calm strategy")
    for lo, hi in BUCKETS:
        row(f"calm {lo:.2f}-{hi:.2f}", ("state_price", "calm", f"{lo:.2f}-{hi:.2f}", "fee1.75"))

    print("\n=== MARKOUTS (maker's side, before fees): negative = price moved against the maker after the fill")
    print(f"  {'':<12}" + "".join(f"{str(n) + ' s':>26}" for n in MARKOUTS))
    for st in ("all", "calm", "mild", "against", "toward"):
        cells = []
        for n in MARKOUTS:
            m_, s_, _ = stat(results, ("markout", n, st))
            cells.append(f"{100 * m_:+7.2f}c +/-{100 * s_:5.2f}")
        print(f"  {st:<12}" + "".join(f"{c:>26}" for c in cells))


if __name__ == "__main__":
    main()
