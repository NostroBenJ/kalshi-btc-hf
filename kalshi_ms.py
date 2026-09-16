"""The news trigger at millisecond delays: how fast does a bot have to be?

    python kalshi_ms.py

Same test as kalshi_trigger.py, with D in milliseconds on a 50 ms Binance grid
(history.py binance-grid; equal to the 1s grid at every whole second).

Pre-registered, to keep the search honest: kalshi_trigger.py already looked at
14 variants, so this one fixes the question before looking —
  trigger: model fair moved >= 2c or >= 5c in the prior 1,000 ms, Kalshi lagged
           by at least half of it, fill bought in that direction with edge >= 0
  spans:   whole window, and the final 60 s
  delays:  -3000 (leak) 0 100 250 500 750 1000 2000 ms
and a split by date (first half / second half of the 30 days) for the 250 ms
row, since one pooled number can hide an edge that lived in one week.

What D means. Information as of t - D on BINANCE'S clock. A US bot also pays
Binance -> US transit (~150 ms) plus the trip to Kalshi (~20 ms, measured), so
~200 ms is the realistic floor on Binance data, before any processing. Binance
and Kalshi clocks are not synchronised to each other; tens of ms of offset are
within this test's error. The grid adds up to 50 ms of extra staleness.
"""

import array
import gzip
import json
from datetime import datetime, timezone
from multiprocessing import Pool
from pathlib import Path

from backtest import Prices, clustered, market_sigma, strike
from fair_value import fair_up, settle_times, taker_fee

DATA = Path(__file__).with_name("data")
WINDOW = 900
SLOT = 50
DELAYS_MS = (-3000, 0, 100, 250, 500, 750, 1000, 2000)
DELTAS = (0.02, 0.05)
LOOKBACK_MS = 1000
SPLIT = datetime(2026, 8, 28, tzinfo=timezone.utc).timestamp()


class Grid:
    """price_at(ms) = last Binance trade strictly before the start of that 50 ms slot."""

    def __init__(self):
        self.days = {}

    def __call__(self, ms):
        slot_ms = ms - ms % SLOT
        d0 = slot_ms // 1000 - (slot_ms // 1000) % 86400
        a = self.days.get(d0)
        if a is None:
            a = array.array("d")
            path = DATA / "binance" / f"{datetime.fromtimestamp(d0, timezone.utc).date().isoformat()}.g50"
            if path.exists():
                with open(path, "rb") as f:
                    a.fromfile(f, 1728000)
            self.days[d0] = a
        if not a:
            return None
        p = a[(slot_ms - d0 * 1000) // SLOT]
        return p if p == p else None


def process(path_str):
    try:
        with gzip.open(path_str, "rt") as f:
            m = json.load(f)
    except (OSError, EOFError, ValueError):
        return None
    if m["result"] not in ("yes", "no") or m["close"] - m["open"] != WINDOW:
        return None
    sec, grid = Prices(), Grid()
    start, end = m["open"], m["close"]
    K, sigma = strike(sec, start), market_sigma(sec, start)
    if K is None or sigma is None:
        return None
    yes_won = m["result"] == "yes"
    times = settle_times(start, WINDOW, 60)
    cache = {}

    def fair_at(ms):
        """P(yes) using only prices before the start of the 50 ms slot containing ms."""
        slot_ms = ms - ms % SLOT
        if slot_ms in cache:
            return cache[slot_ms]
        now = slot_ms / 1000
        val = None
        if start + 1 <= now < end:
            S = grid(slot_ms)
            known = {s: sec(s) for s in times if s <= now}
            if S is not None and all(v is not None for v in known.values()):
                val = fair_up(now, times, known, S, K, sigma)[0]
        cache[slot_ms] = val
        return val

    trades = sorted(m["trades"])
    yes_ms = [int(t * 1000) for t, _, _, _ in trades]
    yes_px = [yp for _, _, yp, _ in trades]
    import bisect

    def last_yes_before(ms):
        i = bisect.bisect_left(yes_ms, ms) - 1
        return yes_px[i] if i >= 0 else None

    half = "h1" if start < SPLIT else "h2"
    cells = {}
    prev_yes = None
    for t, side, yes_price, count in trades:
        if start + 1 <= t < end:
            t_ms = int(t * 1000)
            p = yes_price if side == "yes" else 1.0 - yes_price
            pnl = (1.0 if (side == "yes") == yes_won else 0.0) - p - taker_fee(p)
            spans = ("all", "last60") if t >= end - 60 else ("all",)
            for D in DELAYS_MS:
                f0 = fair_at(t_ms - D)
                if f0 is None:
                    continue
                edge = (f0 if side == "yes" else 1 - f0) - p - taker_fee(p)
                if edge < 0:
                    continue
                keys = [("base", span) for span in spans]
                f1 = fair_at(t_ms - D - LOOKBACK_MS)
                m1 = last_yes_before(t_ms - D - LOOKBACK_MS)
                if f1 is not None and m1 is not None and prev_yes is not None:
                    d_fair = f0 - f1
                    lag = d_fair - (prev_yes - m1)
                    for delta in DELTAS:
                        if (side == "yes" and d_fair >= delta and lag >= delta / 2) or \
                           (side == "no" and d_fair <= -delta and lag <= -delta / 2):
                            keys += [(delta, span) for span in spans]
                            if D == 250 and delta == 0.05:
                                keys.append((delta, "all", half))
                for key in keys:
                    c = cells.setdefault((D,) + key, [0.0, 0.0, 0])
                    c[0] += pnl * count
                    c[1] += count
                    c[2] += 1
        prev_yes = yes_price
    return cells


def safe_process(path_str):
    """A worker error comes back as data. The first full run hung on 2026-09-13
    with every worker idle and pool.map waiting forever; this makes that impossible
    to repeat silently."""
    import traceback
    try:
        return path_str, process(path_str), None
    except Exception:
        return path_str, None, traceback.format_exc(limit=3)


def main():
    import sys
    paths = sorted(str(p) for p in (DATA / "kalshi").glob("KXBTC15M-*.json.gz"))
    print(f"processing {len(paths)} Kalshi markets at 50 ms resolution ...", flush=True)
    results, errors = [], []
    with Pool(12) as pool:
        for i, (path, r, err) in enumerate(pool.imap_unordered(safe_process, paths, chunksize=1), 1):
            if err:
                errors.append((path, err))
            elif r is not None:
                results.append(r)
            if i % 250 == 0 or i == len(paths):
                print(f"  {i}/{len(paths)} done, {len(errors)} errors", flush=True, file=sys.stderr)
    for path, err in errors[:3]:
        print(f"ERROR {Path(path).name}:\n{err}", flush=True)
    print(f"markets used {len(results)}   errors {len(errors)}\n")

    def stat(key):
        groups = [tuple(r[key][:2]) if key in r else (0.0, 0.0) for r in results]
        fills = sum(r[key][2] for r in results if key in r)
        mean, se, _ = clustered(groups)
        return mean, se, fills

    for span, title in (("all", "WHOLE 15-MINUTE WINDOW"), ("last60", "FINAL 60 SECONDS")):
        print(f"=== {title}: P&L per contract after fees (cents), market-clustered SE")
        header = f"  {'delay (Binance clock)':<24}{'no trigger':>26}" + "".join(f"{'moved ' + str(int(100 * d)) + 'c in 1s':>26}" for d in DELTAS)
        print(header)
        for D in DELAYS_MS:
            label = f"{D:+d} ms" + (" LEAK" if D < 0 else "") + ("  <- ~US floor" if D == 250 else "")
            cells = []
            for key in [(D, "base", span)] + [(D, d, span) for d in DELTAS]:
                mean, se, fills = stat(key)
                cells.append(f"{100 * mean:+6.2f} +/-{100 * se:4.2f} ({fills / 1e3:.0f}k)" if fills else "none")
            print(f"  {label:<24}" + "".join(f"{c:>26}" for c in cells))
        print()

    print("=== stability: moved 5c in 1s, whole window, D = +250 ms, by half of the sample")
    for half, label in (("h1", "Aug 13 - Aug 27"), ("h2", "Aug 28 - Sep 11")):
        mean, se, fills = stat((250, 0.05, "all", half))
        print(f"  {label}: {100 * mean:+.2f}c +/- {100 * se:.2f}  ({fills / 1e3:.0f}k fills)")


if __name__ == "__main__":
    main()
