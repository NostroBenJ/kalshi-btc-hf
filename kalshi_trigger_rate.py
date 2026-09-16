"""How often would the news trigger fire? Counts, not P&L, over 30 days of Kalshi history.

    python kalshi_trigger_rate.py

Scans every KXBTC15M market on a 50 ms grid (Binance prices for the model, Kalshi's
last traded price for the market) with exactly the bot's rule:
    |model move over 1,000 ms| >= delta, Kalshi's move lags by >= delta/2,
    and the traded side's price is below fair after the fee,
with one entry per side per 2 s. Reports signals per market and per day.
Kalshi's last trade stands in for the ask (history has no quotes), so this is a
count of opportunities to TRY, not of fills.
"""

import bisect
import gzip
import json
from collections import defaultdict
from datetime import datetime, timezone
from multiprocessing import Pool
from pathlib import Path

from backtest import Prices, market_sigma, strike
from fair_value import fair_up, settle_times, taker_fee
from kalshi_ms import Grid

DATA = Path(__file__).with_name("data") / "kalshi"
DELTAS = (0.02, 0.05)
COOLDOWN_MS = 2000


def count(path_str):
    try:
        with gzip.open(path_str, "rt") as f:
            m = json.load(f)
    except (OSError, EOFError, ValueError):
        return None
    start, end = m["open"], m["close"]
    if end - start != 900:
        return None
    sec, grid = Prices(), Grid()
    K, sigma = strike(sec, start), market_sigma(sec, start)
    if K is None or sigma is None:
        return None
    times = settle_times(start, 900, 60)
    trades = sorted(m["trades"])
    t_ms = [int(t * 1000) for t, *_ in trades]
    y_px = [yp for _, _, yp, _ in trades]
    fair = {}
    out = {d: {"all": 0, "last60": 0} for d in DELTAS}
    last = {(d, s): -10**12 for d in DELTAS for s in ("yes", "no")}
    for slot_ms in range(start * 1000 + 1000, end * 1000 - 1000, 50):
        for ms in (slot_ms, slot_ms - 1000):
            if ms not in fair:
                now = ms / 1000
                S = grid(ms)
                known = {s: sec(s) for s in times if s <= now}
                fair[ms] = (fair_up(now, times, known, S, K, sigma)[0]
                            if S is not None and start + 1 <= now and all(v is not None for v in known.values()) else None)
        f0, f1 = fair[slot_ms], fair[slot_ms - 1000]
        i0, i1 = bisect.bisect_left(t_ms, slot_ms) - 1, bisect.bisect_left(t_ms, slot_ms - 1000) - 1
        if f0 is None or f1 is None or i0 < 0 or i1 < 0:
            continue
        m0, m1 = y_px[i0], y_px[i1]
        d_fair = f0 - f1
        lag = d_fair - (m0 - m1)
        last60 = slot_ms >= (end - 60) * 1000
        for d in DELTAS:
            for side, sign, price, fair_side in (("yes", 1, m0, f0), ("no", -1, 1 - m0, 1 - f0)):
                if sign * d_fair >= d and sign * lag >= d / 2 and fair_side - price - taker_fee(price) >= 0 \
                        and slot_ms - last[(d, side)] >= COOLDOWN_MS:
                    last[(d, side)] = slot_ms
                    out[d]["all"] += 1
                    out[d]["last60"] += last60
        # keep the cache small: only the last ~1 s of slots is ever re-read
        if len(fair) > 100:
            for k in [k for k in fair if k < slot_ms - 1100]:
                del fair[k]
    return datetime.fromtimestamp(start, timezone.utc).date().isoformat(), out


def main():
    paths = sorted(str(p) for p in DATA.glob("KXBTC15M-*.json.gz"))
    print(f"counting trigger signals over {len(paths)} markets ...", flush=True)
    per_day = defaultdict(lambda: {d: [0, 0, 0] for d in DELTAS})  # signals, last-minute signals, markets
    with Pool(12) as pool:
        for r in pool.imap_unordered(count, paths, chunksize=2):
            if r is None:
                continue
            day, out = r
            for d in DELTAS:
                per_day[day][d][0] += out[d]["all"]
                per_day[day][d][1] += out[d]["last60"]
                per_day[day][d][2] += 1
    days = sorted(per_day)
    print(f"\n{'day (UTC)':<12}" + "".join(f"{'moved ' + str(int(100 * d)) + 'c/1s: signals (last min)':>34}" for d in DELTAS))
    for day in days:
        print(f"{day:<12}" + "".join(f"{per_day[day][d][0]:>22,} ({per_day[day][d][1]:>5,})     " for d in DELTAS))
    for d in DELTAS:
        full = [per_day[x][d][0] for x in days if per_day[x][d][2] >= 90]  # days with ~all 96 markets
        mk = sum(per_day[x][d][2] for x in days)
        tot = sum(per_day[x][d][0] for x in days)
        if full:
            full.sort()
            print(f"\nmoved {int(100 * d)}c/1s: {tot / mk:.1f} signals per 15-min market; per full day "
                  f"median {full[len(full) // 2]:,}, quietest {full[0]:,}, busiest {full[-1]:,}")


if __name__ == "__main__":
    main()
