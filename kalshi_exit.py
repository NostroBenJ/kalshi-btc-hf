"""Should the bot EXIT news-trigger trades instead of holding them to settlement? And does a
minimum edge help? 30 days of Kalshi KXBTC15M, same trigger and 50 ms grid as kalshi_ms.py.

    python kalshi_exit.py

Why: the trigger bets that Kalshi REPRICES within seconds, not on who wins the 15 minutes.
Holding to settlement turns a ~3-6c edge into a +/-50c coin flip. Paper trading on the first (burstable) server
showed exactly that shape: wins ~+17, losses ~-24 per order.

Entries: every real taker fill the trigger would have copied (2c model move in 1 s, Kalshi
lagging by >= 1c, edge after fee >= MIN_EDGE), at information delay D.

Exits compared, per contract, after all costs:
  hold            settle at the market's result (what the bot does now)
  sell after N s  sell at Kalshi's last traded price N s later, minus HALF_SPREAD, minus the
                  taker fee on the sale. If the market closes first, it settles instead.
  catch-up        sell as soon as the traded price has moved our way by the lag we entered on,
                  or at 10 s, whichever comes first.

History has trades, not quotes, so "last traded price" stands in for the bid: HALF_SPREAD
charges half a typical 1c spread on every exit. Errors are clustered by market.
"""

import bisect
import gzip
import json
from multiprocessing import Pool
from pathlib import Path

from backtest import Prices, clustered, market_sigma, strike
from fair_value import fair_up, settle_times, taker_fee
from kalshi_ms import Grid

DATA = Path(__file__).with_name("data") / "kalshi"
DELTA = 0.02
DELAYS_MS = (-3000, 0, 100, 250)
MIN_EDGES = (0.0, 0.01, 0.02)
HOLDS_S = (1, 3, 5, 10, 30, 60)
HALF_SPREAD = 0.005
SLOT = 50


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

    trades = sorted(m["trades"])
    t_ms = [int(t * 1000) for t, *_ in trades]
    y_px = [yp for _, _, yp, _ in trades]

    def yes_before(ms):
        i = bisect.bisect_left(t_ms, ms) - 1
        return y_px[i] if i >= 0 else None

    cells = {}

    def add(key, pnl, count):
        c = cells.setdefault(key, [0.0, 0.0, 0])
        c[0] += pnl * count
        c[1] += count
        c[2] += 1

    prev_yes = None
    for t, side, yes_price, count in trades:
        if start + 1 <= t < end - 1:
            ms = int(t * 1000)
            p = yes_price if side == "yes" else 1.0 - yes_price
            for D in DELAYS_MS:
                f0, f1 = fair_at(ms - D), fair_at(ms - D - 1000)
                m1 = yes_before(ms - D - 1000)
                if None in (f0, f1, m1, prev_yes):
                    continue
                d_fair = f0 - f1
                lag = d_fair - (prev_yes - m1)
                sign = 1 if side == "yes" else -1
                if sign * d_fair < DELTA or sign * lag < DELTA / 2:
                    continue
                edge = (f0 if side == "yes" else 1 - f0) - p - taker_fee(p)
                won = 1.0 if (side == "yes") == yes_won else 0.0
                hold_pnl = won - p - taker_fee(p)
                for e in MIN_EDGES:
                    if edge >= e:
                        add((D, e, "hold"), hold_pnl, count)
                if edge < 0:
                    continue

                def exit_value(exit_ms):
                    if exit_ms >= end * 1000:
                        return won, 0.0  # market closed first: settles
                    yp = yes_before(exit_ms)
                    if yp is None:
                        return None, None
                    px = yp if side == "yes" else 1.0 - yp
                    px = max(0.0, px - HALF_SPREAD)
                    return px, taker_fee(px)

                for N in HOLDS_S:
                    px, fee_out = exit_value(ms + N * 1000)
                    if px is not None:
                        add((D, 0.0, f"sell {N}s"), px - fee_out - p - taker_fee(p), count)
                # catch-up exit: first trade within 10 s that has moved our way by the entry lag
                target = abs(lag)
                i = bisect.bisect_right(t_ms, ms)
                exit_ms = ms + 10_000
                while i < len(t_ms) and t_ms[i] <= ms + 10_000:
                    moved = (y_px[i] - yes_price) if side == "yes" else (yes_price - y_px[i])
                    if moved >= target:
                        exit_ms = t_ms[i] + 1
                        break
                    i += 1
                px, fee_out = exit_value(exit_ms)
                if px is not None:
                    add((D, 0.0, "catch-up"), px - fee_out - p - taker_fee(p), count)
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

    def row(key):
        groups = [tuple(r[key][:2]) if key in r else (0.0, 0.0) for r in results]
        fills = sum(r[key][2] for r in results if key in r)
        mean, se, _ = clustered(groups)
        return f"{100 * mean:+6.2f}c +/-{100 * se:4.2f} ({fills / 1e3:5.0f}k)" if fills else "none"

    head = "".join(f"{('D=' + str(D) + 'ms' + (' LEAK' if D < 0 else '')):>30}" for D in DELAYS_MS)
    print("=== MINIMUM EDGE, held to settlement (P&L per contract after fees, market-clustered SE)")
    print(f"  {'':<24}{head}")
    for e in MIN_EDGES:
        print(f"  {'edge >= ' + str(int(100 * e)) + 'c':<24}" + "".join(f"{row((D, e, 'hold')):>30}" for D in DELAYS_MS))
    print("\n=== EXIT RULE (edge >= 0 entries)")
    print(f"  {'':<24}{head}")
    for rule in ["hold"] + [f"sell {N}s" for N in HOLDS_S] + ["catch-up"]:
        print(f"  {rule:<24}" + "".join(f"{row((D, 0.0, rule)):>30}" for D in DELAYS_MS))
    print("\nExit prices are last trades minus half a cent; a real bid can be worse in a fast market.")


if __name__ == "__main__":
    main()
