"""Confirmation test: do favourites pay in Kalshi's NON-sports markets, on fresh trades?

    python kalshi_longshot_confirm.py fetch    # ~30-60 min, polite rate, cached under data/longshot_confirm/
    python kalshi_longshot_confirm.py          # the verdict

PRE-REGISTERED 2026-09-15, after kalshi_longshot.py and BEFORE any of this data was fetched.

WHERE THE IDEA CAME FROM (stated plainly, because it decides how much this can prove)
-------------------------------------------------------------------------------------
kalshi_longshot.py's six pre-registered tests all failed on the whole exchange. Looking afterwards
at 17 categories, the small non-sports ones showed the classic favourite-longshot bias: taker buys
of favourites >= 90c earned about +2% per $ risked (Mentions +2.2 +/-0.5, Elections +1.7 +/-0.4,
Politics +1.9 +/-0.9, Financials +3.6 +/-0.9). Found by looking, so it is tested here on trades
that analysis never saw.

THE UNIVERSE (fixed now)
------------------------
Every series in that scan's series cache whose category is NOT Sports, Crypto or Unknown, and not a
parlay (KXMVE*): 1,713 series, 13 categories. All of them, not only the four that looked good.

THE SAMPLE
----------
Settled markets in those series that closed 2025-09-15 .. 2026-09-12 (live + historical endpoints).
EVENTS are sampled uniformly at random (seed 11) with one fraction for the whole universe, chosen
from the event count alone, so each category keeps its natural weight; every market of a sampled
event with volume is kept. Every trade in those markets is fetched, and trades inside the 300
minutes kalshi_longshot.py sampled are DROPPED. What is left has never been analysed.

THE STRATEGY MEASURED
---------------------
Buy the side priced >= 0.90 as a taker, at the traded price, hold to settlement. Per trade:
net = won - p - 0.07 * fee_multiplier * p * (1 - p). Return per $ risked = sum(net) / sum(p),
contract-weighted. SEs clustered by EVENT and by SERIES x CLOSE DATE; the larger is used.

PASS requires ALL of:
  P1  pooled net return per $ > 0 with t >= 2.0
  P2  excluding trades in the final 5 minutes before close: > 0 with t >= 1.65
  P3  both halves of the sample (split at the median trade time): > 0
  P4  without the single event that contributes most profit: still > 0
Anything less is FAIL, and the idea is closed. Per-category rows, holding time and daily capacity
are descriptive only.
"""

import gzip
import json
import math
import random
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import kalshi_longshot as ks

DATA = Path(__file__).with_name("data") / "longshot_confirm"
SEED = 11
TARGET_MARKETS = 20000     # markets with volume to fetch trades for; sets the event sampling fraction
FAV = 0.90


def universe():
    sinfo = json.load(gzip.open(ks.DATA / "series.json.gz", "rt"))
    return {s: v for s, v in sinfo.items()
            if not s.startswith("KXMVE") and (v.get("category") or "Unknown") not in ("Sports", "Crypto", "Unknown")}


MAX_PAGES = 10   # per endpoint per series: at most ~20,000 markets listed for one series

# DEVIATION, recorded 2026-09-15 before any trade was fetched: a few series are enormous (KXINXU, the
# S&P 500 hourly range markets, lists ~1,000 markets per 2 days, ~180 pages a year). Paging all of them
# stalled the listing for an hour. Each series now lists at most MAX_PAGES pages per endpoint, newest
# first. Capped series are therefore represented by their most recent months and are UNDER-weighted in
# the pooled sample relative to their true trade share; analyze() prints which series were capped.


def list_series(series):
    """Settled markets of one series closing inside the year (live endpoint, then historical), capped."""
    out = {}
    cur, pages = "", 0
    while pages < MAX_PAGES:
        pages += 1
        d = ks.get(f"/markets?series_ticker={series}&status=settled&min_close_ts={int(ks.START)}"
                   f"&max_close_ts={int(ks.END)}&limit=1000" + (f"&cursor={cur}" if cur else "")) or {}
        for m in d.get("markets") or []:
            out[m["ticker"]] = m
        cur = d.get("cursor")
        if not cur or not d.get("markets"):
            break
    cur, pages = "", 0
    while pages < MAX_PAGES:
        pages += 1
        d = ks.get(f"/historical/markets?series_ticker={series}&limit=1000" + (f"&cursor={cur}" if cur else "")) or {}
        ms = d.get("markets") or []
        oldest = None
        for m in ms:
            close = ks._epoch(m["close_time"])
            oldest = close if oldest is None else min(oldest, close)
            if ks.START <= close <= ks.END:
                out[m["ticker"]] = m
        cur = d.get("cursor")
        if not cur or not ms or (oldest is not None and oldest < ks.START):
            break
    return series, [{"ticker": m["ticker"], "event": m["event_ticker"], "close": m["close_time"], "result": m.get("result"),
                     "volume": float(m.get("volume_fp") or 0)} for m in out.values()]


def fetch_trades(m, cutoff):
    path = DATA / "trades" / f"{m['ticker']}.json.gz"
    if path.exists():
        return 0
    # Trades move to /historical after the cutoff date, so a market that closed AFTER the cutoff can
    # have its early trades there and its late trades live: read both and de-duplicate by trade id.
    prefixes = ["/historical/trades"] if ks._epoch(m["close"]) < cutoff else ["/markets/trades", "/historical/trades"]
    seen, rows = set(), []
    for prefix in prefixes:
        cur = ""
        while True:
            d = ks.get(f"{prefix}?ticker={m['ticker']}&limit=1000" + (f"&cursor={cur}" if cur else "")) or {}
            tr = d.get("trades") or []
            for t in tr:
                if t.get("trade_id") in seen:
                    continue
                seen.add(t.get("trade_id"))
                rows.append([ks._epoch(t["created_time"]), t["taker_side"], float(t["yes_price_dollars"]), float(t["count_fp"])])
            cur = d.get("cursor")
            if not cur or not tr:
                break
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        json.dump(rows, f)
    return len(rows)


def fetch():
    uni = universe()
    DATA.mkdir(parents=True, exist_ok=True)
    # one cache file per series, so a failed request (seen twice on 2026-09-15: the API gave up after
    # retries, then answered the same URL fine) costs only that series and a rerun resumes
    (DATA / "listing").mkdir(parents=True, exist_ok=True)

    def cached_list(series):
        path = DATA / "listing" / f"{series}.json.gz"
        if path.exists():
            return series, json.load(gzip.open(path, "rt"))
        try:
            _, ms = list_series(series)
        except RuntimeError:
            return series, None
        with gzip.open(path, "wt") as f:
            json.dump(ms, f)
        return series, ms

    markets, failed = {}, sorted(uni)
    for attempt in (1, 2, 3):
        print(f"listing settled markets: pass {attempt}, {len(failed):,} series ...", flush=True)
        todo, failed = failed, []
        with ThreadPoolExecutor(4) as ex:
            for i, (s, ms) in enumerate(ex.map(cached_list, todo), 1):
                if ms is None:
                    failed.append(s)
                else:
                    markets[s] = ms
                if i % 100 == 0:
                    print(f"  series {i:,}/{len(todo):,}  markets so far {sum(len(v) for v in markets.values()):,}", flush=True)
        if not failed:
            break
        time.sleep(30)
    if failed:
        print(f"{len(failed)} series could not be listed after 3 passes and are left out: {failed[:10]}", flush=True)
    events = defaultdict(list)
    for s, ms in markets.items():
        # the same cap for every series, including ones listed in full before the cap existed
        ms = sorted(ms, key=lambda m: m["close"], reverse=True)[:2 * MAX_PAGES * 1000]
        for m in ms:
            if m["result"] in ("yes", "no") and m["volume"] > 0:
                events[m["event"]].append({**m, "series": s})
    n_markets = sum(len(v) for v in events.values())
    frac = min(1.0, TARGET_MARKETS / max(n_markets, 1))
    rng = random.Random(SEED)
    chosen = sorted(e for e in events if rng.random() < frac)
    sample = [m for e in chosen for m in events[e]]
    print(f"{len(events):,} events / {n_markets:,} markets with volume; sampling fraction {frac:.3f} -> "
          f"{len(chosen):,} events, {len(sample):,} markets", flush=True)
    with gzip.open(DATA / "sample.json.gz", "wt") as f:
        json.dump({"fraction": frac, "markets": sample}, f)
    cutoff = ks._epoch(ks.get("/historical/cutoff")["trades_created_ts"])
    ks.PACE_S = 0.066   # ~15 requests/s for the trade download: 20,000 markets at 8/s was ~80 minutes
    total = 0
    with ThreadPoolExecutor(16) as ex:
        for i, n in enumerate(ex.map(lambda m: fetch_trades(m, cutoff), sample), 1):
            total += n
            if i % 1000 == 0:
                print(f"  markets {i:,}/{len(sample):,}  (+{total:,} trades this run)", flush=True)
    print("fetch done", flush=True)


def analyze():
    uni = universe()
    s = json.load(gzip.open(DATA / "sample.json.gz", "rt"))
    old_windows = sorted(ks.windows())
    import bisect

    def in_old_window(ts):
        i = bisect.bisect_right(old_windows, ts) - 1
        return i >= 0 and ts <= old_windows[i] + ks.WINDOW_S

    trades, dropped_old, missing = [], 0, 0
    for m in s["markets"]:
        path = DATA / "trades" / f"{m['ticker']}.json.gz"
        if not path.exists():
            missing += 1
            continue
        info = uni.get(m["series"]) or {}
        mult = float(info.get("fee_multiplier") or 1) if info.get("fee_type", "quadratic") == "quadratic" else 0.0
        close = ks._epoch(m["close"])
        for ts, side, yes_px, n in json.load(gzip.open(path, "rt")):
            if in_old_window(ts):
                dropped_old += 1
                continue
            p = yes_px if side == "yes" else 1 - yes_px
            if not (FAV <= p < 1.0) or n <= 0:
                continue
            won = 1.0 if m["result"] == side else 0.0
            trades.append({"p": p, "n": n, "won": won, "net": won - p - 0.07 * mult * p * (1 - p), "ts": ts,
                           "event": m["event"], "slate": f"{m['series']}|{m['close'][:10]}",
                           "cat": info.get("category"), "to_close": close - ts})
    capped = [p.name[:-8] for p in (DATA / "listing").glob("*.json.gz")
              if len(json.load(gzip.open(p, "rt"))) >= 9000]
    print(f"series listed at or near the page cap (under-weighted, recent months only): {len(capped)} {sorted(capped)[:12]}")
    print(f"sample: {len(s['markets']):,} markets (event fraction {s['fraction']:.3f}), {missing} without trade files")
    print(f"favourite trades (>= {FAV:.2f}): {len(trades):,}, {sum(t['n'] for t in trades):,.0f} contracts, "
          f"{len({t['event'] for t in trades}):,} events; {dropped_old:,} trades dropped as already analysed\n")

    def row(label, rows):
        r, se = ks.per_dollar(rows)
        c = sum(t["n"] for t in rows)
        win = sum(t["won"] * t["n"] for t in rows) / c if c else float("nan")
        avg = sum(t["p"] * t["n"] for t in rows) / c if c else float("nan")
        print(f"  {label:<44}{c:>13,.0f}{len({t['event'] for t in rows}):>8,}  price {avg:.3f} won {win:.3f}"
              f"   {100 * r:+6.2f}% +/- {100 * se:4.2f}  t={r / se if se else float('nan'):+5.2f}")
        return r, se

    print("=== PRE-REGISTERED")
    r1, s1 = row("P1 all favourites", trades)
    r2, s2 = row("P2 excluding the final 5 minutes", [t for t in trades if t["to_close"] >= 300])
    med = sorted(t["ts"] for t in trades)[len(trades) // 2]
    r3a, _ = row("P3 first half", [t for t in trades if t["ts"] < med])
    r3b, _ = row("P3 second half", [t for t in trades if t["ts"] >= med])
    by_event = defaultdict(float)
    for t in trades:
        by_event[t["event"]] += t["net"] * t["n"]
    top = max(by_event, key=by_event.get)
    r4, _ = row(f"P4 without top event ({top[:28]})", [t for t in trades if t["event"] != top])
    checks = [r1 > 0 and s1 and r1 / s1 >= 2.0, r2 > 0 and s2 and r2 / s2 >= 1.65, r3a > 0 and r3b > 0, r4 > 0]
    print(f"\n  VERDICT: {'PASS' if all(checks) else 'FAIL'}   (P1..P4 = {checks})\n")

    print("=== DESCRIPTIVE: by category")
    cats = defaultdict(list)
    for t in trades:
        cats[t["cat"]].append(t)
    for cat, rows in sorted(cats.items(), key=lambda kv: -sum(t["n"] for t in kv[1])):
        row(cat or "?", rows)
    print("\n=== DESCRIPTIVE: price and holding time")
    row("0.90-0.95", [t for t in trades if t["p"] < 0.95])
    row("0.95-1.00", [t for t in trades if t["p"] >= 0.95])
    for label, lo, hi in (("< 5 min to close", 0, 300), ("5 min - 1 h", 300, 3600), ("1 h - 1 day", 3600, 86400),
                          ("1 - 7 days", 86400, 7 * 86400), ("> 7 days", 7 * 86400, float("inf"))):
        row(label, [t for t in trades if lo <= t["to_close"] < hi])
    days = (max(t["ts"] for t in trades) - min(t["ts"] for t in trades)) / 86400
    print(f"\n  capacity: {sum(t['n'] for t in trades) / s['fraction'] / days:,.0f} favourite contracts/day traded "
          f"in this universe (sample scaled by 1/{s['fraction']:.3f})")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "fetch":
        fetch()
    else:
        analyze()
