"""Are longshots overpriced on Kalshi? A calibration scan across every category.

    python kalshi_longshot.py fetch      # ~10-20 min, polite rate, cached under data/longshot/
    python kalshi_longshot.py            # analysis on the cache

PRE-REGISTERED 2026-09-15, before any statistic below was computed.

THE IDEA
--------
The favourite-longshot bias: in betting markets, cheap contracts (longshots) win LESS often than
their price says and expensive ones (favourites) win MORE often. If Kalshi has it, a slow, boring,
speed-free trade exists: buy favourites / sell longshots. If Kalshi doesn't, the idea is closed.

THE SAMPLE
----------
Kalshi prints ~7,000 trades a minute (measured 2026-09-15), mostly parlays, sports and 15-minute
crypto. Listing every settled market is impossible (1,000 parlay markets settle every ~14 min), so
the unit is the TRADE: 300 random 60-second windows (seed 7) spread over 2025-09-15 .. 2026-09-12,
every trade in each window. Trades before 2026-07-17 come from /historical/trades. Each trade is
joined to its market's result and its series' category and fee.

ASSUMPTION (sample): trades in markets that have NOT settled yet are dropped and counted. That drops
long-dated markets (elections, year-end) more than short ones, so the scan speaks mainly for markets
that resolve within months. Voided / scalar results are dropped and counted too.

THE MEASUREMENTS (the taker bought `side` at price p; won = result == side)
-----------------------------------------------------------------------------
  calibration   contract-weighted win rate vs average price, per price bucket
  taker gross   won - p per contract
  taker net     won - p - fee, fee = 0.07 * fee_multiplier * p * (1 - p) (quadratic series)
  per $ risked  sum(net) / sum(p): what the strategy returns on the money it puts up
  maker gross   -(won - p): the resting side of the same trades, before any maker fee

Standard errors are clustered by EVENT (mutually exclusive outcomes inside one event are one bet)
and, separately, by SERIES x CLOSE DATE (a slate of games or a day of 15-minute BTC markets move
together). The LARGER of the two is reported.

HYPOTHESES (Bonferroni over 6 tests, |t| > 2.64)
  L1  PRIMARY. Taker gross per contract, longshots (p < 0.10) minus favourites (p >= 0.90).
      Longshot bias predicts negative.
  L2  Buying favourites (p >= 0.90) as a taker: net return per $ risked. The tradeable version.
  L3  Selling longshots as a maker: gross per contract on p < 0.10 trades.
  L4  L2 excluding the final hour before close (a 97c contract a minute before the whistle is not
      the bias, it is the game being over).
  L5  L2 in the half of the year before 2026-03-15 vs after (stable, or one regime?).
  L6  L2 excluding parlays (KXMVE*), which dominate trade counts.
Descriptive tables (by category, by time to close) are not tests.
"""

import bisect
import gzip
import json
import math
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

API = "https://api.elections.kalshi.com/trade-api/v2"
DATA = Path(__file__).with_name("data") / "longshot"
START = datetime(2025, 9, 15, tzinfo=timezone.utc).timestamp()
END = datetime(2026, 9, 12, tzinfo=timezone.utc).timestamp()
SPLIT = datetime(2026, 3, 15, tzinfo=timezone.utc).timestamp()
N_WINDOWS, WINDOW_S, SEED = 300, 60, 7
BUCKETS = [0.0, 0.05, 0.10, 0.20, 0.35, 0.50, 0.65, 0.80, 0.90, 0.95, 1.0001]

PACE_S = 0.125   # seconds between requests (8/s); confirm fetch raises it, 429s back off either way
_rate_lock = threading.Lock()
_next_slot = [0.0]


def get(path, tries=6):
    """GET with a global ~8 requests/s pace and 429 backoff."""
    for attempt in range(tries):
        with _rate_lock:
            wait = _next_slot[0] - time.time()
            _next_slot[0] = max(time.time(), _next_slot[0]) + PACE_S
        if wait > 0:
            time.sleep(wait)
        req = urllib.request.Request(API + path, headers={"User-Agent": "pm-hf-research", "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 429 or e.code >= 500:
                time.sleep(2 ** attempt)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(2 ** attempt)
    raise RuntimeError(f"gave up on {path}")


def _epoch(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


# ---------------------------------------------------------------- fetch

def windows():
    rng = random.Random(SEED)
    starts = []
    while len(starts) < N_WINDOWS:
        t = int(rng.uniform(START, END - WINDOW_S))
        if all(abs(t - s) > 3600 for s in starts):  # no two windows within an hour
            starts.append(t)
    return sorted(starts)


def fetch_window(t0, cutoff):
    path = DATA / "trades" / f"{t0}.json.gz"
    if path.exists():
        return 0
    prefix = "/historical/trades" if t0 + WINDOW_S < cutoff else "/markets/trades"
    out, cur = [], ""
    while True:
        pg = get(f"{prefix}?min_ts={t0}&max_ts={t0 + WINDOW_S}&limit=1000" + (f"&cursor={cur}" if cur else "")) or {}
        tr = pg.get("trades") or []
        out += [[t["ticker"], _epoch(t["created_time"]), t["taker_side"], float(t["yes_price_dollars"]),
                 float(t["count_fp"])] for t in tr]
        cur = pg.get("cursor")
        if not cur or not tr:
            break
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as f:
        json.dump(out, f)
    return len(out)


def fetch_markets(tickers):
    """Result, close time, event and series for each ticker. Live endpoint first, then historical."""
    path = DATA / "markets.json.gz"
    have = json.load(gzip.open(path, "rt")) if path.exists() else {}
    todo = sorted(t for t in tickers if t not in have)
    print(f"markets: {len(have):,} cached, {len(todo):,} to look up", flush=True)

    def info(m):
        return {"result": m.get("result"), "close": m.get("close_time"), "event": m.get("event_ticker"), "status": m.get("status")}

    def batch(chunk):
        """50 tickers per request (the tickers= filter works on both endpoints, checked 2026-09-15)."""
        found = {}
        for prefix in ("/markets", "/historical/markets"):
            need = [t for t in chunk if t not in found]
            if not need:
                break
            try:
                d = get(f"{prefix}?tickers={','.join(need)}&limit=100", tries=3) or {}
            except RuntimeError:
                # one bad request (seen 2026-09-15 on /historical/markets): split and retry the halves,
                # so a single failing ticker costs only itself, counted as "lookup failed"
                if len(chunk) == 1:
                    return {chunk[0]: {"result": None, "close": None, "event": None, "status": "lookup failed"}}
                half = len(chunk) // 2
                return {**batch(chunk[:half]), **batch(chunk[half:])}
            for m in d.get("markets") or []:
                found[m["ticker"]] = info(m)
        return {t: found.get(t, {"result": None, "close": None, "event": None, "status": "not found"}) for t in chunk}

    chunks = [todo[i:i + 50] for i in range(0, len(todo), 50)]
    with ThreadPoolExecutor(8) as ex:
        for i, got in enumerate(ex.map(batch, chunks), 1):
            have.update(got)
            if i % 50 == 0:
                print(f"  {i * 50:,}/{len(todo):,}", flush=True)
                with gzip.open(path, "wt") as f:
                    json.dump(have, f)
    with gzip.open(path, "wt") as f:
        json.dump(have, f)
    return have


def fetch_series(series_tickers):
    path = DATA / "series.json.gz"
    have = json.load(gzip.open(path, "rt")) if path.exists() else {}
    todo = sorted(s for s in series_tickers if s not in have)
    print(f"series: {len(have):,} cached, {len(todo):,} to look up", flush=True)

    def one(s):
        d = get(f"/series/{s}")
        ser = (d or {}).get("series") or {}
        return s, {"category": ser.get("category"), "fee_type": ser.get("fee_type"), "fee_multiplier": ser.get("fee_multiplier")}

    with ThreadPoolExecutor(8) as ex:
        for s, info in ex.map(one, todo):
            have[s] = info
    with gzip.open(path, "wt") as f:
        json.dump(have, f)
    return have


def fetch():
    cutoff = _epoch(get("/historical/cutoff")["trades_created_ts"])
    ws = windows()
    print(f"{len(ws)} windows of {WINDOW_S}s, trades cutoff {datetime.fromtimestamp(cutoff, timezone.utc):%Y-%m-%d}", flush=True)
    total = 0
    with ThreadPoolExecutor(4) as ex:
        for i, n in enumerate(ex.map(lambda t: fetch_window(t, cutoff), ws), 1):
            total += n
            if i % 25 == 0:
                print(f"  windows {i}/{len(ws)}  (+{total:,} trades this run)", flush=True)
    tickers = set()
    for p in (DATA / "trades").glob("*.json.gz"):
        tickers.update(r[0] for r in json.load(gzip.open(p, "rt")))
    markets = fetch_markets(tickers)
    fetch_series({(m["event"] or t).split("-")[0] for t, m in markets.items()})
    print("fetch done", flush=True)


# ---------------------------------------------------------------- analysis

def category(series, sinfo):
    if series.startswith("KXMVE"):
        return "Parlays"
    return (sinfo.get(series) or {}).get("category") or "Unknown"


def clustered(rows, key):
    """Mean of value per contract and its cluster-robust SE. rows: (value_total, contracts, cluster)."""
    n = sum(r[1] for r in rows)
    if n <= 0:
        return float("nan"), float("nan"), 0
    mean = sum(r[0] for r in rows) / n
    g = defaultdict(float)
    for v, c, k in rows:
        g[k] += v - mean * c
    G = len(g)
    var = sum(x * x for x in g.values()) / (n * n) * (G / (G - 1) if G > 1 else 1)
    return mean, math.sqrt(var), G


def stat(trades, value, weight=None):
    """Per-contract mean of value(t) with the larger of the two clustered SEs."""
    by_event = [(value(t) * t["n"], t["n"], t["event"]) for t in trades]
    by_day = [(value(t) * t["n"], t["n"], t["slate"]) for t in trades]
    m, se1, g1 = clustered(by_event, "event")
    _, se2, g2 = clustered(by_day, "slate")
    return {"mean": m, "se": max(se1, se2), "events": g1, "slates": g2,
            "contracts": sum(t["n"] for t in trades), "trades": len(trades)}


def per_dollar(trades):
    """Net return per $ risked, clustered by event, as a ratio estimator (linearised SE)."""
    risk = sum(t["p"] * t["n"] for t in trades)
    if risk <= 0:
        return float("nan"), float("nan")
    r = sum(t["net"] * t["n"] for t in trades) / risk
    worst = 0.0
    for key in ("event", "slate"):
        g = defaultdict(float)
        for t in trades:
            g[t[key]] += (t["net"] - r * t["p"]) * t["n"]
        G = len(g)
        var = sum(x * x for x in g.values()) / (risk * risk) * (G / (G - 1) if G > 1 else 1)
        worst = max(worst, math.sqrt(var))
    return r, worst


def load():
    markets = json.load(gzip.open(DATA / "markets.json.gz", "rt"))
    sinfo = json.load(gzip.open(DATA / "series.json.gz", "rt"))
    drops = defaultdict(int)
    trades = []
    for p in sorted((DATA / "trades").glob("*.json.gz")):
        for ticker, ts, side, yes_px, n in json.load(gzip.open(p, "rt")):
            m = markets.get(ticker) or {}
            if m.get("result") not in ("yes", "no"):
                drops["unsettled" if m.get("status") in ("active", "open", "initialized", "closed", "determined")
                      else f"result {m.get('result')!r}"] += 1
                continue
            px = yes_px if side == "yes" else 1.0 - yes_px
            if not 0.0 < px < 1.0 or n <= 0:
                drops["price at 0 or 1"] += 1
                continue
            series = (m["event"] or ticker).split("-")[0]
            s = sinfo.get(series) or {}
            mult = float(s.get("fee_multiplier") or 1) if s.get("fee_type", "quadratic") == "quadratic" else 0.0
            won = 1.0 if m["result"] == side else 0.0
            fee = 0.07 * mult * px * (1 - px)
            close = _epoch(m["close"]) if m.get("close") else None
            trades.append({"p": px, "n": n, "won": won, "gross": won - px, "net": won - px - fee, "ts": ts,
                           "event": m["event"] or ticker, "series": series,
                           "slate": f"{series}|{m['close'][:10] if m.get('close') else '?'}",
                           "cat": category(series, sinfo), "to_close": (close - ts) if close else None})
    return trades, drops


def analyze():
    trades, drops = load()
    kept = len(trades)
    print(f"trades kept {kept:,}  contracts {sum(t['n'] for t in trades):,.0f}  events {len({t['event'] for t in trades}):,}")
    print("dropped:", dict(sorted(drops.items(), key=lambda kv: -kv[1])))
    print(f"span {datetime.fromtimestamp(min(t['ts'] for t in trades), timezone.utc):%Y-%m-%d} .. "
          f"{datetime.fromtimestamp(max(t['ts'] for t in trades), timezone.utc):%Y-%m-%d}\n")

    def bucket_table(rows, title):
        print(f"=== {title}")
        print(f"  {'price bought':<13}{'contracts':>13}{'events':>8}{'avg price':>10}{'won':>8}"
              f"{'taker gross':>20}{'taker net':>20}{'net per $':>18}")
        for lo, hi in zip(BUCKETS, BUCKETS[1:]):
            b = [t for t in rows if lo <= t["p"] < hi]
            if not b:
                continue
            c = sum(t["n"] for t in b)
            avg = sum(t["p"] * t["n"] for t in b) / c
            win = sum(t["won"] * t["n"] for t in b) / c
            g = stat(b, lambda t: t["gross"])
            n_ = stat(b, lambda t: t["net"])
            r, rse = per_dollar(b)
            print(f"  {lo:.2f}-{min(hi, 1):.2f}   {c:>13,.0f}{g['events']:>8,}{avg:>10.3f}{win:>8.3f}"
                  f"{100 * g['mean']:>+11.2f}c +/-{100 * g['se']:.2f}{100 * n_['mean']:>+11.2f}c +/-{100 * n_['se']:.2f}"
                  f"{100 * r:>+10.1f}% +/-{100 * rse:.1f}")
        print()

    bucket_table(trades, "ALL CATEGORIES (taker = the side that crossed the spread)")

    tests = []

    def test(label, mean, se):
        t = mean / se if se else float("nan")
        tests.append((label, mean, se, t))

    long_, fav = [t for t in trades if t["p"] < 0.10], [t for t in trades if t["p"] >= 0.90]
    a, b = stat(long_, lambda t: t["gross"]), stat(fav, lambda t: t["gross"])
    test("L1 longshot minus favourite, taker gross c/contract", 100 * (a["mean"] - b["mean"]), 100 * math.hypot(a["se"], b["se"]))
    r, se = per_dollar(fav)
    test("L2 buy favourites >= 90c, net % per $ risked", 100 * r, 100 * se)
    m3 = stat(long_, lambda t: -t["gross"])
    test("L3 maker selling longshots < 10c, gross c/contract", 100 * m3["mean"], 100 * m3["se"])
    r, se = per_dollar([t for t in fav if t["to_close"] is not None and t["to_close"] > 3600])
    test("L4 L2 excluding the final hour before close", 100 * r, 100 * se)
    r1, s1 = per_dollar([t for t in fav if t["ts"] < SPLIT])
    r2, s2 = per_dollar([t for t in fav if t["ts"] >= SPLIT])
    test(f"L5 L2 first half {100 * r1:+.1f}% vs second half {100 * r2:+.1f}%: difference", 100 * (r2 - r1), 100 * math.hypot(s1, s2))
    r, se = per_dollar([t for t in fav if t["cat"] != "Parlays"])
    test("L6 L2 excluding parlays", 100 * r, 100 * se)

    print("=== PRE-REGISTERED TESTS (Bonferroni over 6: |t| > 2.64)")
    for label, mean, se, t in tests:
        flag = "CLEARS" if abs(t) > 2.64 else ""
        print(f"  {label:<62} {mean:+8.2f} +/- {se:5.2f}   t={t:+6.2f}  {flag}")
    print()

    print("=== DESCRIPTIVE: favourites >= 90c and longshots < 10c by category (net per $ risked, taker)")
    cats = defaultdict(list)
    for t in trades:
        cats[t["cat"]].append(t)
    print(f"  {'category':<24}{'contracts':>14}{'events':>9}{'fav >=90c net/$':>22}{'longshot <10c net/$':>24}")
    for cat, rows in sorted(cats.items(), key=lambda kv: -sum(t["n"] for t in kv[1])):
        f_ = [t for t in rows if t["p"] >= 0.90]
        l_ = [t for t in rows if t["p"] < 0.10]
        fr, fs = per_dollar(f_) if f_ else (float("nan"), float("nan"))
        lr, ls = per_dollar(l_) if l_ else (float("nan"), float("nan"))
        print(f"  {cat:<24}{sum(t['n'] for t in rows):>14,.0f}{len({t['event'] for t in rows}):>9,}"
              f"{100 * fr:>+14.1f}% +/-{100 * fs:4.1f}{100 * lr:>+16.1f}% +/-{100 * ls:4.1f}")
    print()

    print("=== DESCRIPTIVE: favourites >= 90c by time left before close (net per $ risked, taker)")
    for label, lo, hi in (("< 5 min", 0, 300), ("5-60 min", 300, 3600), ("1-24 h", 3600, 86400), ("1-7 days", 86400, 7 * 86400),
                          ("> 7 days", 7 * 86400, float("inf"))):
        b = [t for t in fav if t["to_close"] is not None and lo <= t["to_close"] < hi]
        if b:
            r, se = per_dollar(b)
            print(f"  {label:<10} {sum(t['n'] for t in b):>14,.0f} contracts  {100 * r:+6.1f}% +/- {100 * se:.1f}")
    print()
    print("Reading: 'won' vs 'avg price' is calibration. Gross is before fees; net after Kalshi's taker fee.")
    print("Maker gross is minus taker gross (no maker fee assumed).")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "fetch":
        fetch()
    else:
        analyze()
