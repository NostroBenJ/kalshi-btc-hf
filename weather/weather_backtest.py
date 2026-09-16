"""Kalshi daily high-temperature markets vs free weather-model forecasts.

    python weather_backtest.py fetch     # a few minutes: Kalshi markets + hourly quotes, Open-Meteo forecasts
    python weather_backtest.py           # the verdict

PRE-REGISTERED 2026-09-15, before any forecast or quote below was downloaded.

THE IDEA
--------
Kalshi lists "highest temperature today" markets per city in 2-degree brackets, settled on the
National Weather Service climate report for one station (rules: "maximum temperature recorded at
New York City (CLINYC)"). Professional weather models forecast that number well and are free. If the
crowd prices brackets looser than the models justify, buying the brackets the model says are cheap
makes money after Kalshi's fee. This needs knowing, not speed.

CITIES (the seven series with history back to at least early 2025)
------------------------------------------------------------------
KXHIGHNY Central Park, KXHIGHCHI Chicago Midway, KXHIGHMIA Miami Intl, KXHIGHAUS Austin-Bergstrom,
KXHIGHDEN Denver Intl, KXHIGHLAX LAX, KXHIGHPHIL Philadelphia Intl. Market dates 2025-03-01 ..
2026-09-13. Each market's `expiration_value` is the settled high; `result` is its outcome.

THE FORECAST, WITH NO LOOKAHEAD
-------------------------------
Open-Meteo's Previous Runs API keeps, for every hour, what the model forecast N days EARLIER
(temperature_2m_previous_dayN), so a backtest sees only what existed at the time. Model: gfs_seamless
(GFS + HRRR over the US). Forecast daily high = max of the hourly forecasts over the climate day.
ASSUMPTION (NWS convention): the climate day runs midnight-to-midnight LOCAL STANDARD TIME all year,
so it is built on a fixed UTC offset, not local clock time.

Two decision times, both leaving the forecast hours old when used:
  D0  "morning of"  13:00 UTC on the market day (9 am ET .. 6 am PT), forecast previous_day1
  D1  "day before"  16:00 UTC the day before (noon ET),               forecast previous_day2
The newest run either can use was initialised >= 6 h before the decision, past GFS's ~4 h delivery.

THE PROBABILITY
---------------
Residual r = settled high - forecast high, per city and decision time. At each decision, mean and
standard deviation of that city's residuals from events settled at least 2 days earlier (trailing 60,
at least 30 required, sd floored at 1.0 F). High ~ Normal(forecast + mean, sd), integer-rounded:
P(high = k) = Phi((k + .5 - mu)/sd) - Phi((k - .5 - mu)/sd). Brackets: "between" floor..cap
inclusive; "greater" > floor; "less" < cap (checked against every settled result first).

THE TRADE
---------
Price = the YES ask (and YES bid, for buying NO at 1 - bid) from Kalshi's hourly candle ending at the
decision time (the last one within 3 h). Buy YES if p - ask - fee >= EDGE, else buy NO if
(1 - p) - (1 - bid) - fee >= EDGE; one contract per market; held to settlement.
fee = 0.07 * price * (1 - price). EDGE = 0.05 primary (0.10 shown).
ASSUMPTION (execution): one contract fills at the quoted ask. Kalshi's per-order fee rounding is not
modelled (it adds up to ~1c on a single contract; shown as a sensitivity).

HYPOTHESES   (SEs clustered by EVENT = city-day, and by DATE across cities; the larger is used)
  W1  PRIMARY. D0 strategy, net P&L per contract > 0 with t >= 2.
  W2  D1 strategy, same.
  W3  CONTROL, must LOSE: the same D0 strategy fed a forecast from a random other day of the same
      city (seed 3). If this makes money, the "edge" is the bracket structure, not the forecast.
  W4  CONTROL, must WIN BIG: fed the settled high itself (sd 0.5). If this does not make a lot, the
      bracket mapping or the price reading is broken and nothing else can be trusted.
  W5  Brier score, model vs Kalshi mid, all markets at D0. Descriptive, but it says who knows more.
  W6  W1 in the first vs second half of dates: both must be positive.
PASS = W1 (t >= 2) AND W6 AND W3 loses AND W4 wins.
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
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).parent
DATA = HERE / "data"
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
METEO = "https://previous-runs-api.open-meteo.com/v1/forecast"
FIRST_DAY, LAST_DAY = "2025-03-01", "2026-09-13"
CITIES = {  # series: (station, lat, lon, standard-time UTC offset in hours)
    "KXHIGHNY": ("Central Park", 40.7789, -73.9692, -5),
    "KXHIGHCHI": ("Chicago Midway", 41.7868, -87.7522, -6),
    "KXHIGHMIA": ("Miami Intl", 25.7959, -80.2870, -5),
    "KXHIGHAUS": ("Austin-Bergstrom", 30.1945, -97.6699, -6),
    "KXHIGHDEN": ("Denver Intl", 39.8466, -104.6562, -7),
    "KXHIGHLAX": ("Los Angeles Intl", 33.9382, -118.3866, -8),
    "KXHIGHPHIL": ("Philadelphia Intl", 39.8733, -75.2268, -5),
}
MODEL = "gfs_seamless"
DECISIONS = {"D0": (0, 13, "temperature_2m_previous_day1"), "D1": (-1, 16, "temperature_2m_previous_day2")}
EDGE = 0.05
TRAIL, MIN_RESID, SD_FLOOR = 60, 30, 1.0

_lock, _slot = threading.Lock(), [0.0]


def get(url, tries=6, pace=0.125):
    for attempt in range(tries):
        with _lock:
            wait = _slot[0] - time.time()
            _slot[0] = max(time.time(), _slot[0]) + pace
        if wait > 0:
            time.sleep(wait)
        req = urllib.request.Request(url, headers={"User-Agent": "weather-research", "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429,) or e.code >= 500:
                time.sleep(min(60, 2 ** attempt))
                continue
            raise RuntimeError(f"HTTP {e.code} on {url[:160]}: {e.read()[:200]!r}")
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            time.sleep(min(60, 2 ** attempt))
    raise RuntimeError(f"gave up on {url[:160]}")


def epoch(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def _num(v):
    """expiration_value is usually '75.00', but some older markets carry 'No' / 'Yes' instead."""
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def event_date(event_ticker):
    """KXHIGHNY-26SEP14 -> '2026-09-14'."""
    return datetime.strptime(event_ticker.split("-")[1], "%y%b%d").strftime("%Y-%m-%d")


# ------------------------------------------------------------------------------------------ fetch

def list_markets(series):
    lo, hi = epoch(FIRST_DAY + "T00:00:00Z"), epoch(LAST_DAY + "T23:59:59Z") + 2 * 86400
    out, cur = {}, ""
    while True:
        d = get(f"{KALSHI}/markets?series_ticker={series}&status=settled&min_close_ts={int(lo)}&max_close_ts={int(hi)}"
                f"&limit=1000" + (f"&cursor={cur}" if cur else ""))
        for m in d.get("markets") or []:
            out[m["ticker"]] = m
        cur = d.get("cursor")
        if not cur or not d.get("markets"):
            break
    cur = ""
    while True:
        d = get(f"{KALSHI}/historical/markets?series_ticker={series}&limit=1000" + (f"&cursor={cur}" if cur else ""))
        ms = d.get("markets") or []
        for m in ms:
            if lo <= epoch(m["close_time"]) <= hi:
                out[m["ticker"]] = m
        cur = d.get("cursor")
        if not cur or not ms or min(epoch(m["close_time"]) for m in ms) < lo:
            break
    keep = []
    for m in out.values():
        try:
            day = event_date(m["event_ticker"])
        except ValueError:
            continue
        if FIRST_DAY <= day <= LAST_DAY and m.get("result") in ("yes", "no"):
            keep.append({"ticker": m["ticker"], "event": m["event_ticker"], "day": day, "series": series,
                         "type": m.get("strike_type"), "floor": m.get("floor_strike"), "cap": m.get("cap_strike"),
                         "result": m["result"], "settled": _num(m.get("expiration_value")),
                         "open": m["open_time"], "close": m["close_time"]})
    return series, keep


def fetch_forecasts(series):
    _, lat, lon, off = CITIES[series]
    rows = {}
    start = datetime.strptime(FIRST_DAY, "%Y-%m-%d") - timedelta(days=2)
    end = datetime.strptime(LAST_DAY, "%Y-%m-%d") + timedelta(days=1)
    while start <= end:
        stop = min(start + timedelta(days=89), end)
        d = get(f"{METEO}?latitude={lat}&longitude={lon}&hourly=temperature_2m_previous_day1,temperature_2m_previous_day2"
                f"&temperature_unit=fahrenheit&timezone=GMT&models={MODEL}"
                f"&start_date={start:%Y-%m-%d}&end_date={stop:%Y-%m-%d}", pace=0.5)
        h = d["hourly"]
        for i, t in enumerate(h["time"]):
            rows[t] = [h["temperature_2m_previous_day1"][i], h["temperature_2m_previous_day2"][i]]
        start = stop + timedelta(days=1)
    return series, rows


def fetch_candles(day, markets):
    """Hourly quotes for every market of one market-day, from 15:00 UTC the day before to 14:00 UTC on it."""
    d0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    lo, hi = int((d0 - timedelta(hours=9)).timestamp()), int((d0 + timedelta(hours=14)).timestamp())
    out = {}
    tickers = [m["ticker"] for m in markets]
    for i in range(0, len(tickers), 100):
        d = get(f"{KALSHI}/markets/candlesticks?market_tickers={','.join(tickers[i:i + 100])}"
                f"&start_ts={lo}&end_ts={hi}&period_interval=60")
        for mk in d.get("markets") or []:
            out[mk["market_ticker"]] = [[c["end_period_ts"], c["yes_bid"]["close_dollars"], c["yes_ask"]["close_dollars"]]
                                        for c in mk.get("candlesticks") or []]
    return day, out


def fetch():
    DATA.mkdir(exist_ok=True)
    print("listing Kalshi markets ...", flush=True)
    with ThreadPoolExecutor(4) as ex:
        markets = dict(ex.map(list_markets, CITIES))
    n = sum(len(v) for v in markets.values())
    print("  " + ", ".join(f"{s} {len(v):,}" for s, v in markets.items()) + f"  (total {n:,})", flush=True)
    json.dump(markets, gzip.open(DATA / "markets.json.gz", "wt"))

    print("Open-Meteo previous-run forecasts ...", flush=True)
    with ThreadPoolExecutor(2) as ex:
        fc = dict(ex.map(fetch_forecasts, CITIES))
    json.dump(fc, gzip.open(DATA / "forecasts.json.gz", "wt"))

    by_day = defaultdict(list)
    for ms in markets.values():
        for m in ms:
            by_day[m["day"]].append(m)
    print(f"Kalshi hourly quotes for {len(by_day):,} market days ...", flush=True)
    candles = {}
    with ThreadPoolExecutor(6) as ex:
        for i, (day, got) in enumerate(ex.map(lambda kv: fetch_candles(*kv), sorted(by_day.items())), 1):
            candles.update(got)
            if i % 100 == 0:
                print(f"  {i}/{len(by_day)} days", flush=True)
    json.dump(candles, gzip.open(DATA / "candles.json.gz", "wt"))
    print("fetch done", flush=True)


# ---------------------------------------------------------------------------------------- analysis

def phi(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def p_bracket(m, mu, sd):
    """P(YES) for one bracket, the high being an integer drawn from Normal(mu, sd) rounded."""
    lo = -math.inf if m["type"] == "less" else (m["floor"] + (1 if m["type"] == "greater" else 0))
    hi = math.inf if m["type"] == "greater" else (m["cap"] - (1 if m["type"] == "less" else 0))
    a = phi((lo - 0.5 - mu) / sd) if lo != -math.inf else 0.0
    b = phi((hi + 0.5 - mu) / sd) if hi != math.inf else 1.0
    return min(1.0, max(0.0, b - a))


def resolves_yes(m, high):
    if m["type"] == "between":
        return m["floor"] <= high <= m["cap"]
    if m["type"] == "greater":
        return high > m["floor"]
    return high < m["cap"]


def forecast_high(fc, series, day, col):
    """Max hourly forecast over the climate day (local standard time)."""
    off = CITIES[series][3]
    d = datetime.strptime(day, "%Y-%m-%d") - timedelta(hours=off)   # 00:00 LST in UTC
    rows = fc.get(series) or {}
    vals = []
    for h in range(24):
        v = rows.get((d + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M"))
        if v is None or v[col] is None:
            return None
        vals.append(v[col])
    return max(vals)


def quote(candles, ticker, ts):
    rows = candles.get(ticker) or []
    best = None
    for end, bid, ask in rows:
        if ts - 3 * 3600 <= end <= ts:
            best = (end, bid, ask)
    if not best or best[1] is None or best[2] is None:
        return None
    bid, ask = float(best[1]), float(best[2])
    if not (0 < ask < 1) or not (0 <= bid < 1) or bid > ask:
        return None
    return bid, ask


def clustered(rows):
    """rows: (value, cluster_a, cluster_b). Mean per contract and the larger of the two clustered SEs."""
    n = len(rows)
    if n < 2:
        return float("nan"), float("nan"), 0
    mean = sum(r[0] for r in rows) / n
    worst = 0.0
    for k in (1, 2):
        g = defaultdict(float)
        for r in rows:
            g[r[k]] += r[0] - mean
        G = len(g)
        worst = max(worst, math.sqrt(sum(x * x for x in g.values()) / (n * n) * G / max(G - 1, 1)))
    return mean, worst, n


def analyze():
    markets = json.load(gzip.open(DATA / "markets.json.gz", "rt"))
    fc = json.load(gzip.open(DATA / "forecasts.json.gz", "rt"))
    candles = json.load(gzip.open(DATA / "candles.json.gz", "rt"))

    # 0. the bracket semantics must reproduce every settled result before anything else is trusted
    checked = wrong = 0
    for ms in markets.values():
        for m in ms:
            if m["settled"] is None:
                continue
            checked += 1
            wrong += resolves_yes(m, round(m["settled"])) != (m["result"] == "yes")
    print(f"bracket rules vs settled results: {checked - wrong:,}/{checked:,} agree")
    if wrong > checked * 0.002:
        print("  bracket mapping is wrong; stopping")
        return

    events = defaultdict(list)
    for ms in markets.values():
        for m in ms:
            events[(m["series"], m["day"])].append(m)
    actual = {k: v[0]["settled"] for k, v in events.items() if v[0]["settled"] is not None}

    rng = random.Random(3)
    days_by_city = defaultdict(list)
    for (s, day) in sorted(actual):
        days_by_city[s].append(day)

    def run(name, col_key, mode="model"):
        shift, hour, col = DECISIONS[col_key]
        ci = 0 if col.endswith("day1") else 1
        resid = defaultdict(list)   # series -> [(day, residual)] in date order
        trades, briers = [], []
        skipped = defaultdict(int)
        for (s, day) in sorted(events, key=lambda k: k[1]):
            if (s, day) not in actual:
                skipped["no settled value"] += 1
                continue
            f = forecast_high(fc, s, day, ci)
            src_day = day
            if mode == "shuffled":
                src_day = rng.choice(days_by_city[s])
                f = forecast_high(fc, s, src_day, ci)
            if f is None:
                skipped["no forecast"] += 1
                continue
            decide = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=shift, hours=hour)
            usable = [r for d_, r in resid[s] if d_ <= (decide - timedelta(days=2)).strftime("%Y-%m-%d")][-TRAIL:]
            real_f = forecast_high(fc, s, day, ci)
            if real_f is not None:
                resid[s].append((day, actual[(s, day)] - real_f))
            if mode == "perfect":
                mu, sd = actual[(s, day)], 0.5
            else:
                if len(usable) < MIN_RESID:
                    skipped["residual history < 30"] += 1
                    continue
                mean = sum(usable) / len(usable)
                sd = max(SD_FLOOR, math.sqrt(sum((x - mean) ** 2 for x in usable) / (len(usable) - 1)))
                mu = f + mean
            for m in events[(s, day)]:
                q = quote(candles, m["ticker"], decide.timestamp())
                if q is None:
                    skipped["no quote at decision"] += 1
                    continue
                bid, ask = q
                p = p_bracket(m, mu, sd)
                won_yes = 1.0 if m["result"] == "yes" else 0.0
                briers.append(((p - won_yes) ** 2, ((bid + ask) / 2 - won_yes) ** 2))
                ey = p - ask - 0.07 * ask * (1 - ask)
                no_px = 1 - bid
                en = (1 - p) - no_px - 0.07 * no_px * (1 - no_px)
                for edge_min in (EDGE, 0.10):
                    if ey >= edge_min:
                        pnl = won_yes - ask - 0.07 * ask * (1 - ask)
                        trades.append({"edge_min": edge_min, "pnl": pnl, "event": f"{s}|{day}", "date": day, "side": "yes", "px": ask})
                    elif en >= edge_min and no_px > 0:
                        pnl = (1 - won_yes) - no_px - 0.07 * no_px * (1 - no_px)
                        trades.append({"edge_min": edge_min, "pnl": pnl, "event": f"{s}|{day}", "date": day, "side": "no", "px": no_px})
        return trades, briers, skipped

    def show(label, trades, edge_min=EDGE):
        rows = [(t["pnl"], t["event"], t["date"]) for t in trades if t["edge_min"] == edge_min]
        m, se, n = clustered(rows)
        t_ = m / se if se else float("nan")
        wins = sum(1 for r in rows if r[0] > 0) / n if n else float("nan")
        print(f"  {label:<46} n={n:>6,}  {100 * m:+6.2f}c +/-{100 * se:4.2f}  t={t_:+6.2f}  win {wins:5.1%}")
        return m, se, n

    print()
    results = {}
    for key, label in (("D0", "W1 PRIMARY  morning of (previous_day1)"), ("D1", "W2 day before (previous_day2)")):
        tr, br, sk = run(label, key)
        print(f"{label}   skipped: {dict(sk)}")
        results[key] = show(f"edge >= {EDGE:.2f}", tr)
        show("edge >= 0.10", tr, 0.10)
        show("  - with 1c extra per contract (fee rounding)", [dict(t, pnl=t["pnl"] - 0.01) for t in tr])
        if key == "D0":
            d0_trades, d0_brier = tr, br
    dates = sorted({t["date"] for t in d0_trades})
    mid = dates[len(dates) // 2]
    print("W6 halves of the primary")
    h1 = show(f"first half (< {mid})", [t for t in d0_trades if t["date"] < mid])
    h2 = show(f"second half (>= {mid})", [t for t in d0_trades if t["date"] >= mid])
    print("W3 CONTROL: forecast from a random other day (must lose)")
    w3 = show("shuffled forecast", run("W3", "D0", "shuffled")[0])
    print("W4 CONTROL: the settled high itself (must win big)")
    w4 = show("perfect forecast", run("W4", "D0", "perfect")[0])
    bm = sum(b[0] for b in d0_brier) / len(d0_brier)
    bk = sum(b[1] for b in d0_brier) / len(d0_brier)
    print(f"\nW5 Brier at D0 over {len(d0_brier):,} markets: model {bm:.4f}  vs  Kalshi mid {bk:.4f}  "
          f"({'model sharper' if bm < bk else 'market sharper'})")

    print("\nBy city, primary (edge >= 0.05)")
    for s in CITIES:
        show(s, [t for t in d0_trades if t["event"].startswith(s + "|")])
    print("By side, primary")
    show("buy YES (the bracket)", [t for t in d0_trades if t["side"] == "yes"])
    show("buy NO (against the bracket)", [t for t in d0_trades if t["side"] == "no"])

    w1m, w1se, _ = results["D0"]
    verdict = [w1se and w1m / w1se >= 2.0, h1[0] > 0 and h2[0] > 0, w3[0] < 0, w4[0] > 0.10]
    print(f"\nVERDICT: {'PASS' if all(verdict) else 'FAIL'}   (W1 t>=2, W6 halves, W3 loses, W4 wins big) = {verdict}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "fetch":
        fetch()
    else:
        analyze()
