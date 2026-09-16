"""Historical inputs for the backtest. Stdlib only.

    python history.py binance  2026-08-13 2026-09-11   # BTCUSDT perp aggTrades -> 1s as-of prices
    python history.py polymarket 2026-08-13 2026-09-11 # every 5m market: outcome, fee schedule, taker fills

Binance: daily aggTrades zips from data.binance.vision, each verified against
its published SHA-256 before use, reduced to one float per second:

    price[s] = last trade strictly BEFORE second s

so reading price[s] never sees a trade from second s itself. Stored as a raw
array of doubles (86,400 per day) next to the zip.

Polymarket: one gzipped JSON per market in data/pm/, resumable. Trades are
paged until they reach 5 minutes before the open; a market whose paging ends
on a full page (the API refused more) is flagged `truncated` rather than
silently kept short.
"""

import array
import csv
import gzip
import hashlib
import io
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

DATA = Path(__file__).with_name("data")
BIN_DIR = DATA / "binance"
PM_DIR = DATA / "pm"
ARCHIVE = "https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT"


def _get(url, timeout=60, tries=6):
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pm-hf-history"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            time.sleep(2 ** attempt)
        except OSError:
            time.sleep(2 ** attempt)
    raise OSError(f"gave up on {url}")


def _days(a, b):
    d0, d1 = date.fromisoformat(a), date.fromisoformat(b)
    return [d0 + timedelta(n) for n in range((d1 - d0).days + 1)]


# ------------------------------------------------------------------ Binance

def binance_day(day):
    name = f"BTCUSDT-aggTrades-{day.isoformat()}.zip"
    zpath, apath = BIN_DIR / name, BIN_DIR / f"{day.isoformat()}.f8"
    if apath.exists():
        return f"{day} cached"
    if not zpath.exists():
        blob = _get(f"{ARCHIVE}/{name}")
        want = _get(f"{ARCHIVE}/{name}.CHECKSUM").decode().split()[0]
        got = hashlib.sha256(blob).hexdigest()
        if got != want:
            raise ValueError(f"{name}: sha256 {got} != published {want}")
        zpath.write_bytes(blob)
    day0 = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
    prices = array.array("d", [float("nan")] * 86400)
    last_px, filled_to, n = float("nan"), 0, 0
    with zipfile.ZipFile(zpath) as z, z.open(z.namelist()[0]) as f:
        for row in csv.reader(io.TextIOWrapper(f)):
            if not row[0].isdigit():
                continue  # header row, present in newer files
            ts = int(row[5])
            ts = ts // 1000 if ts > 10**14 else ts  # microseconds in some archives
            sec = ts // 1000 - day0
            # every second up to and including `sec` has only seen trades before it
            while filled_to <= min(sec, 86399):
                prices[filled_to] = last_px
                filled_to += 1
            last_px = float(row[1])
            n += 1
    while filled_to < 86400:
        prices[filled_to] = last_px
        filled_to += 1
    with open(apath, "wb") as out:
        prices.tofile(out)
    return f"{day} {n:,} trades"


SLOT_MS = 50


def binance_grid(day):
    """Price as of the start of every 50 ms slot of the day: last trade strictly
    before the slot opens. 1,728,000 doubles per day, next to the 1s file.
    Same no-lookahead contract as the 1s grid, twenty times finer."""
    zpath = BIN_DIR / f"BTCUSDT-aggTrades-{day.isoformat()}.zip"
    gpath = BIN_DIR / f"{day.isoformat()}.g50"
    if gpath.exists():
        return f"{day} grid cached"
    n_slots = 86400 * 1000 // SLOT_MS
    day0_ms = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()) * 1000
    grid = array.array("d", [float("nan")] * n_slots)
    last_px, filled_to = float("nan"), 0
    with zipfile.ZipFile(zpath) as z, z.open(z.namelist()[0]) as f:
        for row in csv.reader(io.TextIOWrapper(f)):
            if not row[0].isdigit():
                continue
            ts = int(row[5])
            ts = ts // 1000 if ts > 10**14 else ts
            slot = (ts - day0_ms) // SLOT_MS
            while filled_to <= min(slot, n_slots - 1):
                grid[filled_to] = last_px
                filled_to += 1
            last_px = float(row[1])
    while filled_to < n_slots:
        grid[filled_to] = last_px
        filled_to += 1
    with open(gpath, "wb") as out:
        grid.tofile(out)
    return f"{day} grid built"


def load_prices(day_list):
    """{epoch_second: price-as-of-start-of-second} over the given days."""
    out = {}
    for day in day_list:
        day0 = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
        a = array.array("d")
        with open(BIN_DIR / f"{day.isoformat()}.f8", "rb") as f:
            a.fromfile(f, 86400)
        out.update((day0 + i, p) for i, p in enumerate(a) if p == p)
    return out


# ------------------------------------------------------------------ Polymarket

def pm_market(start):
    slug = f"btc-updown-5m-{start}"
    path = PM_DIR / f"{slug}.json.gz"
    if path.exists():
        return "cached"
    ev = json.loads(_get(f"https://gamma-api.polymarket.com/events?slug={slug}"))
    if not ev:
        return "missing"
    m = ev[0]["markets"][0]
    rec = {
        "slug": slug, "start": start, "end": start + 300,
        "condition_id": m["conditionId"],
        "tokens": dict(zip(json.loads(m["outcomes"]), json.loads(m["clobTokenIds"]))),
        "outcome_prices": dict(zip(json.loads(m["outcomes"]), json.loads(m["outcomePrices"]))),
        "closed": m.get("closed"), "fees_enabled": m.get("feesEnabled"),
        "fee_schedule": m.get("feeSchedule"), "description": m.get("description"),
    }
    trades, offset, truncated = [], 0, False
    while True:
        try:
            page = json.loads(_get(f"https://data-api.polymarket.com/trades?market={m['conditionId']}"
                                   f"&limit=500&offset={offset}&takerOnly=true"))
        except (OSError, urllib.error.HTTPError):
            truncated = True
            break
        trades += [[t["timestamp"], t["outcome"], t["side"], t["price"], t["size"], t["transactionHash"]]
                   for t in page]
        if len(page) < 500:
            break
        if min(t["timestamp"] for t in page) < start - 300:
            break
        offset += 500
    rec["trades"] = trades  # [timestamp, outcome, taker side, price, size, tx]
    rec["truncated"] = truncated
    with gzip.open(path, "wt") as f:
        json.dump(rec, f)
    return f"{len(trades)} fills" + (" TRUNCATED" if truncated else "")


def main():
    what, a, b = sys.argv[1], sys.argv[2], sys.argv[3]
    if what == "binance":
        BIN_DIR.mkdir(parents=True, exist_ok=True)
        with ThreadPoolExecutor(4) as ex:
            for msg in ex.map(binance_day, _days(a, b)):
                print(msg, flush=True)
    elif what == "binance-grid":  # CPU-bound CSV parsing: processes, not threads
        from multiprocessing import Pool
        with Pool(8) as pool:
            for msg in pool.imap_unordered(binance_grid, _days(a, b)):
                print(msg, flush=True)
    elif what == "polymarket":
        PM_DIR.mkdir(parents=True, exist_ok=True)
        d0 = int(datetime.fromisoformat(a).replace(tzinfo=timezone.utc).timestamp())
        d1 = int(datetime.fromisoformat(b).replace(tzinfo=timezone.utc).timestamp()) + 86400
        starts = list(range(d0, d1, 300))
        done = 0
        with ThreadPoolExecutor(8) as ex:
            for start, msg in zip(starts, ex.map(pm_market, starts)):
                done += 1
                if done % 100 == 0 or "TRUNC" in msg or msg == "missing":
                    print(f"{done}/{len(starts)} {start} {msg}", flush=True)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
