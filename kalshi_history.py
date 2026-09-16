"""Kalshi KXBTC15M history: every settled 15-minute market and every trade. Stdlib only.

    python kalshi_history.py 2026-08-13 2026-09-11

Facts checked against the API on 2026-09-13 before this was written:
  * `floor_strike` is the level to beat and `expiration_value` the settlement
    average; expiration_value >= floor_strike <-> result "yes" on 40/40.
  * Trades carry `created_time` to the microsecond. Kalshi is a central
    exchange, so this is the match time: no on-chain stamping lag to correct.
  * taker_side is the side the taker BOUGHT: across 165,982 trades it always
    equals taker_outcome_side, and yes_price + no_price == 1 on every trade.
  * Contracts are fractional (count_fp 0.02 exists).

One gzipped JSON per market in data/kalshi/, resumable. Trades are stored as
[epoch_seconds_float, taker_side, yes_price, count].
"""

import gzip
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from history import _get

API = "https://api.elections.kalshi.com/trade-api/v2"
OUT = Path(__file__).with_name("data") / "kalshi"


def _epoch(iso):
    """'2026-09-13T17:59:59.635654Z' -> float epoch seconds."""
    base, _, frac = iso.rstrip("Z").partition(".")
    t = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    return t + (float("0." + frac) if frac else 0.0)


def list_markets(d0, d1):
    out, cursor = [], ""
    while True:
        page = json.loads(_get(f"{API}/markets?series_ticker=KXBTC15M&status=settled&limit=1000"
                               + (f"&cursor={cursor}" if cursor else "")))
        for m in page["markets"]:
            o, c = _epoch(m["open_time"]), _epoch(m["close_time"])
            if d0 <= o and c <= d1:
                out.append({"ticker": m["ticker"], "open": int(o), "close": int(c), "result": m["result"],
                            # 4 of 4,000 settled markets omit the strike (same rule, same result)
                            "floor_strike": m.get("floor_strike"), "expiration_value": m.get("expiration_value")})
        cursor = page.get("cursor")
        oldest = min(_epoch(m["open_time"]) for m in page["markets"]) if page["markets"] else 0
        if not cursor or oldest < d0:
            return out


def fetch_trades(m):
    path = OUT / f"{m['ticker']}.json.gz"
    if path.exists():
        return "cached"
    trades, cursor, pages = [], "", 0
    while True:
        page = json.loads(_get(f"{API}/markets/trades?ticker={m['ticker']}&limit=1000"
                               + (f"&cursor={cursor}" if cursor else "")))
        trades += [[_epoch(t["created_time"]), t["taker_side"], float(t["yes_price_dollars"]),
                    float(t["count_fp"])] for t in page["trades"]]
        pages += 1
        cursor = page.get("cursor")
        if not cursor or not page["trades"]:
            break
    tmp = path.with_suffix(".tmp")  # write-then-rename: a killed run never leaves a "cached" half file
    with gzip.open(tmp, "wt") as f:
        json.dump({**m, "trades": trades}, f)
    tmp.replace(path)
    return f"{len(trades)} trades / {pages} pages"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    d0 = datetime.fromisoformat(sys.argv[1]).replace(tzinfo=timezone.utc).timestamp()
    d1 = datetime.fromisoformat(sys.argv[2]).replace(tzinfo=timezone.utc).timestamp() + 86400
    index = OUT / "markets.json"
    markets = json.loads(index.read_text()) if index.exists() else list_markets(d0, d1)
    index.write_text(json.dumps(markets))
    print(f"{len(markets)} settled markets in range", flush=True)
    # 4 workers is ~8 reads/s. 8 workers (~15/s) plus the paper bot's book polling
    # from the same IP drew 429s on 2026-09-13; leave the bot the headroom.
    with ThreadPoolExecutor(4) as ex:
        for i, msg in enumerate(ex.map(fetch_trades, markets), 1):
            if i % 50 == 0 or i == len(markets):
                print(f"{i}/{len(markets)} {msg}", flush=True)


if __name__ == "__main__":
    main()
