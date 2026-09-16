"""Did taking Polymarket 5-minute BTC mispricings make money? 30 days of real fills.

    python backtest.py            # verify, then the report
    python backtest.py --verify   # tests only

For every taker fill in every market (history.py polymarket), price the side
that was traded with fair_value.fair_up using Binance BTCUSDT perp prices as of
`fill timestamp - D` seconds, then settle it against the official outcome:

    taker BUY  of X at p:  pnl/share = won(X) - p - fee(p)
    taker SELL of X at p:  pnl/share = p - won(X) - fee(p)
    model edge at fill  :  the same with the model's fair value in place of won(X)

The strategy is "copy every fill whose model edge was at least theta".
These are fills that really happened, so every price was really there. What
this cannot see is whether we would have been first: that is the recorder's job.

THE TIMESTAMP TRAP. Polymarket's data-api stamps a fill when it lands on-chain,
1.0-4.1s after the match (median 2.5s; 3,522 fills matched by tx hash against
the live feed on 2026-09-13). D=0 therefore lets the model see up to 4s of the
future. It is run anyway, as the positive control: it SHOULD look profitable.
D=5 is the first honest row.

Errors: fills inside one market share one outcome, so standard errors are
clustered by market (verify [2] shows why naive ones are too tight).
"""

import argparse
import array
import gzip
import json
import math
import random
from datetime import date, datetime, timezone
from multiprocessing import Pool
from pathlib import Path

from fair_value import FEE_RATE, fair_up, realized_vol, settle_times, taker_fee

DATA = Path(__file__).with_name("data")
DELAYS = (0, 3, 5, 10)
THETAS = (None, 0.0, 0.02, 0.05, 0.10)


# ------------------------------------------------------------------ prices

class Prices:
    """price(s) = last Binance perp trade strictly before epoch second s."""

    def __init__(self):
        self.days = {}

    def __call__(self, s):
        d0 = s - s % 86400
        a = self.days.get(d0)
        if a is None:
            path = DATA / "binance" / f"{datetime.fromtimestamp(d0, timezone.utc).date().isoformat()}.f8"
            a = array.array("d")
            if path.exists():
                with open(path, "rb") as f:
                    a.fromfile(f, 86400)
            self.days[d0] = a
        if not a:
            return None
        p = a[s - d0]
        return p if p == p else None


def strike(price, start):
    """The level to beat: the 60-second average ending at the open.

    Measured, not read: the description says "TWAP of the range >= price at the
    beginning", but on 506 markets with |margin| > $15 only
    mean(last 60s) >= mean(60s before open) agreed with every official outcome
    (341/341); the full-window TWAP against the opening print got 96.9%.
    """
    px = [price(s) for s in range(start - 59, start + 1)]
    return None if any(p is None for p in px) else sum(px) / 60


def fair_path(price, start, avg, sigma, window=300):
    """P(Up) using information as of each whole second start+1 .. start+window-1.
    window=900 prices Kalshi's 15-minute markets, which settle on the same rule.

    Only price(s) for s <= t enters the value at t: known prints are the
    settlement seconds already passed, S is price(t). The strike uses seconds
    up to the open only.
    """
    times = settle_times(start, window, avg)
    K = strike(price, start)
    out = {}
    known = {}
    for t in range(start + 1, start + window):
        for s in times:
            if s <= t and s not in known:
                known[s] = price(s)
        S = price(t)
        if K is None or S is None or any(v is None for v in known.values()):
            continue
        out[t] = fair_up(t, times, known, S, K, sigma)[0]
    return out


def market_sigma(price, start):
    """Annualised vol from 10s returns over the hour before the open. As-of start."""
    px = [price(s) for s in range(start - 3600, start + 1, 10)]
    px = [p for p in px if p]
    return realized_vol(px, dt_seconds=10) if len(px) > 300 else None


# ------------------------------------------------------------------ one market

def process(path_str, avg=60):
    with gzip.open(path_str, "rt") as f:
        m = json.load(f)
    op = m["outcome_prices"]
    if m.get("truncated") or not m.get("closed") or op.get("Up") not in ("0", "1"):
        return {"skip": "unresolved/truncated"}
    winner = "Up" if op["Up"] == "1" else "Down"
    price = Prices()
    start, end = m["start"], m["end"]
    K, sigma = strike(price, start), market_sigma(price, start)
    if K is None or sigma is None:
        return {"skip": "no binance"}
    K_point = price(start)

    fs = m.get("fee_schedule") or {}
    rate = fs.get("rate", FEE_RATE) if m.get("fees_enabled") else 0.0

    # settlement readings, proxied by Binance (basis cancels: K and A share a feed)
    end_px = price(end)
    full = [price(s) for s in settle_times(start, 300, 300)]
    last60 = [price(s) for s in settle_times(start, 300, 60)]
    readings = None
    if end_px and K_point and all(full) and all(last60):
        up = winner == "Up"
        a60 = sum(last60) / 60
        readings = {"end avg60 vs open avg60": ((a60 >= K) == up, abs(a60 - K)),
                    "end avg60 vs open print": ((a60 >= K_point) == up, abs(a60 - K_point)),
                    "avg300 vs open print (description)": ((sum(full) / 300 >= K_point) == up,
                                                           abs(sum(full) / 300 - K_point)),
                    "end print vs open print": ((end_px >= K_point) == up, abs(end_px - K_point))}

    fair = fair_path(price, start, avg, sigma)
    fills = []
    for ts, outcome, side, p, size, _tx in m["trades"]:
        if not (start + 1 <= ts < end) or outcome not in ("Up", "Down"):
            continue
        won = 1.0 if outcome == winner else 0.0
        fee = taker_fee(p, rate=rate)
        sign = 1.0 if side == "BUY" else -1.0
        pnl = sign * (won - p) - fee
        edges = []
        for D in DELAYS:
            f_up = fair.get(ts - D)
            if f_up is None:
                edges.append(None)
                continue
            f = f_up if outcome == "Up" else 1 - f_up
            edges.append(sign * (f - p) - fee)
        fills.append((ts - start, p, size, pnl, edges))
    # calibration samples: model P(Up) at fixed points, beside the last Up trade price before it
    up_trades = sorted((ts, p if side == "BUY" else p) for ts, outcome, side, p, size, _ in m["trades"]
                       if outcome == "Up" and start - 300 <= ts)
    samples = []
    for into in (30, 90, 150, 210, 270, 290):
        f = fair.get(start + into)
        prior = [p for ts, p in up_trades if ts <= start + into - 5]  # -5: the on-chain stamp lag
        samples.append((into, f, prior[-1] if prior else None, winner == "Up"))
    return {"slug": m["slug"], "rate": rate, "readings": readings, "fills": fills, "samples": samples}


# ------------------------------------------------------------------ stats

def clustered(groups):
    """Per-share mean and market-clustered SE from [(sum_pnl, n_shares), ...] per market."""
    groups = [(s, n) for s, n in groups if n > 0]
    M = len(groups)
    N = sum(n for _, n in groups)
    if M < 2 or N == 0:
        return float("nan"), float("nan"), M
    mean = sum(s for s, _ in groups) / N
    var = sum((s - mean * n) ** 2 for s, n in groups) / N ** 2 * M / (M - 1)
    return mean, math.sqrt(var), M


def report(results):
    used = [r for r in results if "fills" in r]
    skips = {}
    for r in results:
        if "skip" in r:
            skips[r["skip"]] = skips.get(r["skip"], 0) + 1
    print(f"markets used {len(used)}   skipped {skips}")
    rates = {}
    for r in used:
        rates[r["rate"]] = rates.get(r["rate"], 0) + 1
    print(f"taker fee rate by market: {rates}")

    print("\n[A] settlement reading vs official outcome (Binance as the Chainlink proxy)")
    rd = [r["readings"] for r in used if r["readings"]]
    for k in rd[0] if rd else ():
        allv = [x[k][0] for x in rd]
        far = [x[k][0] for x in rd if x[k][1] > 15]
        print(f"  {k:<36} agrees {sum(allv) / len(allv):6.1%}   when |margin| > $15: "
              f"{sum(far)}/{len(far)} ({sum(far) / max(len(far), 1):.2%})")
    print("  The right rule should only miss on near-ties, where Binance and Chainlink differ by a few dollars.")

    print("\n[A2] is the model calibrated? P(Up) buckets vs how often Up actually won")
    smp = [(f, px, won) for r in used for into, f, px, won in r["samples"] if f is not None]
    print(f"  {'model P(Up)':>12} {'samples':>8} {'Up won':>8} {'+/- 2SE':>8}")
    for lo in [i / 10 for i in range(10)]:
        b = [won for f, _, won in smp if lo <= f < lo + 0.1 or (lo == 0.9 and f == 1.0)]
        if b:
            w = sum(b) / len(b)
            print(f"  {lo:>5.1f}-{lo + 0.1:.1f} {len(b):>8} {w:>8.3f} {2 * math.sqrt(max(w * (1 - w), 1e-9) / len(b)):>8.3f}")
    both = [(f, px, won) for f, px, won in smp if px is not None]
    if both:
        bm = sum((f - won) ** 2 for f, _, won in both) / len(both)
        bp = sum((px - won) ** 2 for _, px, won in both) / len(both)
        print(f"  Brier on {len(both)} samples with a prior trade: model {bm:.4f}   last Polymarket trade {bp:.4f}   (lower wins)")

    print("\n[B] copy every taker fill with model edge >= theta: realised P&L per share, clustered by market")
    print(f"  {'':>18}" + "".join(f"{'D=' + str(D) + 's' + (' (LEAK)' if D == 0 else ''):>30}" for D in DELAYS))
    for theta in THETAS:
        cells = []
        for j, D in enumerate(DELAYS):
            groups, n_fills = [], 0
            for r in used:
                s = n = 0.0
                for into, p, size, pnl, edges in r["fills"]:
                    e = edges[j]
                    if e is None or (theta is not None and e < theta):
                        continue
                    s += pnl * size
                    n += size
                    n_fills += 1
                groups.append((s, n))
            mean, se, M = clustered(groups)
            cells.append(f"{100 * mean:+6.2f}c +/-{100 * se:4.2f} n={n_fills:>7,}")
        label = "all takers" if theta is None else f"edge >= {100 * theta:.0f}c"
        print(f"  {label:>18}" + "".join(f"{c:>30}" for c in cells))
    print("  Cents per share after fees; +/- is one market-clustered SE; n is fills.")

    print("\n[C] where the D=5s, edge>=2c fills sit in the window")
    j = DELAYS.index(5)
    for lo, hi in ((1, 60), (60, 180), (180, 240), (240, 285), (285, 300)):
        groups = []
        for r in used:
            s = n = 0.0
            for into, p, size, pnl, edges in r["fills"]:
                if lo <= into < hi and edges[j] is not None and edges[j] >= 0.02:
                    s += pnl * size
                    n += size
            groups.append((s, n))
        mean, se, M = clustered(groups)
        shares = sum(n for _, n in groups)
        print(f"  seconds {lo:>3}-{hi:<3}  {100 * mean:+6.2f}c +/- {100 * se:4.2f}   shares {shares:>12,.0f}   markets {M}")


# ------------------------------------------------------------------ verify

def verify():
    ok = True

    def check(label, cond, detail=""):
        nonlocal ok
        ok &= cond
        print(f"  {'PASS' if cond else 'FAIL'}  {label:<64} {detail}")

    print("[1] no lookahead: corrupting every price after t leaves fair(t) unchanged")
    rng = random.Random(1)
    start = 1_786_000_000 - 1_786_000_000 % 300
    base = {s: 77_000 + 30 * math.sin(s / 17) + rng.gauss(0, 3) for s in range(start - 100, start + 400)}
    clean = lambda s: base.get(s)
    ref = fair_path(clean, start, 300, 0.4)
    worst = 0.0
    for t in (start + 1, start + 90, start + 200, start + 299):
        dirty = lambda s, t=t: base[s] if s <= t else base[s] + 5000.0
        got = fair_path(dirty, start, 300, 0.4)
        worst = max(worst, abs(got[t] - ref[t]))
        later = [u for u in ref if u > t]
        # the corruption must be visible somewhere, or the test proves nothing;
        # at the last second there is no later value to show it
        moved = not later or any(got[u] != ref[u] for u in later)
        check(f"t = open+{t - start:<3}: fair(t) identical" + (", later values do move" if later else ""),
              got[t] == ref[t] and moved)

    print("[2] clustered SE recovers the true SE when fills share a market outcome")
    reps, M, per = 400, 200, 30
    means, ses, naive = [], [], []
    for _ in range(reps):
        groups, flat = [], []
        for _m in range(M):
            shock = rng.gauss(0, 0.5)                      # one outcome per market
            xs = [shock + rng.gauss(0, 0.1) for _ in range(per)]
            groups.append((sum(xs), per))
            flat += xs
        mean, se, _ = clustered(groups)
        means.append(mean)
        ses.append(se)
        mu = sum(flat) / len(flat)
        naive.append(math.sqrt(sum((x - mu) ** 2 for x in flat) / (len(flat) - 1) / len(flat)))
    mu = sum(means) / reps
    true_se = math.sqrt(sum((x - mu) ** 2 for x in means) / (reps - 1))
    avg_se = sum(ses) / reps
    avg_naive = sum(naive) / reps
    check("clustered SE within 10% of the Monte Carlo SE", abs(avg_se / true_se - 1) < 0.10,
          f"{avg_se:.4f} vs true {true_se:.4f}")
    check("naive SE is far too tight (expect ~sqrt(30) = 5.5x)", true_se / avg_naive > 4,
          f"{true_se / avg_naive:.1f}x")

    print("[3] P&L sign conventions")
    check("buy at 0.40, wins: 1 - 0.40 - fee", abs((1 - 0.40) - taker_fee(0.40) - 0.5832) < 1e-12)
    check("sell at 0.40, wins: 0.40 - 1 - fee", abs((0.40 - 1) - taker_fee(0.40) + 0.6168) < 1e-12)

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--avg", type=int, default=60, help="settlement seconds averaged at the close (measured: 60)")
    ap.add_argument("--limit", type=int, default=0, help="first N markets only (smoke test)")
    args = ap.parse_args()
    if not verify():
        raise SystemExit(1)
    if args.verify:
        return
    paths = sorted(str(p) for p in (DATA / "pm").glob("*.json.gz"))
    if args.limit:
        paths = paths[:args.limit]
    print(f"\nprocessing {len(paths)} markets ...")
    with Pool() as pool:
        results = pool.starmap(process, [(p, args.avg) for p in paths], chunksize=8)
    report(results)


if __name__ == "__main__":
    main()
