"""Are there dislocations between Polymarket's 5-minute BTC books and BTC itself
that survive fees AND last long enough for us to reach them?

Replays pm_hf.sqlite strictly in local receive order: at any instant the model
sees only rows whose recv_ns is already past. Nothing is aligned on exchange
timestamps, so this machine's clock offset (~3.2s slow, measured 2026-09-13)
cannot leak the future — it only enters through `now`, estimated as-of.

    python analyze.py                      # default: OKX perp reference, DVOL sigma
    python analyze.py --ref coinbase --avg 0

What it prints, in the order you should distrust the result:

  [1] settlement rule — which reading of "TWAP of the range" matches the
      official outcomes on recorded windows. The model is wrong until this agrees.
  [2] calibration — Brier score of the model vs Polymarket's own mid on resolved
      windows, differenced per window. If Polymarket's mid is the better
      forecaster, "dislocations" are mostly the model's error, not the market's.
  [3] opportunities — episodes where a quoted ask sits below fair value by more
      than the taker fee, how long each survives, and what is left at our latency.
  [4] controls — the same count with the reference feed delayed 3s, and with
      the slow feed (Chainlink itself) as reference. An information edge shrinks
      when the information is made stale; a model artifact does not.
"""

import argparse
import bisect
import json
import math
import sqlite3
import statistics
import urllib.request
from pathlib import Path

from backtest import clustered
from fair_value import fair_up, realized_vol, settle_times, taker_fee

DB_PATH = Path(__file__).with_name("pm_hf.sqlite")
WINDOW = 300
LATENCIES_MS = (0, 100, 250, 500, 1000, 2000)


# ------------------------------------------------------------------ loading

def load_series(db, src, lo_ns, hi_ns):
    """[(recv_ns, exch_ms, value)] sorted by receive time. value = mid, or last if no book."""
    rows = db.execute("SELECT recv_ns, exch_ms, bid, ask, last FROM ticks WHERE src=? AND recv_ns BETWEEN ? AND ? "
                      "ORDER BY recv_ns", (src, lo_ns, hi_ns)).fetchall()
    out = []
    for recv, exch, bid, ask, last in rows:
        v = (bid + ask) / 2 if bid and ask else last
        if v:
            out.append((recv, exch, v))
    return out


def fetch_outcomes(db):
    """Official outcome for closed windows, cached in the windows table."""
    pending = db.execute("SELECT slug FROM windows WHERE outcome IS NULL AND end < strftime('%s','now') - 600").fetchall()
    for (slug,) in pending:
        try:
            req = urllib.request.Request(f"https://gamma-api.polymarket.com/events?slug={slug}",
                                         headers={"User-Agent": "pm-hf-analyze"})
            m = json.loads(urllib.request.urlopen(req, timeout=15).read())[0]["markets"][0]
            prices = dict(zip(json.loads(m["outcomes"]), json.loads(m["outcomePrices"])))
            if prices.get("Up") in ("1", "0") and m.get("closed"):
                db.execute("UPDATE windows SET outcome=? WHERE slug=?",
                           ("Up" if prices["Up"] == "1" else "Down", slug))
        except (OSError, ValueError, KeyError, IndexError):
            pass  # not resolved yet, or the lookup failed: stays NULL and is retried next run
    db.commit()


class Book:
    """Top-of-book from Polymarket's market channel. Checks itself against the
    best_bid/best_ask the exchange sends with every price change."""

    def __init__(self):
        self.levels = {}  # asset -> {"bids": {price: size}, "asks": {...}}
        self.checked = self.mismatched = 0

    def apply(self, msg):
        for m in (msg if isinstance(msg, list) else [msg]):
            if "bids" in m and "asks" in m and "asset_id" in m:
                self.levels[m["asset_id"]] = {
                    "bids": {float(x["price"]): float(x["size"]) for x in m["bids"]},
                    "asks": {float(x["price"]): float(x["size"]) for x in m["asks"]}}
            for c in m.get("price_changes", []):
                book = self.levels.get(c["asset_id"])
                if book is None:
                    continue
                side = book["bids" if c["side"] == "BUY" else "asks"]
                p, s = float(c["price"]), float(c["size"])
                if s == 0:
                    side.pop(p, None)
                else:
                    side[p] = s
                if c.get("best_ask"):
                    self.checked += 1
                    ask = self.best_ask(c["asset_id"])
                    self.mismatched += ask is None or abs(ask[0] - float(c["best_ask"])) > 1e-9

    def best_ask(self, asset):
        asks = self.levels.get(asset, {}).get("asks")
        if not asks:
            return None
        p = min(asks)
        return p, asks[p]

    def mid(self, asset):
        lv = self.levels.get(asset)
        if not lv or not lv["bids"] or not lv["asks"]:
            return None
        return (max(lv["bids"]) + min(lv["asks"])) / 2


# ------------------------------------------------------------------ replay one window

def replay_window(db, w, ref_src, lag_ms, avg_seconds, sigma_src, threshold, usd, step_ms):
    slug, start, end, up_tok, dn_tok, outcome = w
    # Exchange time and local receive time differ by the clock offset, so pull a
    # generous local range: 2 minutes either side.
    lo, hi = (start - 400) * 10**9, (end + 120) * 10**9
    chain = load_series(db, "chainlink", lo, hi)
    ref = chain if ref_src == "chainlink" else load_series(db, ref_src, lo, hi)
    # The clock offset is always read off the fastest feed, never the (possibly
    # delayed) reference: a control that stales the price must not also stale `now`.
    clock = load_series(db, "okx_perp", lo, hi)
    dvol = db.execute("SELECT recv_ns, value FROM dvol WHERE recv_ns BETWEEN ? AND ? ORDER BY recv_ns",
                      (lo - 600 * 10**9, hi)).fetchall()
    msgs = db.execute("SELECT recv_ns, payload FROM pm_msgs WHERE slug=? ORDER BY recv_ns", (slug,)).fetchall()
    if not (chain and ref and msgs):
        return None
    if sigma_src == "rv":
        # The backtest's sigma, which calibrated on 8,599 markets: annualised vol of
        # 10-second moves over the hour before the open, as-of the open. DVOL (30-day
        # implied) left the live model at Brier 0.154 against Polymarket's 0.092.
        vol_feed = load_series(db, "okx_perp", (start - 3600) * 10**9, start * 10**9)
        grid, j, last = [], 0, None
        for g in range((start - 3600) * 10**9, start * 10**9 + 1, 10 * 10**9):
            while j < len(vol_feed) and vol_feed[j][0] <= g:
                last = vol_feed[j][2]
                j += 1
            if last is not None:
                grid.append(last)
        if len(grid) < 60:  # under 10 minutes of history: no honest sigma
            return {"slug": slug, "skipped": f"only {len(grid) * 10}s of pre-open history for sigma"}
        sigma_fixed = realized_vol(grid, dt_seconds=10)
    # Coverage. A window replayed across a hole is not evidence: on 2026-09-13
    # the machine slept for 38 minutes mid-recording and the first two windows
    # came back full of "opportunities" that were nothing but stale state.
    # The relay routinely skips isolated seconds (~2.6% measured), so the gate is
    # on the longest RUN of missing seconds, not the total.
    have = {c[1] // 1000 for c in chain}
    run_len = longest = 0
    for s in range(start, end + 1):
        run_len = 0 if s in have else run_len + 1
        longest = max(longest, run_len)
    local = lambda rows: [r[0] for r in rows if start * 10**9 <= r[0] <= end * 10**9]
    def max_gap_s(stamps):
        return max((b - a for a, b in zip(stamps, stamps[1:])), default=10**12) / 1e9
    if start not in have or end not in have or longest > 3 or max_gap_s(local(clock)) > 5 or max_gap_s(local(msgs)) > 20:
        return {"slug": slug, "skipped": f"longest Chainlink hole {longest}s (start print "
                f"{'present' if start in have else 'MISSING'}), OKX gap {max_gap_s(local(clock)):.0f}s, "
                f"Polymarket gap {max_gap_s(local(msgs)):.0f}s"}

    times = settle_times(start, WINDOW, avg_seconds)
    book, known, basis_samples = Book(), {}, []
    ci = ri = di = ki = 0
    ref_now = None
    ref_hist_exch, ref_hist_val = [], []
    offset_ms = None  # exchange clock minus local clock, EWMA over the OKX feed
    sigma = None
    episodes = {"Up": [], "Down": []}
    open_ep = {"Up": None, "Down": None}
    samples = []  # (seconds into window, model P(Up), Polymarket mid)
    last_eval = -10**18
    next_sample = start + 10

    for recv, payload in msgs:
        try:
            book.apply(json.loads(payload))
        except ValueError:
            continue
        # --- advance every other feed to what had arrived by `recv`
        while ci < len(chain) and chain[ci][0] <= recv:
            sec, val = chain[ci][1] // 1000, chain[ci][2]
            known[sec] = val
            j = bisect.bisect_right(ref_hist_exch, sec * 1000) - 1
            if ref_src != "chainlink" and j >= 0:
                basis_samples = (basis_samples + [val - ref_hist_val[j]])[-60:]
            ci += 1
        while ri < len(ref) and ref[ri][0] <= recv - lag_ms * 10**6:
            r_recv, r_exch, r_val = ref[ri]
            ref_now = r_val
            ref_hist_exch.append(r_exch)
            ref_hist_val.append(r_val)
            ri += 1
        while di < len(dvol) and dvol[di][0] <= recv:
            if sigma_src == "dvol":
                sigma = dvol[di][1] / 100
            di += 1
        while ki < len(clock) and clock[ki][0] <= recv:
            o = clock[ki][1] - clock[ki][0] / 1e6
            offset_ms = o if offset_ms is None else 0.99 * offset_ms + 0.01 * o
            ki += 1
        if sigma_src == "rv":
            sigma = sigma_fixed

        if recv - last_eval < step_ms * 10**6 or offset_ms is None or ref_now is None or not sigma:
            continue
        last_eval = recv
        now = recv / 1e9 + offset_ms / 1000  # exchange-clock seconds
        open_prints = [known[s] for s in range(start - 59, start + 1) if s in known]
        if not (start + 1 <= now < end) or len(open_prints) < 55:
            continue
        basis = statistics.median(basis_samples) if basis_samples else (0.0 if ref_src == "chainlink" else None)
        if basis is None:
            continue
        # strike = 60s average ending at the open (measured on 786 markets, see backtest.strike)
        S, K = ref_now + basis, sum(open_prints) / len(open_prints)
        p_up = fair_up(now, times, known, S, K, sigma)[0]

        if now >= next_sample:
            samples.append((next_sample - start, p_up, book.mid(up_tok)))
            next_sample += 10

        for side, tok, fair in (("Up", up_tok, p_up), ("Down", dn_tok, 1 - p_up)):
            ba = book.best_ask(tok)
            edge = fair - ba[0] - taker_fee(ba[0]) if ba else -1.0
            ep = open_ep[side]
            if edge > threshold:
                shares = min(ba[1], usd / ba[0])
                point = (recv, edge, shares * edge, ba[0])
                if ep is None:
                    open_ep[side] = {"start": recv, "points": [point], "ask": ba[0], "fair": fair,
                                     "into": now - start}
                else:
                    ep["points"].append(point)
            elif ep is not None:
                ep["end"] = recv
                episodes[side].append(ep)
                open_ep[side] = None
    for side, ep in open_ep.items():
        if ep is not None:
            ep["end"] = ep["points"][-1][0]
            episodes[side].append(ep)

    return {"slug": slug, "start": start, "outcome": outcome, "episodes": episodes, "samples": samples,
            "book_checked": book.checked, "book_mismatch": book.mismatched,
            "known": known, "times": times}


# ------------------------------------------------------------------ reports

def settlement_check(results):
    print("\n[1] settlement rule vs official outcomes")
    rows = {"print at end >= print at start": 0, "mean of 300 prints >= print at start": 0,
            "mean of last 60 prints >= print at start": 0, "mean of last 60 >= mean of 60 before open": 0}
    n = 0
    for r in results:
        k, s = r["known"], r["start"]
        # Averages use the prints that exist; the relay skips ~2.6% of seconds.
        full = [k[s + 1 + j] for j in range(WINDOW) if s + 1 + j in k]
        tail = [k[t] for t in range(s + WINDOW - 59, s + WINDOW + 1) if t in k]
        if r["outcome"] is None or s not in k or s + WINDOW not in k:
            continue
        n += 1
        up = r["outcome"] == "Up"
        rows["print at end >= print at start"] += (k[s + WINDOW] >= k[s]) == up
        rows["mean of 300 prints >= print at start"] += (sum(full) / len(full) >= k[s]) == up
        rows["mean of last 60 prints >= print at start"] += (sum(tail) / len(tail) >= k[s]) == up
        pre = [k[t] for t in range(s - 59, s + 1) if t in k]
        if pre:
            rows["mean of last 60 >= mean of 60 before open"] += (sum(tail) / len(tail) >= sum(pre) / len(pre)) == up
    if not n:
        print("  no resolved window with a complete set of Chainlink prints yet")
        return
    for label, hits in rows.items():
        print(f"  {label:<44} agrees {hits}/{n}")


def calibration(results):
    print("\n[2] Brier score on resolved windows: model vs Polymarket mid (lower is better)")
    per_window = []
    for r in results:
        if r["outcome"] is None:
            continue
        y = 1.0 if r["outcome"] == "Up" else 0.0
        pairs = [(p, m) for _, p, m in r["samples"] if m is not None]
        if pairs:
            bm = sum((p - y) ** 2 for p, _ in pairs) / len(pairs)
            bp = sum((m - y) ** 2 for _, m in pairs) / len(pairs)
            per_window.append((bm, bp))
    n = len(per_window)
    if n < 2:
        print(f"  {n} resolved window(s) — need more recording")
        return
    d = [bm - bp for bm, bp in per_window]
    se = statistics.stdev(d) / math.sqrt(n)
    print(f"  windows {n}   model {statistics.mean(x for x, _ in per_window):.4f}   "
          f"polymarket {statistics.mean(y for _, y in per_window):.4f}   "
          f"model - pm {statistics.mean(d):+.4f} +/- {se:.4f} (SE across windows)")
    print("  Samples every 10s within a window share one outcome, so the window — not the sample — is the unit.")


def opportunities(results, label, rtt_ms):
    """Episode survival by arrival latency, and what taking one share at that
    moment actually paid at settlement.

    The realized column is the one that decides. Episode COUNTS measure where the
    model disagrees with the book, which on 2026-09-13 was flat across the
    future-leak, live and stale controls (115/115/116): mostly model error. The
    backtest found the edge by scoring takes against outcomes, so this does too.
    """
    eps = [(r, side, e) for r in results for side in ("Up", "Down") for e in r["episodes"][side]]
    print(f"\n  {label}: {len(eps)} episodes over {len(results)} windows")
    if not eps:
        return {L: (0, float("nan"), float("nan")) for L in LATENCIES_MS}
    durs = sorted((e["end"] - e["start"]) / 1e6 for _, _, e in eps)
    print(f"    duration ms  median {durs[len(durs) // 2]:.0f}   p90 {durs[int(0.9 * (len(durs) - 1))]:.0f}   "
          f"max {durs[-1]:.0f}")
    out = {}
    print(f"    {'arrive after':>14} {'still open':>11} {'model edge c':>13} {'realized c/share':>22} {'windows':>8}")
    for L in LATENCIES_MS:
        alive, edges, per_window = 0, [], {}
        for r, side, e in eps:
            if L > 0 and e["end"] - e["start"] < L * 10**6:
                continue
            target = e["start"] + L * 10**6
            pt = next((p for p in e["points"] if p[0] >= target), e["points"][-1])
            alive += 1
            edges.append(pt[1])
            if r["outcome"] is not None:
                ask = pt[3]
                pnl = (1.0 if r["outcome"] == side else 0.0) - ask - taker_fee(ask)
                s, n = per_window.get(r["slug"], (0.0, 0))
                per_window[r["slug"]] = (s + pnl, n + 1)
        mean, se, M = clustered(list(per_window.values()))
        out[L] = (alive, mean, se)
        below = [x for x in LATENCIES_MS if rtt_ms and x <= rtt_ms]
        mark = "  <- nearest row at or below our TCP connect time" if below and L == below[-1] else ""
        realized = f"{100 * mean:+.2f} +/- {100 * se:.2f}" if M >= 2 else "n/a"
        print(f"    {L:>11} ms {alive:>11} {100 * statistics.mean(edges):>13.2f} {realized:>22} {M:>8}{mark}")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ref", default="okx_perp", choices=["okx_perp", "coinbase", "deribit_index", "deribit_perp", "chainlink"])
    ap.add_argument("--avg", type=int, default=60, help="settlement prints averaged at the close (measured: 60)")
    ap.add_argument("--sigma", default="rv", choices=["rv", "dvol"],
                    help="rv = backtest's trailing-hour 10s realized vol (calibrated); dvol = Deribit 30d implied")
    ap.add_argument("--threshold", type=float, default=0.0, help="edge after fee required, in dollars/share")
    ap.add_argument("--usd", type=float, default=100.0, help="max dollars per take, for the $ column")
    ap.add_argument("--step-ms", type=int, default=50, help="evaluate at most once per N ms of receive time")
    args = ap.parse_args()

    db = sqlite3.connect(DB_PATH, timeout=30)
    fetch_outcomes(db)
    windows = db.execute("SELECT slug, start, end, up_token, down_token, outcome FROM windows "
                         "WHERE end < strftime('%s','now') ORDER BY start").fetchall()
    rtt = [r[0] for r in db.execute("SELECT ms FROM rtt")]
    rtt_ms = statistics.median(rtt) if rtt else None
    print(f"windows finished: {len(windows)}   resolved: {sum(1 for w in windows if w[5])}   "
          f"median TCP connect to clob.polymarket.com: {rtt_ms if rtt_ms is None else round(rtt_ms)} ms")
    print(f"model: ref={args.ref}  settle avg={args.avg}s  sigma={args.sigma}  threshold={args.threshold}")

    def run(ref, lag):
        res = [r for w in windows
               if (r := replay_window(db, w, ref, lag, args.avg, args.sigma, args.threshold, args.usd, args.step_ms))]
        return [r for r in res if "skipped" not in r], [r for r in res if "skipped" in r]

    base, skipped = run(args.ref, 0)
    for r in skipped:
        print(f"  SKIPPED {r['slug']}: {r['skipped']}")
    if not base:
        print("no complete windows recorded yet")
        return
    checked = sum(r["book_checked"] for r in base)
    bad = sum(r["book_mismatch"] for r in base)
    print(f"book rebuild: {bad} of {checked} best-ask checks disagreed with the exchange ({bad / max(checked, 1):.2%})")

    settlement_check(base)
    calibration(base)
    print("\n[3] opportunities: ask below fair by more than the taker fee")
    live = opportunities(base, f"reference {args.ref}, live", rtt_ms)
    print("\n[4] controls")
    cheat = opportunities(run(args.ref, -3000)[0], f"reference {args.ref} 3000 ms IN THE FUTURE (a deliberate "
                          "leak: proves the detector can see an edge when one exists)", rtt_ms)
    stale = opportunities(run(args.ref, 3000)[0], f"reference {args.ref} delayed 3000 ms", rtt_ms)
    if args.ref != "chainlink":
        opportunities(run("chainlink", 0)[0], "reference chainlink (the slow feed)", rtt_ms)
    print("\n  realized c/share arriving 250 ms after the dislocation opens (clustered by window):")
    for name, res in (("future-leak", cheat), ("live", live), ("stale-3s", stale)):
        n, mean, se = res[250]
        print(f"    {name:<12} {100 * mean:+6.2f} +/- {100 * se:.2f}   ({n} episodes)")
    print("  Read it as: leak >> live means the detector works. live >> stale means speed pays.")
    print("  The backtest's answer on 13.8M fills was leak +2.4c, ~0 at 0.5s, -1.1c at 2.5s:")
    print("  expect days of windows before these error bars can say anything as sharp.")


if __name__ == "__main__":
    main()
