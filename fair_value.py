"""Fair value of Polymarket's 5-minute BTC Up/Down contracts. Stdlib only.

Settlement, from the market description (read 2026-09-13):

    Up  if  the Chainlink BTC/USD TWAP "of the time range" >= the price at the
    beginning of that range.

That wording is NOT the rule. Measured against official outcomes on 786
markets (backtest.py [A]): Up iff the 60-second average ending at the close >=
the 60-second average ending at the open — 431/431 where the margin exceeded
$15; the description's full-window reading got 96%. So the caller passes
settle_times(start, 300, 60) and a strike K that is itself a 60s average.
Every function takes the list of averaged timestamps, so any reading is the
same code.

The model. Settlement is A = mean of the prints at `times`. At time `now`:

  * prints already received are fixed numbers;
  * prints at or before `now` that have not arrived yet (the relay runs ~1.7s
    late) are set to S, the reference price now, which already reflects them;
  * prints after `now` are S + sigma*S*B(u), u = seconds ahead: zero drift,
    arithmetic Brownian motion.

Over five minutes sigma*sqrt(T) is ~0.1%, so the arithmetic approximation to a
lognormal path is far inside any quoted tick; verify() [4] checks it against a
GBM simulation rather than asserting it.

A is then normal with

    mean = (sum(known) + n_future * S) / n
    var  = (sigma*S / n)^2 * sum_i sum_k min(u_i, u_k)

and P(Up) = N((mean - K) / sd). The double sum has an O(m) form for sorted u:
sum_k u_k * (2(m-k) + 1), k = 1..m, checked against the brute-force sum in [2].

Units: sigma is annualised on a 365-day, 24-hour year (BTC never closes),
timestamps are whole seconds. Sensitivities are returned per $1 of BTC (delta)
and per 1 vol point (vega, the raw partial / 100) — scaled here, at the boundary.
"""

import math
import random

SECONDS_PER_YEAR = 365 * 86400
FEE_RATE = 0.07  # Polymarket crypto markets, taker only


def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def taker_fee(price, shares=1.0, rate=FEE_RATE):
    """USDC fee for taking `shares` at `price`: C * rate * p * (1 - p).
    Peaks at p = 0.5 (1.75c a share, 3.5% of the price). Makers pay nothing."""
    return shares * rate * price * (1.0 - price)


def settle_times(start, window=300, avg_seconds=300):
    """Whole-second timestamps of the prints settlement averages.

    avg_seconds=300 -> every print in (start, start+300]   (full-window TWAP)
    avg_seconds=60  -> the last 60 prints
    avg_seconds=0   -> the single print at start+300     (point settlement)
    """
    end = start + window
    n = max(1, int(avg_seconds))
    return [end - n + 1 + j for j in range(n)]


def _cov_sum(u):
    """sum_i sum_k min(u_i, u_k) for u sorted ascending. O(m)."""
    m = len(u)
    return sum(uk * (2 * (m - k) + 1) for k, uk in enumerate(u, start=1))


def settle_moments(now, times, known, S, sigma_ann):
    """Mean and sd of the settlement average, seen at `now`.

    Returns (mean, sd, weight) where weight = d(mean)/dS, the share of the
    average not yet received.
    """
    s = sigma_ann / math.sqrt(SECONDS_PER_YEAR)
    total, n_ref, u = 0.0, 0, []
    for ts in times:
        if ts in known:
            total += known[ts]
        else:
            total += S
            n_ref += 1
            if ts > now:
                u.append(ts - now)
    n = len(times)
    var = (s * S / n) ** 2 * _cov_sum(u)
    return total / n, math.sqrt(var), n_ref / n


def fair_up(now, times, known, S, K, sigma_ann):
    """P(Up), delta per $1 of BTC, vega per vol point.

    K is the print at the start of the window. Before it is known the caller
    should not price: with zero drift the answer is 0.5 by symmetry and there
    is nothing to measure.
    """
    mean, sd, w = settle_moments(now, times, known, S, sigma_ann)
    if sd == 0.0:
        return (1.0 if mean >= K else 0.0), 0.0, 0.0
    z = (mean - K) / sd
    pdf = norm_pdf(z)
    # d sd/dS = sd/S (sd is proportional to S); d sd/d sigma = sd/sigma.
    delta = pdf * (w * sd - (mean - K) * sd / S) / sd ** 2
    vega = -pdf * z / sigma_ann
    return norm_cdf(z), delta, vega / 100.0


def realized_vol(prices, dt_seconds=1.0):
    """Annualised close-to-close vol of an evenly spaced price series (zero-mean)."""
    r = [math.log(b / a) for a, b in zip(prices, prices[1:])]
    if not r:
        return float("nan")
    return math.sqrt(sum(x * x for x in r) / len(r) * SECONDS_PER_YEAR / dt_seconds)


# --------------------------------------------------------------------------- verify

def verify():
    ok = True

    def check(label, cond, detail):
        nonlocal ok
        ok &= cond
        print(f"  {'PASS' if cond else 'FAIL'}  {label:<58} {detail}")

    print("[1] taker fee against Polymarket's worked example")
    check("100 shares at 0.50 = $1.75", abs(taker_fee(0.5, 100) - 1.75) < 1e-12,
          f"{taker_fee(0.5, 100):.6f}")
    check("symmetric: fee(p) == fee(1-p)",
          all(abs(taker_fee(p) - taker_fee(1 - p)) < 1e-15 for p in (0.01, 0.2, 0.37, 0.49)), "")
    check("peaks at 0.5", max(range(1, 100), key=lambda c: taker_fee(c / 100)) == 50, "")

    print("[2] O(m) covariance sum against the brute-force double sum")
    rng = random.Random(7)
    worst = 0.0
    for _ in range(50):
        u = sorted(rng.uniform(0, 300) for _ in range(rng.randint(1, 40)))
        brute = sum(min(a, b) for a in u for b in u)
        worst = max(worst, abs(_cov_sum(u) - brute) / brute)
    check("50 random sets, relative error", worst < 1e-12, f"{worst:.1e}")
    # Closed form when pricing at the open of a full-window TWAP: u = 1..n,
    # sum = n(n+1)(2n+1)/6.
    n = 300
    check("u = 1..300 matches n(n+1)(2n+1)/6",
          _cov_sum(list(range(1, n + 1))) == n * (n + 1) * (2 * n + 1) // 6, "")

    print("[3] finite differences: delta and vega against the analytic forms")
    start, S0, sig = 1_000_000, 77_000.0, 0.40
    states = []
    for avg in (300, 60, 0):
        times = settle_times(start, 300, avg)
        for elapsed in (5, 150, 262, 297):
            now = start + elapsed
            known = {t: S0 + 3.0 * math.sin(t) for t in times if t <= now - 2}
            for K in (S0 - 25, S0, S0 + 40):
                states.append((avg, now, times, known, K))
    worst_d = worst_v = 0.0
    for avg, now, times, known, K in states:
        p, delta, vega = fair_up(now, times, known, S0, K, sig)
        if not 1e-6 < p < 1 - 1e-6:
            continue
        hS, hv = 0.01, 1e-5
        fd_d = (fair_up(now, times, known, S0 + hS, K, sig)[0]
                - fair_up(now, times, known, S0 - hS, K, sig)[0]) / (2 * hS)
        fd_v = (fair_up(now, times, known, S0, K, sig + hv)[0]
                - fair_up(now, times, known, S0, K, sig - hv)[0]) / (2 * hv) / 100
        worst_d = max(worst_d, abs(delta - fd_d) / max(abs(fd_d), 1e-9))
        worst_v = max(worst_v, abs(vega - fd_v) / max(abs(fd_v), 1e-9))
    check(f"delta, {len(states)} states x 3 settlement readings", worst_d < 1e-5, f"worst rel {worst_d:.1e}")
    check("vega", worst_v < 1e-5, f"worst rel {worst_v:.1e}")

    print("[4] Monte Carlo under GBM with 1s prints (independent of the formula)")
    rng = random.Random(20260913)
    s1 = sig / math.sqrt(SECONDS_PER_YEAR)
    for avg, elapsed, K in ((300, 60, S0 + 20), (60, 250, S0 - 15), (0, 240, S0 + 10),
                           # 5s left on a full-window TWAP: sd is ~14 cents, K set inside it
                           (300, 295, S0 - 4.83)):
        times = settle_times(start, 300, avg)
        now = start + elapsed
        known = {t: S0 - 5.0 for t in times if t <= now}
        mean, sd, _ = settle_moments(now, times, known, S0, sig)
        p_model = fair_up(now, times, known, S0, K, sig)[0]
        future = [t for t in times if t > now]
        N, hits, sq = 20000, 0, 0.0
        for _ in range(N):
            x, t_prev, acc = math.log(S0), now, 0.0
            for t in future:
                x += s1 * math.sqrt(t - t_prev) * rng.gauss(0, 1) - 0.5 * s1 * s1 * (t - t_prev)
                t_prev = t
                acc += math.exp(x)
            A = (sum(known.values()) + acc) / len(times)
            hits += A >= K
            sq += (A - mean) ** 2
        p_mc, sd_mc = hits / N, math.sqrt(sq / N)
        se = math.sqrt(p_model * (1 - p_model) / N)
        check(f"avg={avg:>3}s at t+{elapsed}s  P(Up) model {p_model:.4f} vs MC {p_mc:.4f}",
              abs(p_model - p_mc) < 4 * se, f"|diff| {abs(p_model - p_mc) / se:.1f} SE")
        check(f"             sd model {sd:.3f} vs MC {sd_mc:.3f}",
              abs(sd_mc / sd - 1) < 4 * math.sqrt(0.5 / N) + 0.002, f"ratio {sd_mc / sd:.4f}")

    print("[5] edges")
    times = settle_times(start, 300, 300)
    known = {t: S0 for t in times}
    check("every print known, A == K  -> Up (>= rule), exactly 1",
          fair_up(start + 301, times, known, S0, S0, sig)[0] == 1.0, "")
    check("every print known, A < K   -> exactly 0",
          fair_up(start + 301, times, known, S0, S0 + 1e-6, sig)[0] == 0.0, "")
    far = fair_up(start + 10, times, {}, S0, S0 - 5000, sig)[0]
    check("deep in the money (K $5000 below) -> 1 to 1e-12", abs(far - 1) < 1e-12, f"{1 - far:.1e}")
    wide = fair_up(start + 10, times, {}, S0, S0 - 20, 500.0)[0]
    check("vol of 50,000%: price barely matters -> 0.5", abs(wide - 0.5) < 1e-3, f"{wide:.5f}")
    # Point settlement must collapse to the textbook arithmetic digital,
    # N((S-K) / (sigma*S*sqrt(tau))), at every time left, including 1 second.
    worst = 0.0
    for left in (299, 120, 30, 5, 1):
        got = fair_up(start + 300 - left, settle_times(start, 300, 0), {}, S0, S0 - 20, sig)[0]
        want = norm_cdf(20 / (s1 * S0 * math.sqrt(left)))
        worst = max(worst, abs(got - want))
    check("point settle == N((S-K)/(sigma S sqrt tau)), tau 1..299s", worst < 1e-14, f"{worst:.1e}")

    print("[6] realized_vol recovers a known sigma from a GBM path")
    path, x = [S0], math.log(S0)
    for _ in range(100_000):
        x += s1 * rng.gauss(0, 1) - 0.5 * s1 * s1
        path.append(math.exp(x))
    rv = realized_vol(path)
    check("sigma 0.40 on 100k 1s steps (SE 0.2%)", abs(rv / sig - 1) < 0.01, f"{rv:.4f}")

    print("\nALL PASS" if ok else "\nFAILURES ABOVE")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if verify() else 1)
