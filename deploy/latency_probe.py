"""Measure what a server location actually buys. Run it on each candidate machine.

    python3 deploy/latency_probe.py

Reports medians over 20 samples of:
  * Kalshi REST on a kept-alive connection (what the bot's book poll pays)
  * TCP connect to Kalshi's API host and to Coinbase's websocket host (~1 network round trip)
Only public endpoints; no key needed.
"""

import socket
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from kalshi_client import Rest  # noqa: E402


def tcp_ms(host, port=443):
    t0 = time.perf_counter()
    with socket.create_connection((host, port), timeout=5):
        pass
    return (time.perf_counter() - t0) * 1000


def main():
    rest = Rest("prod")
    rest.request("GET", "/exchange/status")  # warm the connection; the first includes TLS
    kalshi = []
    for _ in range(20):
        kalshi.append(rest.request("GET", "/exchange/status")[1])
        time.sleep(0.25)
    rows = [("Kalshi REST, kept-alive", kalshi),
            ("TCP connect api.elections.kalshi.com", [tcp_ms("api.elections.kalshi.com") for _ in range(20)]),
            ("TCP connect ws-feed.exchange.coinbase.com", [tcp_ms("ws-feed.exchange.coinbase.com") for _ in range(20)])]
    for label, xs in rows:
        xs = sorted(xs)
        print(f"{label:<44} median {statistics.median(xs):6.1f} ms   p90 {xs[int(0.9 * (len(xs) - 1))]:6.1f} ms")


if __name__ == "__main__":
    main()
