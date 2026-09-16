"""Kalshi REST + websocket client: signing, one persistent connection, 429 backoff.

Keys come ONLY from environment variables the operator sets in their own shell.
This module never prints, logs, or writes a key or a signature.

    KALSHI_DEMO_KEY_ID  / KALSHI_DEMO_KEY_PATH   demo exchange: orders allowed
    KALSHI_KEY_ID       / KALSHI_KEY_PATH        production: READ-ONLY market data
    KALSHI_TRADE_KEY_ID / KALSHI_TRADE_KEY_PATH  production: the capped live mode's trading key

`Rest` refuses any non-GET request to production unless it was built with
live_orders=True. Only paper_bot.LiveBroker does that, and only when the operator has
armed live mode on the server (the arming script (not published)). Real-money caps live there.

    python kalshi_client.py verify            # offline: fee rounding, the production order guard
    python kalshi_client.py demo-selftest     # auth, balance, one unfillable IOC order, latency
    python kalshi_client.py prod-readcheck    # auth works for the read-only websocket feed
    python kalshi_client.py prod-order-check  # trading key: balance + two IOC orders at 1c that cannot fill

Signing (docs.kalshi.com, API keys page, read 2026-09-13): RSA-PSS, SHA-256,
salt = digest length, over  timestamp_ms + METHOD + path-without-query,
base64, in headers KALSHI-ACCESS-KEY / -TIMESTAMP / -SIGNATURE.
"""

import base64
import http.client
import json
import math
import os
import sys
import threading
import time
import uuid

HOSTS = {
    "prod": {"rest": "api.elections.kalshi.com", "ws": "wss://api.elections.kalshi.com/trade-api/ws/v2"},
    "demo": {"rest": "external-api.demo.kalshi.co", "ws": "wss://external-api.demo.kalshi.co/trade-api/ws/v2"},
}
PREFIX = "/trade-api/v2"
WS_PATH = "/trade-api/ws/v2"


class RateLimited(Exception):
    pass


class KeyMissing(Exception):
    pass


def load_key(env):
    """(key_id, private_key) for 'prod' or 'demo' from env vars. Raises KeyMissing with the reason."""
    id_var, path_var = {"demo": ("KALSHI_DEMO_KEY_ID", "KALSHI_DEMO_KEY_PATH"),
                        "trade": ("KALSHI_TRADE_KEY_ID", "KALSHI_TRADE_KEY_PATH")}.get(env, ("KALSHI_KEY_ID", "KALSHI_KEY_PATH"))
    key_id, path = os.environ.get(id_var), os.environ.get(path_var)
    if (not key_id or not path) and sys.platform == "win32":
        # set with setx but this process started before: read the operator's saved variables directly
        import winreg
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as reg:
                key_id = key_id or winreg.QueryValueEx(reg, id_var)[0]
                path = path or winreg.QueryValueEx(reg, path_var)[0]
        except OSError:
            pass
    if not key_id or not path:
        raise KeyMissing(f"set {id_var} and {path_var} in your own shell")
    if not os.path.exists(path):
        raise KeyMissing(f"{path_var} points to a file that does not exist")
    from cryptography.hazmat.primitives import serialization  # third-party: exchange connectivity only
    with open(path, "rb") as f:
        return key_id, serialization.load_pem_private_key(f.read(), password=None)


def auth_headers(key, method, path):
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    key_id, private = key
    ts = str(int(time.time() * 1000))
    msg = (ts + method + path.split("?")[0]).encode()
    sig = private.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                       hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": key_id, "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}


class Rest:
    """One kept-alive HTTPS connection. A fresh TLS handshake per request was most
    of the 98-110 ms book round trip measured from home on 2026-09-13."""

    def __init__(self, env="prod", key=None, live_orders=False):
        self.env, self.key = env, key
        self.live_orders = live_orders  # real-money orders to production: LiveBroker only
        self.host = HOSTS[env]["rest"]
        self.conn = None
        self.lock = threading.Lock()
        self.backoff_until = 0.0

    def _connect(self):
        self.conn = http.client.HTTPSConnection(self.host, timeout=10)

    def request(self, method, path, body=None, signed=False):
        """-> (json, rtt_ms). Raises RateLimited while backing off after a 429."""
        if method != "GET" and self.env != "demo" and not self.live_orders:
            raise PermissionError("orders to production need Rest(..., live_orders=True): the armed live mode only")
        if time.time() < self.backoff_until:
            raise RateLimited(f"backing off {self.backoff_until - time.time():.1f}s")
        full = PREFIX + path
        headers = {"Accept": "application/json", "User-Agent": "pm-hf"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if signed:
            headers.update(auth_headers(self.key, method, full))
        payload = json.dumps(body) if body is not None else None
        with self.lock:
            for attempt in (1, 2):
                if self.conn is None:
                    self._connect()
                t0 = time.perf_counter()
                try:
                    self.conn.request(method, full, body=payload, headers=headers)
                    resp = self.conn.getresponse()
                    data = resp.read()
                except (http.client.HTTPException, OSError):
                    self.conn.close()
                    self.conn = None
                    if attempt == 2:
                        raise
                    continue
                rtt = (time.perf_counter() - t0) * 1000
                break
        if resp.status == 429:
            wait = float(resp.getheader("Retry-After") or 2)
            self.backoff_until = time.time() + wait
            raise RateLimited(f"429 from {self.env}; backing off {wait:.0f}s")
        if resp.status >= 400:
            raise RuntimeError(f"{method} {path} -> HTTP {resp.status}: {data[:300].decode(errors='replace')}")
        return (json.loads(data) if data else {}), rtt


def orderbook_top(ob):
    """Kalshi books list YES bids and NO bids. Buying YES lifts the best NO bid at 1 - price."""
    yes = [(float(p), float(s)) for p, s in (ob.get("yes_dollars") or ob.get("yes_dollars_fp") or [])]
    no = [(float(p), float(s)) for p, s in (ob.get("no_dollars") or ob.get("no_dollars_fp") or [])]
    yb, nb = (max(yes) if yes else None), (max(no) if no else None)
    return {"yes_bid": yb[0] if yb else None, "no_bid": nb[0] if nb else None,
            "yes_ask": round(1 - nb[0], 4) if nb else None, "yes_ask_sz": nb[1] if nb else 0,
            "no_ask": round(1 - yb[0], 4) if yb else None, "no_ask_sz": yb[1] if yb else 0}


def order_fee(price, count, rate=0.07):
    """Kalshi's taker fee for ONE order: rate * C * P * (1 - P), rounded UP to the next cent.
    Per-order rounding is what makes tiny orders expensive: 1 contract at 0.40 pays 2c, not 1.68c.
    The inner round() stops float noise (0.0700000001) from rounding a whole cent up."""
    return math.ceil(round(rate * count * price * (1 - price) * 100, 6)) / 100


def place_ioc(rest, ticker, side, price, count):
    """Buy `count` whole contracts of `side` ('yes'/'no') at up to `price`, immediate-or-cancel.

    Create Order (V2), docs.kalshi.com read 2026-09-15: POST /portfolio/events/orders, quotes the
    YES book only ("side": bid/ask; count "10.00"; price "0.5600"). Buying YES is a bid at the YES
    price; buying NO is an ask on YES at 1 - price ("selling YES is economically equivalent to
    buying NO at 1 - price"). Response: fill_count, remaining_count, average_fill_price (YES-book
    price), average_fee_paid (per contract), order_id.
    """
    yes_side, yes_price = ("bid", price) if side == "yes" else ("ask", round(1 - price, 4))
    body = {"ticker": ticker, "side": yes_side, "count": f"{int(count)}.00", "price": f"{yes_price:.4f}",
            "time_in_force": "immediate_or_cancel", "self_trade_prevention_type": "taker_at_cross",
            "client_order_id": str(uuid.uuid4())}
    return rest.request("POST", "/portfolio/events/orders", body=body, signed=True)


def _selftest_demo():
    try:
        key = load_key("demo")
    except KeyMissing as e:
        print("demo key not configured:", e)
        return 1
    rest = Rest("demo", key)
    bal, rtt = rest.request("GET", "/portfolio/balance", signed=True)
    print(f"auth OK   balance response keys {sorted(bal)}   {rtt:.0f} ms")
    ms, _ = rest.request("GET", "/markets?series_ticker=KXBTC15M&status=open&limit=5")
    ticker = sorted(ms["markets"], key=lambda m: m["close_time"])[0]["ticker"]
    for i in range(3):  # a warm connection is what a trading loop would have
        _, rtt = rest.request("GET", f"/markets/{ticker}/orderbook?depth=1")
        print(f"book RTT (kept-alive) {rtt:.0f} ms")
    # 1 contract, YES, at 1 cent, IOC: rests nowhere and fills only if someone sells YES at 1c
    resp, rtt = place_ioc(rest, ticker, "yes", 0.01, 1)
    print(f"IOC order ack {rtt:.0f} ms   fill_count {resp.get('fill_count')}   remaining {resp.get('remaining_count')}")
    return 0


def _readcheck_prod():
    try:
        key = load_key("prod")
    except KeyMissing as e:
        print("prod key not configured:", e)
        return 1
    rest = Rest("prod", key)
    _, rtt = rest.request("GET", "/portfolio/balance", signed=True)
    print(f"prod auth OK (read-only use)   {rtt:.0f} ms")
    return 0


def _ordercheck_prod():
    """Trading key, production: prove auth + the order format without trading.

    Two IOC orders at 1c — buy YES at 0.01 and buy NO at 0.01 — on a market where BOTH asks are at
    least 10c, so neither can fill (IOC never rests). An accepted order with fill_count 0 proves the
    signature, permissions and body format; the V2 side mapping for NO is exercised too."""
    try:
        key = load_key("trade")
    except KeyMissing as e:
        print("trading key not configured:", e)
        return 1
    rest = Rest("prod", key, live_orders=True)
    bal, rtt = rest.request("GET", "/portfolio/balance", signed=True)
    print(f"auth OK   balance ${bal.get('balance', 0) / 100:.2f}   {rtt:.0f} ms")
    ms, _ = rest.request("GET", "/markets?series_ticker=KXBTC15M&status=open&limit=10")
    now = time.time()
    for m in sorted(ms["markets"], key=lambda m: m["close_time"]):
        ob, _ = rest.request("GET", f"/markets/{m['ticker']}/orderbook?depth=1")
        top = orderbook_top(ob.get("orderbook_fp") or ob.get("orderbook") or {})
        if (top["yes_ask"] or 0) >= 0.10 and (top["no_ask"] or 0) >= 0.10:
            ticker = m["ticker"]
            break
    else:
        print("no open market with both asks >= 10c right now; try again in a few minutes")
        return 1
    ok = True
    for side in ("yes", "no"):
        resp, rtt = place_ioc(rest, ticker, side, 0.01, 1)
        filled = float(resp.get("fill_count") or 0)
        ok &= filled == 0 and bool(resp.get("order_id"))
        print(f"{side.upper()} IOC @ 0.01 on {ticker}: accepted={bool(resp.get('order_id'))} "
              f"fill_count={resp.get('fill_count')} remaining={resp.get('remaining_count')}  ack {rtt:.0f} ms")
    print("ORDER CHECK PASSED" if ok else "ORDER CHECK FAILED: tell Claude these lines")
    return 0 if ok else 1


def _verify():
    ok = True

    def expect(label, got, want):
        nonlocal ok
        good = got == want
        ok &= good
        print(f"  {'PASS' if good else 'FAIL'}  {label:<58} {got!r}")

    expect("fee: 1 contract at 0.40 rounds 1.68c up to 2c", order_fee(0.40, 1), 0.02)
    expect("fee: 100 contracts at 0.50 = exactly $1.75", order_fee(0.50, 100), 1.75)
    expect("fee: 2 contracts at 0.02 = 0.27c -> 1c", order_fee(0.02, 2), 0.01)
    expect("fee: 10 contracts at 0.10 -> 6.3c -> 7c", order_fee(0.10, 10), 0.07)
    expect("fee: an exact cent does not round up (20 @ 0.50 = 35c)", order_fee(0.50, 20), 0.35)
    for label, rest in (("read-only prod Rest refuses POST", Rest("prod")),):
        try:
            rest.request("POST", "/portfolio/events/orders", body={})
            expect(label, "sent", "refused")
        except PermissionError:
            expect(label, "refused", "refused")
    sent = []

    class FakeRest:
        def request(self, method, path, body=None, signed=False):
            sent.append(body)
            return {}, 1.0
    place_ioc(FakeRest(), "T", "no", 0.37, 3)
    expect("buy NO @ 0.37 -> ask on YES @ 0.6300, count 3.00",
           (sent[-1]["side"], sent[-1]["price"], sent[-1]["count"]), ("ask", "0.6300", "3.00"))
    place_ioc(FakeRest(), "T", "yes", 0.05, 20)
    expect("buy YES @ 0.05 -> bid @ 0.0500 IOC",
           (sent[-1]["side"], sent[-1]["price"], sent[-1]["time_in_force"]), ("bid", "0.0500", "immediate_or_cancel"))
    print("kalshi_client verify:", "ALL PASS" if ok else "FAILED")
    return ok


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "verify":
        raise SystemExit(0 if _verify() else 1)
    if cmd == "prod-order-check":
        raise SystemExit(_ordercheck_prod())
    if cmd == "demo-selftest":
        raise SystemExit(_selftest_demo())
    if cmd == "prod-readcheck":
        raise SystemExit(_readcheck_prod())
    raise SystemExit(__doc__)
