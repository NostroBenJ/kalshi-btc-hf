# kalshi-btc-hf

Can a small, fast bot make money on Kalshi's 15-minute Bitcoin markets (`KXBTC15M`) by
reacting to BTC moves before the order book reprices?

This repo is the full measurement: multi-venue market-data recorders, a fair-value model,
backtests over 30 days of real fills, a paper/live trading bot with hard risk limits, and a
latency study run on a cloud server. **The answer was no.** The edge is real, but it belongs
to reaction times under ~100 ms on the exchange's own clock, and a US-based bot on public
feeds does not get there. The strategy was retired on the evidence, and the evidence is
all here.

> Research code. Nothing here is investment advice, and no result here claims an edge.

---

## What's in it

| Area | Files | What it does |
|---|---|---|
| **Pricing** | `fair_value.py` | Prices the contract as a digital option on a 60-second average of 1-second prints. Finite-difference delta/vega, GBM Monte Carlo cross-check. |
| **Market data** | `spot_feeds.py`, `kalshi_feed.py`, `kalshi_recorder.py`, `recorder.py` | Websocket recorders for Coinbase, Kraken, Bitstamp, Gemini, Binance/OKX/Bybit/Deribit perps, Chainlink (via Polymarket), and the Kalshi order book. Heap-backed L2 books, each venue in its own process, shared-memory handoff. |
| **Backtests** | `kalshi_backtest.py`, `kalshi_trigger.py`, `kalshi_ms.py`, `kalshi_exit.py`, `kalshi_band.py`, `kalshi_maker.py`, `kalshi_longshot.py`, `backtest.py` | Replays of 74.6M Kalshi taker fills (2,805 markets) and 13.8M Polymarket fills, at pre-registered information delays, with leak and staleness controls. |
| **Live latency** | `kalshi_live_replay.py`, `live_latency_curve.py` | Everything recorded on one server clock and replayed at the bot's measured round trip, so no cross-exchange clock skew can manufacture an edge. |
| **Bot** | `paper_bot.py`, `risk.py`, `kalshi_client.py`, `dash/index.html` | Paper, demo-exchange and capped live modes. RSA-PSS request signing, kept-alive connections, loop-lag overload guard, live dashboard. |
| **Weather** | `weather/weather_backtest.py` | Kalshi daily-high temperature markets vs free GFS forecasts, 7 cities. Also a negative result. |
| **Deploy** | `deploy/` | Ubuntu setup, systemd units, latency probe. |

## How results are judged

A backtest that only reports P&L can't tell a real edge from a bug. Every study here carries
**controls whose right answer is known in advance**:

- **Future-leak control.** Give the model BTC prices from 1-3 seconds *in the future*. It must
  show a large profit. If it doesn't, the detector is broken and nothing else can be read.
- **Staleness control.** Give it prices a few seconds *old*. If that makes as much as the live
  version, the "edge" is the model's own error, not speed.
- **Settlement rule check.** The market description said one thing; the data said another.
  The published rule was reverse-engineered and matched on 2,804 of 2,804 markets before any
  P&L was computed.
- **Clustered standard errors.** Fills inside one 15-minute market are not independent, so
  errors are clustered by market. Tests were pre-registered before looking at the result.

## The findings, in order

| Study | Result |
|---|---|
| Copy fills the model calls cheap (30 days) | D=0 s: **−1.04c** ±0.59 per contract. Takers as a group lose 1.20c after fees. |
| News trigger (fair value jumped, Kalshi lagged) | 0 ms: **+5.99c** ±0.96. **+100 ms: −1.72c** ±1.40. A cliff, not a slope. |
| Same test live, one clock, measured 24 ms round trip | Flat zero at every latency. Underpriced asks lived a median 1.1 s. |
| Leading feeds (Binance/OKX/Bybit/Deribit perps) | None leads Coinbase. All negative at the bot's latency. **Strategy closed.** |
| Price band analysis | 0.50-0.75 lost −5.28c ±1.54 (~3.5 SE). The original band had it backwards. |
| Market making instead | +0.23c gross with no maker fee; any maker fee erases it. |
| Favourite-longshot bias, all of Kalshi (761k trades) | No pre-registered test clears the bar. |
| Weather markets vs GFS | **−2.30c** per contract, t = −2.5. The market out-forecasts a day-old model. |

## Engineering notes worth reading

- **Float dust in order books.** Summing incremental deltas left ~4e-13 on emptied price
  levels, which looked like a crossed book. Levels at or below 1e-9 are now removed, and a
  REST snapshot comparison went from 0/10 to 14/15 matching.
- **"Missed 5 in a row" was the server, not the strategy.** A burstable cloud instance ran
  out of CPU credits: 40-54% steal, websocket ping timeouts, order latency 12 → 77 ms. The
  bot reads `/proc` steal so that shows up on the dashboard. Moving to a non-burstable
  instance gave steal 0.0%, event-loop lag p90 1.0 ms, and order round trip ~9.5 ms.
- **Paper circuit breakers log but don't halt.** A breaker that stops a paper bot for 20 hours
  also stops the data you needed to judge it. Blocked signals are stored as shadow trades and
  settled, so every risk rule shows what it cost.
- **No production order path by default.** `kalshi_client.Rest` raises on any non-GET to
  production. Live mode needs a separate trading key, an arming file, and hard caps: $1 per
  order, $2 per market, $5 per day, $10 lifetime. It stops itself on repeated order errors or
  a position mismatch with the exchange.

## Running it

Python 3.11+. The math and backtests use the standard library. Exchange connectivity needs
`cryptography` and `websockets`; `orjson` and `uvloop` are used when installed.

```bash
python fair_value.py          # pricing checks
python risk.py                # risk-rule checks
python kalshi_client.py verify
python test_live_broker.py
python spot_feeds.py          # order-book checks
python paper_bot.py --broker paper   # dashboard on http://127.0.0.1:8765
```

API keys are read only from environment variables (`KALSHI_KEY_ID` / `KALSHI_KEY_PATH`,
demo and trading variants). None are included. Market data isn't included either; the
`*_history.py` scripts pull it from public endpoints.

---

Benjamin Nostro
