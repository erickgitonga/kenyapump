# KenyaPump — Multi-Chain Memecoin Detection System

A modular, async-first infrastructure for detecting and filtering newly
launched memecoins across Ethereum and Base. Real-time detection via
Alchemy WebSocket, two-layer adversarial screening, and phone alerts for
high-signal events.

**Status:** detection, screening, and alerting layers are functional and
running 24/7 against live data. Execution and risk management are not yet
built — see [Roadmap](#roadmap).

---

## What this does today

- **Real-time detection** of new DEX pools on Ethereum and Base via
  Alchemy WebSocket — Uniswap V2, Uniswap V3, and Aerodrome Slipstream
  factories. ~7ms from chain event to log line.
- **Enrichment** with live price/liquidity from DexScreener, with a
  20-minute adaptive retry window for pairs not yet indexed.
- **Two-layer adversarial screening:**
  - Layer 1: `QuickScreen` — reads the first 60 seconds of `Transfer`
    events directly from chain. Detects bundle launches, deployer
    concentration, and thin holder bases in 2-3 seconds.
  - Layer 2: `HoneypotDetector` — combines GoPlus Security's free API
    with holder concentration data for a 0-100 risk score.
- **Deep holder analysis** (`data/holders.py`) — full historical
  `Transfer` scan for tokens that survive the quick screen, using
  parallel chunked `eth_getLogs` calls within Alchemy free-tier limits.
- **Telegram alerts** — phone notifications for `[PASS]` and `[DANGER]`
  verdicts only. `[REJECT]` and `[WATCH]` stay in logs.
- **Watchdog** (`watchdog.py`) — monitors the bot process, restarts on
  crash, sends Telegram alert on state change.
- **Persistent state** in SQLite — survives restarts without re-processing.

None of this executes trades. `BOT_DRY_RUN=true` by default, and nothing
in this codebase currently calls the transaction-signing path.

---

## Key finding

Over a 24-hour live sample of Base Uniswap V2 launches, **every qualified
token showed 3-6 holders with 82-99% concentration in the first 60
seconds.** Zero showed distributed holder bases. This pattern is
consistent with machine-operated adversarial launches dominating the
cheap-factory venue.

The `QuickScreen` filter catches this in 2-3 seconds — before DexScreener
or GoPlus have indexed the pair — by reading chain state directly.

---

## Architecture (updated)

core/
  models.py            Chain-agnostic data classes
  exceptions.py         Retryable vs. fatal vs. risk-policy errors
  chain_interface.py    BaseChainAdapter — contract every adapter implements
  chain_router.py        Registry across chain adapters

chains/
  ethereum.py            EthereumAdapter(BaseChainAdapter) — web3.py-based
  base.py                 BaseAdapter(EthereumAdapter) — inherits EVM logic,
                          overrides chain identity and factory addresses

config/
  settings.py             Env-driven config for every module below

data/
  dexscreener.py           Free client: pool discovery, price, liquidity
  geckoterminal.py          Free client: OHLCV candle history
  holders.py                 On-chain holder concentration from raw Transfer events
  scraper.py                  TokenScraper — chain-aware WebSocket detection,
                              adaptive chunking around Alchemy's 10-block limit
  persistence.py               SQLite scan state + discovered-pair history

ai/
  quick_screen.py          Layer 1: 2-second adversarial launch filter
  honeypot.py               Layer 2: GoPlus + holder concentration risk score

notifications/
  telegram.py               Async Telegram alerting (fire-and-forget sink)

watchdog.py                 Process monitor + auto-restart + death alerts
main.py                     Wires everything together

---

## Roadmap (updated)

Built:
- [x] Chain abstraction layer (Ethereum + Base)
- [x] Free live data layer (DexScreener, GeckoTerminal)
- [x] New-pair detection (polling + WebSocket, with reconnection/backoff)
- [x] Two-layer adversarial screening (quick screen + honeypot)
- [x] On-chain holder concentration analysis
- [x] Persistent state across restarts
- [x] Telegram alerting
- [x] Process watchdog with auto-restart

Not yet built:
- [ ] Outcome tracker — re-scan detected tokens at T+6h / T+24h
- [ ] Reputation DB — persistent adversarial memory across launches
- [ ] Funding graph — trace deployer funding chains
- [ ] Solana adapter — where 85% of memecoin volume is
- [ ] Risk management engine (position sizing, exposure limits)
- [ ] Execution engine (order scheduling, MEV protection)
