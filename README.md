# Multi-Chain Memecoin Bot — Module 1: Chain Abstraction Layer

## What's here

```
core/
  models.py           # Chain-agnostic data classes (TokenInfo, SwapQuote, GasEstimate, ...)
  exceptions.py        # Exception hierarchy (retryable vs. fatal vs. risk-policy)
  chain_interface.py   # BaseChainAdapter — the contract every chain must implement
  chain_router.py       # Registry across adapters; where "auto-switch chains" will live
chains/
  ethereum.py           # EthereumAdapter(BaseChainAdapter) using web3.py
config/
  settings.py           # Env-driven config (RPC endpoints, risk defaults, dry-run flag)
utils/
  rpc_pool.py           # Multi-endpoint failover/circuit-breaker for RPC calls
```

## Design decisions worth knowing before you build on top of this

1. **Everything is async.** A trading bot spends most of its time waiting on
   RPC calls; sync code here would serialize work that should run concurrently
   (watching multiple pairs, polling multiple chains).

2. **`BaseChainAdapter` is the only thing upstream code should depend on.**
   Risk management, strategy, and execution scheduling should never import
   `web3` or any chain SDK directly — only `core.chain_interface`. That's what
   makes adding Base/Solana/Arbitrum later a matter of writing one new file,
   not touching everything else.

3. **`get_liquidity_info` is intentionally `NotImplementedError`.** Real pool
   discovery and USD pricing need an indexer (Dexscreener, The Graph, or your
   own) — scanning factory events live over RPC is too slow to trade on.
   Wire it as an injected dependency rather than faking numbers.

4. **`simulate_sell` is a first-pass filter, not a honeypot scanner.** It
   checks whether a router quote round-trips without reverting. For real
   protection you want a dedicated service (Honeypot.is, GoPlus Security) in
   the risk-management module — treat this as "catches the obvious cases
   fast," not "guarantees safety."

5. **`BOT_DRY_RUN=true` by default.** `sign_and_send_transaction` still signs
   and broadcasts if called — dry-run enforcement belongs in the execution
   engine (the module after risk management), which should check this flag
   before ever calling `sign_and_send_transaction`.

6. **Private keys never touch disk or logs in this code.** Source them from a
   secrets manager at runtime; `.env.example` documents this but the actual
   key handling is your responsibility at the deployment layer.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in real RPC URLs
```

```python
import asyncio
from config.settings import BotConfig
from chains.ethereum import EthereumAdapter
from core.chain_router import ChainRouter

async def main():
    config = BotConfig.from_env()
    eth = EthereumAdapter(config.ethereum)

    router = ChainRouter()
    router.register(eth)
    await router.connect_all()

    health = await router.health_check_all()
    print(health)

asyncio.run(main())
```

## What's NOT here yet (per your roadmap)

- Additional chain adapters (Base, Solana, Arbitrum, BSC)
- Opportunity scoring / `ChainRouter.best_chain_for()`
- Risk management engine (position sizing, exposure limits, kill switches)
- Real-time analysis / signal generation
- Execution engine (order scheduling, MEV protection, retry policy)
- Liquidity indexer integration
- Honeypot/contract-safety scanning service integration

Send over the next module spec and I'll build it against this same
`BaseChainAdapter` contract.
