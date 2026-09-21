"""
core/chain_router.py

Registry + router sitting above the adapters. With only Ethereum registered
today, this looks like overkill — but it's the seam where "automatic chain
switching based on opportunity" plugs in once Base/Solana/Arbitrum adapters
exist: the strategy layer calls `router.best_chain_for(...)`, never a
specific adapter directly.

Deliberately NOT implementing opportunity-scoring logic yet since that
depends on the risk-management and analysis modules you're adding next —
this just gives them a stable interface to build on.
"""

from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

from core.chain_interface import BaseChainAdapter
from core.models import ChainId, ChainHealthStatus
from core.exceptions import ChainNotSupportedError


class ChainRouter:
    def __init__(self) -> None:
        self._adapters: Dict[ChainId, BaseChainAdapter] = {}

    def register(self, adapter: BaseChainAdapter) -> None:
        self._adapters[adapter.chain_id] = adapter

    def get(self, chain_id: ChainId) -> BaseChainAdapter:
        try:
            return self._adapters[chain_id]
        except KeyError as exc:
            raise ChainNotSupportedError(
                f"No adapter registered for {chain_id}. "
                f"Registered chains: {list(self._adapters.keys())}"
            ) from exc

    def registered_chains(self) -> List[ChainId]:
        return list(self._adapters.keys())

    async def connect_all(self) -> None:
        await asyncio.gather(*(a.connect() for a in self._adapters.values()))

    async def disconnect_all(self) -> None:
        await asyncio.gather(*(a.disconnect() for a in self._adapters.values()))

    async def health_check_all(self) -> Dict[ChainId, ChainHealthStatus]:
        results = await asyncio.gather(
            *(a.health_check() for a in self._adapters.values())
        )
        return dict(zip(self._adapters.keys(), results))

    async def get_healthy_chains(self) -> List[ChainId]:
        statuses = await self.health_check_all()
        return [chain for chain, status in statuses.items() if status.is_healthy]

    # --- extension point --------------------------------------------------
    #
    # def best_chain_for(self, opportunity: "Opportunity") -> ChainId:
    #     """
    #     Placeholder for opportunity-based routing. Will need, at minimum:
    #       - which chains the token/pair actually exists on
    #       - current gas cost on each candidate chain vs. position size
    #       - chain health (skip degraded/unhealthy chains)
    #       - relative execution speed needed (block time vs. opportunity decay)
    #     Wire this up once the analysis module can produce an Opportunity
    #     with cross-chain liquidity data attached.
    #     """
    #     raise NotImplementedError
