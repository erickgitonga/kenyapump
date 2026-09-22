"""
data/wallet_tracker.py

Wallet tracking utilities for memecoin analysis. Reuses HolderAnalyzer
from data/holders.py to scan on-chain Transfer events and identify
top holders by balance.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List

from data.holders import HolderAnalyzer, HolderEntry


@dataclass
class HolderWallet:
    wallet: str
    balance: Decimal
    pct: Decimal
    is_contract: bool


async def get_top_holders(
    adapter,
    token_address: str,
    launch_block: int,
    top_n: int = 20,
) -> List[HolderWallet]:
    """
    Return the top `top_n` holders for `token_address` on `chain`.
    
    Reuses HolderAnalyzer to scan on-chain Transfer events and
    converts the results into HolderWallet dataclasses.
    
    Raises NotImplementedError for Solana (not yet supported).
    """
    if adapter.chain_id.value == "solana":
        raise NotImplementedError("Solana wallet tracking is not implemented yet")
    
    analyzer = HolderAnalyzer(adapter)
    report = await analyzer.analyze(token_address, from_block=launch_block)
    
    # Convert HolderEntry objects to HolderWallet dataclasses
    wallets: List[HolderWallet] = []
    for entry in report.top_holders[:top_n]:
        wallets.append(HolderWallet(
            wallet=entry.address,
            balance=entry.balance,
            pct=entry.pct,
            is_contract=entry.is_contract or False
        ))
    
    return wallets
