"""
data/solana_holders.py

Solana holder analysis via getTokenLargestAccounts + getAccountInfo.
Replaces the EVM Transfer-log scan for SPL tokens — Solana doesn't
have Transfer events the same way EVM does.

Two RPC calls per holder:
  1. getTokenLargestAccounts(mint) → top 20 token accounts
  2. getAccountInfo(token_account, jsonParsed) → owner wallet

Cost: ~6 Helius credits per token (1 list + 5 owner resolutions).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

import aiohttp

log = logging.getLogger("kenyapump.data.solana_holders")

RPC_TIMEOUT_SECONDS = 15.0


@dataclass
class SolanaHolder:
    wallet: str
    token_account: str
    balance_ui: float
    pct: float


async def _rpc(session: aiohttp.ClientSession, rpc_url: str, method: str, params: list) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with session.post(rpc_url, json=payload) as resp:
        return await resp.json()


async def get_solana_top_holders(
    rpc_url: str,
    mint_address: str,
    top_n: int = 5,
    timeout: float = RPC_TIMEOUT_SECONDS,
    exclude: Optional[List[str]] = None,
) -> List[SolanaHolder]:
    """Return top N holders with resolved wallet owners."""
    holders: List[SolanaHolder] = []
    timeout_obj = aiohttp.ClientTimeout(total=timeout)

    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        # 1. Get largest token accounts
        try:
            largest = await _rpc(session, rpc_url, "getTokenLargestAccounts", [mint_address])
        except Exception as exc:
            log.warning("getTokenLargestAccounts failed for %s: %s", mint_address[:12], exc)
            return []

        accounts = largest.get("result", {}).get("value", [])
        if not accounts:
            return []

        # 2. Get total supply for percentage math
        total_supply = 1
        try:
            supply_resp = await _rpc(session, rpc_url, "getTokenSupply", [mint_address])
            supply_value = supply_resp.get("result", {}).get("value", {})
            total_supply = int(supply_value.get("amount", 0)) or 1
        except Exception as exc:
            log.debug("getTokenSupply failed: %s", exc)

        # 3. Resolve each token account to its wallet owner
        exclude_set = {a.lower() for a in (exclude or []) if a}

        for acc in accounts:
            if len(holders) >= top_n:
                break

            token_account = acc.get("address")
            amount_raw = int(acc.get("amount", 0))
            ui_amount = float(acc.get("uiAmount") or 0)

            owner = None
            try:
                info = await _rpc(
                    session, rpc_url, "getAccountInfo",
                    [token_account, {"encoding": "jsonParsed"}],
                )
                parsed = info.get("result", {}).get("value") or {}
                data = parsed.get("data", {})
                if isinstance(data, dict):
                    owner = data.get("parsed", {}).get("info", {}).get("owner")
            except Exception as exc:
                log.debug("getAccountInfo failed for %s: %s", token_account, exc)
                continue

            if not owner:
                continue
            if owner.lower() in exclude_set:
                continue

            pct = amount_raw / total_supply if total_supply else 0.0
            holders.append(SolanaHolder(
                wallet=owner,
                token_account=token_account,
                balance_ui=ui_amount,
                pct=pct,
            ))

    return holders
