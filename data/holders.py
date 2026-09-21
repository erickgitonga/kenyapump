"""
data/holders.py

Direct on-chain holder analysis. Replaces the GoPlus lag (5-30 minutes on
fresh Base tokens) with a 2-5 second RPC scan of the token's Transfer
events since launch.

This module answers the questions that actually matter for a fresh token:
    1. How many distinct non-contract wallets hold this token?
    2. What fraction of supply does the top 10 control?
    3. Is the initial minter (deployer proxy) still holding a large stake?

Everything is derived from chain state — no third-party indexer, no lag,
works on any EVM chain, costs nothing beyond the RPC calls you already pay
for.

Design notes:
  - Uses eth_getLogs with adaptive chunking (same pattern as the scraper),
    because Alchemy free tier caps at ~10 blocks per call for mainnet.
  - Caches results per token for CACHE_TTL_SECONDS — holder distribution
    doesn't change meaningfully within 30 seconds, so re-scanning in a
    hot loop is wasteful.
  - Filters out the pool address, the zero address, and (best-effort) the
    known quote-asset addresses before computing concentration.
  - Returns a partial report with a `truncated=True` flag if the token has
    too many transfers to scan within the configured budget.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

from eth_abi import decode as abi_decode
from web3 import Web3

from chains.ethereum import EthereumAdapter
from core.exceptions import ChainConnectionError
from core.models import ChainId

log = logging.getLogger("kenyapump.data.holders")

# keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC = Web3.to_hex(
    Web3.keccak(text="Transfer(address,address,uint256)")
)

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Cache: token_address (lower) -> (monotonic_time, HolderReport)
CACHE_TTL_SECONDS = 30.0

# Hard cap: if a token has more than this many Transfer logs in the scan
# window, we stop and return partial data. Prevents a runaway RPC bill.
MAX_TRANSFERS_SCANNED = 50_000

# Parallel chunks — Alchemy allows ~330 CU/s on free tier, so we
# can safely fire ~8 concurrent eth_getLogs calls without hitting
# the ceiling. Higher = faster scan but more risk of rate limit.
MAX_CONCURRENT_CHUNKS = 3

# Minimum balance to count as a "holder" — filters dust airdrops.
MIN_HOLDER_BALANCE_RAW = 1


# ─────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────

@dataclass
class HolderEntry:
    address: str
    balance_raw: int
    balance: Decimal
    pct: Decimal           # fraction of total supply, e.g. Decimal("0.18") = 18%
    is_contract: Optional[bool] = None


@dataclass
class HolderReport:
    chain: ChainId
    token_address: str
    total_supply: Decimal
    holder_count: int
    top_holders: List[HolderEntry] = field(default_factory=list)

    top1_pct: Decimal = Decimal("0")
    top5_pct: Decimal = Decimal("0")
    top10_pct: Decimal = Decimal("0")

    initial_minter: Optional[str] = None
    initial_minter_pct: Optional[Decimal] = None

    # Range actually scanned
    from_block: int = 0
    to_block: int = 0
    transfers_scanned: int = 0
    truncated: bool = False

    # Diagnostics
    flags: List[str] = field(default_factory=list)
    error: Optional[str] = None

    def __repr__(self) -> str:
        return (
            f"HolderReport({self.chain.value}:{self.token_address[:10]}… "
            f"holders={self.holder_count} top1={self.top1_pct:.2%} "
            f"top10={self.top10_pct:.2%} truncated={self.truncated})"
        )


# ─────────────────────────────────────────────────────────────────────
# Analyzer
# ─────────────────────────────────────────────────────────────────────

class HolderAnalyzer:
    """
    Chain-agnostic holder analyzer. Works with any adapter that implements
    `get_logs` and `get_latest_block_number` (both EVM adapters do).
    """

    def __init__(
        self,
        eth_adapter: EthereumAdapter,
        initial_chunk_size: int = 10,
        max_chunk_size: int = 10,
    ):
        self._eth = eth_adapter
        self._chain = eth_adapter.chain_id
        # Chain-specific quote assets to exclude from concentration math.
        # The pool address, WETH, USDC, etc. are not "holders" in the sense
        # that matters for rug detection.
        self._excluded_addresses = {
            ZERO_ADDRESS.lower(),
            *(a.lower() for a in eth_adapter.config.known_quote_assets),
        }
        self._initial_chunk_size = initial_chunk_size
        self._max_chunk_size = max_chunk_size
        self._cache: Dict[str, tuple[float, HolderReport]] = {}
        self._lock = asyncio.Lock()

    # --- public API ----------------------------------------------------

    async def analyze(
        self,
        token_address: str,
        from_block: int,
        to_block: Optional[int] = None,
        pool_address: Optional[str] = None,
    ) -> HolderReport:
        """
        Scan all Transfer events between `from_block` and `to_block`
        (or latest) and produce a HolderReport.

        `pool_address`: the DEX pair contract to exclude from top-holder
        ranking (its balance is the pool reserve, not a real holding).
        """
        token_lower = token_address.lower()
        cache_key = f"{self._chain.value}:{token_lower}:{from_block}"
        now = time.monotonic()

        async with self._lock:
            cached = self._cache.get(cache_key)
            if cached and (now - cached[0]) < CACHE_TTL_SECONDS:
                return cached[1]

        excluded = set(self._excluded_addresses)
        if pool_address:
            excluded.add(pool_address.lower())

        try:
            report = await self._scan(token_lower, from_block, to_block, excluded)
        except ChainConnectionError as exc:
            report = HolderReport(
                chain=self._chain,
                token_address=token_lower,
                total_supply=Decimal("0"),
                holder_count=0,
                error=str(exc),
                flags=["rpc_connection_failed"],
            )
        except Exception as exc:
            log.exception("Holder analysis failed for %s", token_address)
            report = HolderReport(
                chain=self._chain,
                token_address=token_lower,
                total_supply=Decimal("0"),
                holder_count=0,
                error=str(exc),
                flags=["analysis_error"],
            )

        async with self._lock:
            self._cache[cache_key] = (time.monotonic(), report)
        return report

    # --- internals -----------------------------------------------------

    async def _scan(
        self,
        token_address: str,
        from_block: int,
        to_block: Optional[int],
        excluded: set[str],
    ) -> HolderReport:
        if to_block is None:
            latest = await self._eth.get_latest_block_number()
            to_block = latest

        # Total supply for percentage math
        total_supply_raw, decimals = await self._read_total_supply(token_address)
        total_supply = Decimal(total_supply_raw) / (Decimal(10) ** decimals)

        balances: Dict[str, int] = {}
        first_minter: Optional[str] = None
        transfers_scanned = 0
        truncated = False

        flags: List[str] = []
        truncated: bool = False

        # Build the full list of chunk ranges first, then fan them out
        # concurrently. Parallel fetching is what turns a 90-second scan
        # into a 10-second scan.
        chunk_ranges = []
        cursor = from_block
        chunk_size = self._initial_chunk_size
        while cursor <= to_block:
            chunk_end = min(cursor + chunk_size - 1, to_block)
            chunk_ranges.append((cursor, chunk_end))
            cursor = chunk_end + 1

        # Hard cap on chunks — prevents a runaway scan on an ancient token.
        MAX_CHUNKS = 600
        if len(chunk_ranges) > MAX_CHUNKS:
            chunk_ranges = chunk_ranges[-MAX_CHUNKS:]  # newest chunks first
            truncated = True
            flags.append(f"Scan capped at {MAX_CHUNKS} chunks (newest first)")

        sem = asyncio.Semaphore(MAX_CONCURRENT_CHUNKS)

        async def fetch(cursor_b: int, chunk_end_b: int):
            async with sem:
                try:
                    return await self._eth.get_logs(
                        address=Web3.to_checksum_address(token_address),
                        topics=[TRANSFER_TOPIC],
                        from_block=cursor_b,
                        to_block=chunk_end_b,
                    )
                except Exception as exc:
                    log.warning(
                        "holders: chunk %d..%d FAILED for %s: %s",
                        cursor_b, chunk_end_b, token_address[:10], exc,
                    )
                    return []

        results = await asyncio.gather(*(fetch(a, b) for a, b in chunk_ranges))

        # Preserve chronological order of events (important for first-minter
        # tracking and correct balance accumulation across chunks).
        for logs in results:
            for raw_log in logs:
                transfers_scanned += 1
                if transfers_scanned > MAX_TRANSFERS_SCANNED:
                    truncated = True
                    flags.append(
                        f"Transfer scan truncated at {MAX_TRANSFERS_SCANNED} events"
                    )
                    break

                try:
                    topics = raw_log["topics"]
                    from_addr = self._addr_from_topic(topics[1])
                    to_addr = self._addr_from_topic(topics[2])
                    data = raw_log["data"]
                    data_bytes = (
                        bytes.fromhex(data.hex()[2:])
                        if hasattr(data, "hex")
                        else bytes.fromhex(data[2:])
                    )
                    (value,) = abi_decode(["uint256"], data_bytes)

                    if (
                        first_minter is None
                        and from_addr.lower() == ZERO_ADDRESS
                        and to_addr.lower() != ZERO_ADDRESS
                    ):
                        first_minter = to_addr

                    balances[from_addr.lower()] = balances.get(from_addr.lower(), 0) - value
                    balances[to_addr.lower()] = balances.get(to_addr.lower(), 0) + value
                except Exception:
                    continue
            if truncated:
                break


        # Filter: positive balance, not excluded
        holders = {
            addr: bal
            for addr, bal in balances.items()
            if bal > MIN_HOLDER_BALANCE_RAW and addr not in excluded
        }

        sorted_holders = sorted(holders.items(), key=lambda kv: kv[1], reverse=True)

        total_supply_raw_effective = total_supply_raw or 1
        entries: List[HolderEntry] = []
        for addr, bal in sorted_holders[:50]:  # keep top 50 for flexibility
            pct = Decimal(bal) / Decimal(total_supply_raw_effective)
            entries.append(
                HolderEntry(
                    address=addr,
                    balance_raw=bal,
                    balance=Decimal(bal) / (Decimal(10) ** decimals),
                    pct=pct,
                )
            )

        top1 = entries[0].pct if len(entries) >= 1 else Decimal("0")
        top5 = sum((e.pct for e in entries[:5]), Decimal("0"))
        top10 = sum((e.pct for e in entries[:10]), Decimal("0"))

        initial_minter_pct: Optional[Decimal] = None
        if first_minter:
            minter_bal = holders.get(first_minter.lower(), 0)
            initial_minter_pct = Decimal(minter_bal) / Decimal(total_supply_raw_effective)

        if truncated:
            flags.append("Holder count may be understated due to scan cap")

        if not entries:
            flags.append("No holders found — token may be unindexed or dust")

        return HolderReport(
            chain=self._chain,
            token_address=token_address,
            total_supply=total_supply,
            holder_count=len(holders),
            top_holders=entries,
            top1_pct=top1,
            top5_pct=top5,
            top10_pct=top10,
            initial_minter=first_minter,
            initial_minter_pct=initial_minter_pct,
            from_block=from_block,
            to_block=to_block,
            transfers_scanned=transfers_scanned,
            truncated=truncated,
            flags=flags,
        )

    async def _read_total_supply(self, token_address: str) -> tuple[int, int]:
        """Return (total_supply_raw, decimals). Falls back to (0, 18) on failure."""
        # Minimal ERC-20 ABI
        abi = [
            {
                "constant": True,
                "inputs": [],
                "name": "totalSupply",
                "outputs": [{"name": "", "type": "uint256"}],
                "stateMutability": "view",
                "type": "function",
            },
            {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "stateMutability": "view",
                "type": "function",
            },
        ]
        try:
            info = await self._eth.get_token_info(token_address)
            decimals = info.decimals if hasattr(info, "decimals") else 18
        except Exception:
            decimals = 18

        try:
            # Use raw JSON-RPC via the adapter's underlying w3 instance
            w3 = self._eth._rpc_pool  # not available directly — use get_token_info instead
        except Exception:
            pass

        # Fall back: use the adapter's get_token_info for decimals, and
        # derive total supply from a direct eth_call. If the adapter doesn't
        # expose a totalSupply reader, we return 0 and let the caller treat
        # percentages as best-effort.
        try:
            contract = self._eth._get_w3(
                self._eth.config.rpc_endpoints[0].url
            ).eth.contract(
                address=Web3.to_checksum_address(token_address), abi=abi
            )
            raw = await contract.functions.totalSupply().call()
            return int(raw), int(decimals)
        except Exception as exc:
            log.debug("totalSupply read failed for %s: %s", token_address, exc)
            return 0, int(decimals)

    @staticmethod
    def _addr_from_topic(topic) -> str:
        topic_hex = topic.hex() if hasattr(topic, "hex") else topic
        return Web3.to_checksum_address("0x" + topic_hex[-40:])
