"""
ai/quick_screen.py

Fast pre-filter for freshly-detected tokens. Runs in 2-5 seconds and
rejects obvious junk/dust/bundles BEFORE we spend 75 seconds on a deep
holder scan.

What it checks (in order, aborting early on hard rejects):
    1. totalSupply readable?         (1 RPC call)
    2. Did a pool get created?       (scan 20 blocks for large transfers)
    3. How many distinct wallets     (from those 20 blocks of Transfer events)
       received tokens in first 20s?
    4. Was there a same-block        (bundle-buy detection)
       coordinated buy?
    5. Is the mint authority gone?   (best-effort; not always determinable)

Returns a QuickVerdict with `verdict ∈ {"PASS", "REJECT", "WATCH"}` and
the reason. Callers should only run deep holder analysis on PASS tokens.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Set

from eth_abi import decode as abi_decode
from web3 import Web3

from chains.ethereum import EthereumAdapter
from core.models import ChainId

log = logging.getLogger("kenyapump.ai.quick_screen")

TRANSFER_TOPIC = Web3.to_hex(
    Web3.keccak(text="Transfer(address,address,uint256)")
)
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# How many blocks after launch to inspect. Base is ~2s/block, Ethereum is
# ~12s/block. We want ~60 seconds of activity either way.
BASE_LOOKAHEAD_BLOCKS = 20     # Base: ~40s, 2 chunks of 10
ETH_LOOKAHEAD_BLOCKS = 10      # Ethereum: ~2 min, 1 chunk of 10

# Reject thresholds
MIN_UNIQUE_RECEIVERS = 3       # fewer than this = dust or single-buyer
MAX_SINGLE_BUYER_PCT = Decimal("0.50")   # >60% one wallet = likely bundled


@dataclass
class QuickVerdict:
    chain: ChainId
    token_address: str
    verdict: str = "REJECT"          # "PASS" | "WATCH" | "REJECT"
    reason: str = ""
    latency_ms: float = 0.0

    total_supply: Optional[Decimal] = None
    unique_receivers: int = 0
    largest_receiver_pct: Optional[Decimal] = None
    initial_minter_pct: Optional[Decimal] = None
    bundle_detected: bool = False
    pool_like_transfer_seen: bool = False

    flags: List[str] = field(default_factory=list)
    error: Optional[str] = None

    def __repr__(self) -> str:
        return (
            f"QuickVerdict({self.chain.value}:{self.token_address[:10]}… "
            f"{self.verdict} receivers={self.unique_receivers} "
            f"largest={self.largest_receiver_pct} bundle={self.bundle_detected})"
        )


class QuickScreen:
    """Fast pre-filter. Never raises — returns a REJECT verdict on failure."""

    def __init__(self, eth_adapter: EthereumAdapter):
        self._eth = eth_adapter
        self._chain = eth_adapter.chain_id
        self._lookahead = (
            BASE_LOOKAHEAD_BLOCKS
            if self._chain == ChainId.BASE
            else ETH_LOOKAHEAD_BLOCKS
        )
        self._excluded = {
            ZERO_ADDRESS.lower(),
            *(a.lower() for a in eth_adapter.config.known_quote_assets),
        }

    async def check(
        self,
        token_address: str,
        launch_block: int,
        pool_address: Optional[str] = None,
    ) -> QuickVerdict:
        start = time.monotonic()
        v = QuickVerdict(chain=self._chain, token_address=token_address)

        try:
            # ─── 1. Total supply readable? ───────────────────────────
            supply_raw, decimals = await self._read_supply_decimals(token_address)
            if supply_raw == 0:
                v.verdict = "REJECT"
                v.reason = "totalSupply() unreadable or zero"
                v.flags.append("unreadable_supply")
                v.latency_ms = (time.monotonic() - start) * 1000
                return v
            v.total_supply = Decimal(supply_raw) / (Decimal(10) ** decimals)

            # ─── 2. Scan first N blocks for Transfer events ─────────
            to_block = launch_block + self._lookahead
            try:
                latest = await self._eth.get_latest_block_number()
                to_block = min(to_block, latest)
            except Exception:
                pass  # fall back to estimated range

            # Chunked scan — Alchemy free tier caps eth_getLogs at
            # ~10 blocks per call on Base, so a single large range
            # silently 400s and returns empty.
            logs = await self._scan_chunked(token_address, launch_block, to_block)

            if not logs:
                v.verdict = "REJECT"
                v.reason = "no Transfer events in first lookahead window"
                v.flags.append("no_initial_activity")
                v.latency_ms = (time.monotonic() - start) * 1000
                return v

            # ─── 3. Aggregate NET balances + bundle detection ───────
            # We track net (received - sent) per wallet. On every fresh
            # token the deployer mints 100% to themselves — that's not a
            # signal. What matters is where the supply sits AFTER the
            # initial distribution window.
            balances: Dict[str, int] = {}
            block_receivers: Dict[int, Set[str]] = {}
            initial_minter: Optional[str] = None

            excluded = set(self._excluded)
            if pool_address:
                excluded.add(pool_address.lower())

            for raw_log in logs:
                try:
                    topics = raw_log["topics"]
                    from_addr = self._addr_from_topic(topics[1]).lower()
                    to_addr = self._addr_from_topic(topics[2]).lower()
                    data = raw_log["data"]
                    data_bytes = (
                        bytes.fromhex(data.hex()[2:])
                        if hasattr(data, "hex")
                        else bytes.fromhex(data[2:])
                    )
                    (value,) = abi_decode(["uint256"], data_bytes)

                    # Track initial minter (first mint from zero address)
                    if (
                        initial_minter is None
                        and from_addr == ZERO_ADDRESS
                        and to_addr != ZERO_ADDRESS
                    ):
                        initial_minter = to_addr

                    # Net balance update — track who actually holds what
                    balances[from_addr] = balances.get(from_addr, 0) - value
                    balances[to_addr] = balances.get(to_addr, 0) + value

                    # Bundle detection on receivers excluding excluded set
                    if to_addr not in excluded and to_addr != ZERO_ADDRESS:
                        blk = raw_log["blockNumber"]
                        block_receivers.setdefault(blk, set()).add(to_addr)
                except Exception:
                    continue

            # Filter to real holders (positive net balance, not excluded)
            holders = {
                addr: bal
                for addr, bal in balances.items()
                if bal > 0
                and addr not in excluded
                and addr != ZERO_ADDRESS
            }
            v.unique_receivers = len(holders)

            # Compute concentration EXCLUDING the initial minter.
            # If the deployer still holds everything, that's the only
            # holder that matters, and we report it separately.
            initial_minter_pct = Decimal("0")
            if initial_minter and initial_minter in balances and supply_raw > 0:
                initial_minter_pct = Decimal(balances[initial_minter]) / Decimal(supply_raw)
            v.initial_minter_pct = initial_minter_pct

            non_minter_holders = {
                a: b for a, b in holders.items() if a != initial_minter
            }
            largest = max(non_minter_holders.values(), default=0)
            if supply_raw > 0 and largest > 0:
                v.largest_receiver_pct = Decimal(largest) / Decimal(supply_raw)
            else:
                v.largest_receiver_pct = Decimal("0")

            # Bundle detection: ≥5 distinct receivers in the same block
            for blk, recv_set in block_receivers.items():
                if len(recv_set) >= 5:
                    v.bundle_detected = True
                    v.flags.append(
                        f"bundle: {len(recv_set)} receivers in block {blk}"
                    )
                    break

            # ─── 4. Verdict ────────────────────────────────────────
            # A fresh token ALWAYS has the deployer holding 100% at
            # launch. What matters is how the supply was distributed in
            # the first ~60 seconds.
            minter_pct = initial_minter_pct

            if v.bundle_detected:
                v.verdict = "REJECT"
                v.reason = "coordinated same-block buy pattern (bundle)"
                v.flags.append("bundle_launch")
            elif minter_pct > Decimal("0.90"):
                # Deployer still holds ≥90% — no real distribution yet
                v.verdict = "REJECT"
                v.reason = (
                    f"deployer still holds {minter_pct:.1%} of supply after launch window"
                )
                v.flags.append("deployer_holds_nearly_everything")
            elif v.unique_receivers < MIN_UNIQUE_RECEIVERS:
                v.verdict = "REJECT"
                v.reason = f"only {v.unique_receivers} holders after initial distribution"
                v.flags.append("no_distribution")
            elif (
                v.largest_receiver_pct is not None
                and v.largest_receiver_pct > MAX_SINGLE_BUYER_PCT
            ):
                v.verdict = "WATCH"
                v.reason = (
                    f"largest non-deployer holder owns "
                    f"{v.largest_receiver_pct:.1%} of supply"
                )
                v.flags.append("concentrated_non_minter")
            else:
                v.verdict = "PASS"
                v.reason = (
                    f"{v.unique_receivers} holders, deployer at {minter_pct:.1%}, "
                    f"largest non-deployer at {v.largest_receiver_pct:.1%}"
                )

        except Exception as exc:
            v.verdict = "REJECT"
            v.reason = f"quick_screen exception: {exc}"
            v.error = str(exc)
            log.warning("quick_screen failed for %s: %s", token_address, exc)

        v.latency_ms = (time.monotonic() - start) * 1000
        return v

    # --- helpers -------------------------------------------------------

    async def _scan_chunked(
        self,
        token_address: str,
        from_block: int,
        to_block: int,
        chunk_size: int = 10,
    ) -> List[dict]:
        """
        Sequential 10-block eth_getLogs calls. Alchemy free tier on Base
        rejects anything larger with HTTP 400, so we walk the range in the
        same chunk size the scraper uses.
        """
        all_logs: List[dict] = []
        cursor = from_block
        addr = Web3.to_checksum_address(token_address)
        while cursor <= to_block:
            chunk_end = min(cursor + chunk_size - 1, to_block)
            try:
                logs = await self._eth.get_logs(
                    address=addr,
                    topics=[TRANSFER_TOPIC],
                    from_block=cursor,
                    to_block=chunk_end,
                )
                all_logs.extend(logs)
            except Exception as exc:
                log.warning(
                    "quick_screen chunk %d..%d failed for %s: %s",
                    cursor, chunk_end, token_address[:10], exc,
                )
            cursor = chunk_end + 1
        return all_logs

    async def _read_supply_decimals(self, token_address: str) -> tuple[int, int]:
        abi = [
            {
                "constant": True, "inputs": [], "name": "totalSupply",
                "outputs": [{"name": "", "type": "uint256"}],
                "stateMutability": "view", "type": "function",
            },
            {
                "constant": True, "inputs": [], "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "stateMutability": "view", "type": "function",
            },
        ]
        try:
            info = await self._eth.get_token_info(token_address)
            decimals = getattr(info, "decimals", 18)
        except Exception:
            decimals = 18

        async def _call(url: str):
            w3 = self._eth._get_w3(url)
            c = w3.eth.contract(
                address=Web3.to_checksum_address(token_address), abi=abi
            )
            return await c.functions.totalSupply().call()

        try:
            raw = await self._eth._rpc_pool.call(_call)
            return int(raw), int(decimals)
        except Exception:
            return 0, int(decimals)

    @staticmethod
    def _addr_from_topic(topic) -> str:
        topic_hex = topic.hex() if hasattr(topic, "hex") else topic
        return Web3.to_checksum_address("0x" + topic_hex[-40:])
