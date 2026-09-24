"""
ai/honeypot.py

Multi-signal honeypot / rug-pull risk detector. Free, no API key required.

Two independent signal sources, combined rather than trusted alone:

1. GoPlus Security's Token Security API (https://api.gopluslabs.io) — a
   free, keyless, community-maintained security database covering tax
   rates, ownership risk (hidden owner, can-take-back-ownership, mintable),
   contract risk (proxy, self-destruct, pausable transfers), holder
   concentration, and — notably — whether the token's deployer has created
   known honeypots before. Confirmed empirically to work with no
   Authorization header (some of GoPlus's own docs suggest auth is
   required; it isn't, for this endpoint, as of this writing — if it starts
   returning 401s later, that's the API changing, not this code being wrong).

2. EthereumAdapter.simulate_sell() — our own on-chain check (built into the
   chain-abstraction module) of whether a sell actually round-trips through
   the router right now.

Neither alone is sufficient: GoPlus's data can lag for brand-new tokens
(not indexed yet, same issue as Dexscreener), and simulate_sell only
proves "sellable at this instant," not "won't be rugged in an hour" (a
contract can have sell enabled now and a modifiable-tax or pausable-transfer
function that changes that later — which is exactly why GoPlus's ownership/
mutability flags matter alongside the simulation).

This module does not decide whether to trade — it produces an assessment.
The risk management module (not yet built) is where a HoneypotAssessment's
risk_level should actually gate a trade decision.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Dict, List, Optional

import aiohttp

from chains.ethereum import EthereumAdapter
from core.exceptions import IndexerError, IndexerRateLimitError
from core.models import ChainId
from utils.rate_limiter import RateLimiter

log = logging.getLogger("kenyapump.ai.honeypot")

BASE_URL = "https://api.gopluslabs.io/api/v1"

# GoPlus's numeric chain IDs, for the chains this project targets so far.
_CHAIN_ID_MAP: Dict[ChainId, str] = {
    ChainId.ETHEREUM: "1",
    ChainId.BASE: "8453",
}


class RiskLevel(str, Enum):
    UNKNOWN = "unknown"  # no data from any source — NOT the same as "safe"
    SAFE = "safe"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class HoneypotAssessment:
    token_address: str
    risk_level: RiskLevel
    risk_score: int  # 0-100, higher = riskier
    flags: List[str] = field(default_factory=list)  # human-readable reasons
    is_honeypot: Optional[bool] = None  # GoPlus's direct flag, when available
    can_sell_now: Optional[bool] = None  # from our own on-chain simulation
    buy_tax_pct: Optional[Decimal] = None
    sell_tax_pct: Optional[Decimal] = None
    holder_count: Optional[int] = None
    top_holder_pct: Optional[Decimal] = None
    is_open_source: Optional[bool] = None
    data_sources: List[str] = field(default_factory=list)  # e.g. ["goplus", "on_chain_simulation"]
    confidence: str = "low"  # "high" with both sources, "low" with only one, "none" with neither
    raw_goplus_data: Optional[Dict[str, Any]] = None


class GoPlusClient:
    """
    Free, keyless client for GoPlus's Token Security API. One instance can
    be shared across the whole process.

    Rate limit: GoPlus does not publish a clear documented limit for
    unauthenticated use (unlike Dexscreener/GeckoTerminal, where exact
    numbers are documented). 20/min is a conservative default, not a
    verified ceiling — tighten or loosen based on what you actually observe.
    """

    def __init__(self, max_requests_per_minute: int = 20, request_timeout_seconds: float = 15.0):
        self._limiter = RateLimiter(max_calls=max_requests_per_minute, period_seconds=60.0)
        self._timeout = aiohttp.ClientTimeout(total=request_timeout_seconds)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_token_security(
        self, chain: ChainId, addresses: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Raw GoPlus response for one or more token addresses on one chain,
        keyed by lowercased address. GoPlus supports batching multiple
        addresses in one call (comma-separated); exact batch size limit
        isn't clearly documented, so keep batches modest (well under 30)
        until you've confirmed larger batches work reliably for your use.
        """
        chain_id = self._chain_id(chain)
        await self._limiter.acquire()
        session = await self._get_session()
        url = f"{BASE_URL}/token_security/{chain_id}"
        params = {"contract_addresses": ",".join(a.lower() for a in addresses)}

        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    raise IndexerRateLimitError("GoPlus rate-limited the request")
                if resp.status >= 400:
                    body = await resp.text()
                    raise IndexerError(f"GoPlus {resp.status}: {body[:200]}")
                data = await resp.json()
        except asyncio.TimeoutError as exc:
            raise IndexerError("GoPlus request timed out") from exc
        except aiohttp.ClientError as exc:
            raise IndexerError(f"GoPlus request failed: {exc}") from exc

        if data.get("code") != 1:
            raise IndexerError(f"GoPlus returned non-OK code: {data.get('code')} {data.get('message')}")

        return data.get("result", {})

    @staticmethod
    def _chain_id(chain: ChainId) -> str:
        try:
            return _CHAIN_ID_MAP[chain]
        except KeyError as exc:
            raise ValueError(f"No GoPlus chain_id mapping for {chain}") from exc


class HoneypotDetector:
    """
    Combines GoPlusClient with EthereumAdapter.simulate_sell() into a
    single risk assessment. This is the entry point most callers want —
    GoPlusClient alone only gets you raw data.
    """

    def __init__(self, goplus: GoPlusClient, eth_adapter: Optional[EthereumAdapter] = None):
        self._goplus = goplus
        self._eth = eth_adapter

    async def assess(
        self,
        chain: ChainId,
        token_address: str,
        simulate_wallet_address: Optional[str] = None,
        simulate_amount: Decimal = Decimal("1"),
    ) -> HoneypotAssessment:
        """
        simulate_wallet_address/simulate_amount: if provided (and an
        EthereumAdapter was given at construction), also runs
        simulate_sell() as a second, independent signal. Skipped otherwise
        — GoPlus data alone still produces an assessment, just with lower
        confidence.
        """
        # Chains GoPlus does not support return a neutral UNKNOWN verdict
        if chain not in _CHAIN_ID_MAP:
            return HoneypotAssessment(
                token_address=token_address,
                risk_level=RiskLevel.UNKNOWN,
                risk_score=50,
                flags=[f"GoPlus does not support {chain.value}"],
                data_sources=[],
                confidence="none",
            )

        goplus_data: Optional[Dict[str, Any]] = None
        data_sources: List[str] = []

        try:
            result = await self._goplus.get_token_security(chain, [token_address])
            goplus_data = result.get(token_address.lower())
            if goplus_data:
                data_sources.append("goplus")
            else:
                log.debug("GoPlus has no data yet for %s (not indexed, or invalid address)", token_address)
        except IndexerError as exc:
            log.warning("GoPlus lookup failed for %s: %s", token_address, exc)

        can_sell_now: Optional[bool] = None
        if self._eth is not None and simulate_wallet_address is not None:
            try:
                can_sell_now = await self._eth.simulate_sell(
                    token_address, simulate_amount, simulate_wallet_address
                )
                data_sources.append("on_chain_simulation")
            except Exception as exc:  # noqa: BLE001 - simulation failures are a signal (treat as unknown), not a crash
                log.debug("simulate_sell failed for %s: %s", token_address, exc)

        return self._build_assessment(token_address, goplus_data, can_sell_now, data_sources)

    # --- scoring --------------------------------------------------------

    def _build_assessment(
        self,
        token_address: str,
        goplus_data: Optional[Dict[str, Any]],
        can_sell_now: Optional[bool],
        data_sources: List[str],
    ) -> HoneypotAssessment:
        flags: List[str] = []
        score = 0

        def flag_bool(key: str, points: int, label: str) -> None:
            nonlocal score
            if goplus_data and goplus_data.get(key) == "1":
                score += points
                flags.append(label)

        def parse_decimal(value: Any) -> Optional[Decimal]:
            if value in (None, ""):
                return None
            try:
                return Decimal(str(value))
            except (InvalidOperation, ValueError):
                return None

        is_honeypot_flag: Optional[bool] = None
        buy_tax: Optional[Decimal] = None
        sell_tax: Optional[Decimal] = None
        holder_count: Optional[int] = None
        top_holder_pct: Optional[Decimal] = None
        is_open_source: Optional[bool] = None

        if goplus_data:
            # --- direct / critical flags ---
            is_honeypot_flag = goplus_data.get("is_honeypot") == "1"
            if is_honeypot_flag:
                score += 100
                flags.append("GoPlus flags this directly as a honeypot")

            if goplus_data.get("cannot_buy") == "1":
                score += 60
                flags.append("Cannot buy")

            same_creator_honeypots = goplus_data.get("honeypot_with_same_creator")
            if same_creator_honeypots and same_creator_honeypots not in ("0", ""):
                score += 50
                flags.append(f"Deployer has created {same_creator_honeypots} known honeypot(s) before")

            if goplus_data.get("trust_list") == "1":
                # GoPlus's own vetted whitelist — strong positive signal,
                # rarely applies to brand-new memecoins but matters when it does.
                score = max(0, score - 40)
                flags.append("On GoPlus's trusted token list")

            # --- ownership / mutability risk ---
            flag_bool("hidden_owner", 25, "Hidden owner (ownership obscured)")
            flag_bool("can_take_back_ownership", 25, "Contract can reclaim ownership after renouncing")
            flag_bool("owner_change_balance", 30, "Owner can arbitrarily change holder balances")
            flag_bool("is_mintable", 15, "Supply is mintable (owner can inflate supply)")
            flag_bool("transfer_pausable", 20, "Owner can pause all transfers")
            flag_bool("selfdestruct", 20, "Contract has self-destruct capability")
            flag_bool("slippage_modifiable", 15, "Owner can modify slippage/tax after launch")
            flag_bool("personal_slippage_modifiable", 15, "Owner can set per-wallet slippage/tax")
            flag_bool("is_blacklisted", 15, "Contract has blacklist capability")
            flag_bool("is_proxy", 10, "Upgradeable proxy contract (logic can change post-launch)")
            flag_bool("external_call", 5, "Makes external calls during transfer (harder to audit)")
            flag_bool("trading_cooldown", 5, "Trading cooldown present")

            if goplus_data.get("is_open_source") == "0":
                score += 20
                flags.append("Contract source is not verified/open")
            is_open_source = goplus_data.get("is_open_source") == "1"

            # --- tax ---
            buy_tax = parse_decimal(goplus_data.get("buy_tax"))
            sell_tax = parse_decimal(goplus_data.get("sell_tax"))
            if sell_tax is not None:
                if sell_tax >= Decimal("20"):
                    score += 30
                    flags.append(f"Very high sell tax ({sell_tax}%)")
                elif sell_tax >= Decimal("10"):
                    score += 15
                    flags.append(f"High sell tax ({sell_tax}%)")
            if buy_tax is not None and buy_tax >= Decimal("20"):
                score += 15
                flags.append(f"Very high buy tax ({buy_tax}%)")

            # --- holder concentration ---
            holder_count_raw = goplus_data.get("holder_count")
            if holder_count_raw:
                try:
                    holder_count = int(holder_count_raw)
                except ValueError:
                    holder_count = None
            holders = goplus_data.get("holders") or []
            non_contract_holders = [h for h in holders if h.get("is_contract") == 0]
            if non_contract_holders:
                top = max(non_contract_holders, key=lambda h: float(h.get("percent", 0) or 0))
                top_holder_pct = parse_decimal(top.get("percent"))
                if top_holder_pct is not None:
                    top_holder_pct_display = top_holder_pct * 100
                    if top_holder_pct >= Decimal("0.30"):
                        score += 25
                        flags.append(f"Top non-contract holder owns {top_holder_pct_display:.1f}% of supply")
                    elif top_holder_pct >= Decimal("0.15"):
                        score += 10
                        flags.append(f"Top non-contract holder owns {top_holder_pct_display:.1f}% of supply")

            # Holder sanity — 0 or unknown should never score as "safe".
            # A token with zero known holders is either dust nobody bought,
            # or unindexed by GoPlus. Either way it is unverifiable.
            if holder_count is None:
                score += 20
                flags.append("Holder count unknown — cannot verify distribution")
            elif holder_count == 0:
                score += 35
                flags.append("Zero holders — token is unindexed or dust")
            elif holder_count < 10:
                score += 20
                flags.append(f"Very few holders ({holder_count})")
            elif holder_count < 50:
                score += 5
                flags.append(f"Thin distribution ({holder_count} holders)")

        # --- combine with on-chain simulation ---
        if can_sell_now is False:
            score += 60
            flags.append("On-chain sell simulation failed right now")
        elif can_sell_now is True and is_honeypot_flag is False:
            score = max(0, score - 10)  # both signals agree it's currently sellable

        score = min(100, score)

        if not data_sources:
            # No signal from anywhere — this is categorically different
            # from "checked and found nothing wrong." Never let this
            # collapse into SAFE just because score defaulted to 0.
            risk_level = RiskLevel.UNKNOWN
        elif score >= 70:
            risk_level = RiskLevel.CRITICAL
        elif score >= 45:
            risk_level = RiskLevel.HIGH
        elif score >= 20:
            risk_level = RiskLevel.MEDIUM
        elif score > 0:
            risk_level = RiskLevel.LOW
        else:
            risk_level = RiskLevel.SAFE

        if len(data_sources) >= 2:
            confidence = "high"
        elif len(data_sources) == 1:
            confidence = "low"
        else:
            confidence = "none"
            if not flags:
                flags.append("No data available from any source — token likely too new to assess")

        # Confidence gate: a single data source (typically GoPlus alone,
        # without our on-chain sell simulation) cannot cross-verify. Never
        # let that configuration return SAFE — downgrade to LOW at minimum.
        if confidence != "high" and risk_level == RiskLevel.SAFE:
            risk_level = RiskLevel.LOW
            if score == 0:
                score = 5
            flags.append("Single data source — result not cross-verified")

        return HoneypotAssessment(
            token_address=token_address,
            risk_level=risk_level,
            risk_score=score,
            flags=flags,
            is_honeypot=is_honeypot_flag,
            can_sell_now=can_sell_now,
            buy_tax_pct=buy_tax,
            sell_tax_pct=sell_tax,
            holder_count=holder_count,
            top_holder_pct=top_holder_pct,
            is_open_source=is_open_source,
            data_sources=data_sources,
            confidence=confidence,
            raw_goplus_data=goplus_data,
        )