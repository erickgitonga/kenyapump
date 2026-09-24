"""
data/dexscreener.py

Free, no-API-key client for Dexscreener: live pool discovery, price, and
liquidity across 80+ chains. This is what actually fills in
EthereumAdapter.get_liquidity_info (still NotImplementedError as of the
chain-abstraction module) — plug an instance of this in as the liquidity
data source there.

No historical OHLCV here — Dexscreener's public API has no candle/history
endpoint. For that, see data/geckoterminal.py (also free, no key).

Rate limits per Dexscreener's docs: ~300 requests/min for pair/token/search
endpoints, 60/min for profile/boost endpoints. This client only uses the
former.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

import aiohttp

from core.exceptions import IndexerError, IndexerRateLimitError
from core.models import ChainId, LiquidityInfo
from utils.rate_limiter import RateLimiter

log = logging.getLogger("kenyapump.data.dexscreener")

BASE_URL = "https://api.dexscreener.com"

# Dexscreener's chainId strings, for the chains this project targets so far.
_CHAIN_ID_MAP: Dict[ChainId, str] = {
    ChainId.ETHEREUM: "ethereum",
    ChainId.BASE: "base",
    ChainId.SOLANA: "solana",
}


class DexscreenerClient:
    """Free, no-key client. One instance can be shared across the whole
    process — construct it once and reuse."""

    def __init__(self, max_requests_per_minute: int = 280, request_timeout_seconds: float = 15.0):
        # Stay a little under the documented 300/min ceiling for safety margin.
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

    async def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        await self._limiter.acquire()
        session = await self._get_session()
        url = f"{BASE_URL}{path}"
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    raise IndexerRateLimitError(f"Dexscreener rate-limited request to {path}")
                if resp.status >= 400:
                    body = await resp.text()
                    raise IndexerError(f"Dexscreener {resp.status} on {path}: {body[:200]}")
                return await resp.json()
        except asyncio.TimeoutError as exc:
            # aiohttp's timeout does NOT subclass aiohttp.ClientError, so it
            # needs its own handler or it bypasses IndexerError entirely.
            raise IndexerError(f"Dexscreener request to {path} timed out") from exc
        except aiohttp.ClientError as exc:
            raise IndexerError(f"Dexscreener request to {path} failed: {exc}") from exc

    # --- public API ---------------------------------------------------

    async def get_pairs_for_token(self, chain: ChainId, token_address: str) -> List[Dict[str, Any]]:
        """All pools trading this token on this chain, as returned by
        Dexscreener (already liquidity-sorted by their API)."""
        chain_slug = self._chain_slug(chain)
        data = await self._request(f"/token-pairs/v1/{chain_slug}/{token_address}")
        # Documented as a bare list, but handle a dict-wrapped shape too so a
        # future API change reads as "0 pools" only when that's actually true.
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("pairs", [])
        log.warning("Unexpected response shape from get_pairs_for_token: %r", type(data))
        return []

    async def get_pair(self, chain: ChainId, pair_address: str) -> Optional[Dict[str, Any]]:
        """Single pair detail by chain + pair (pool) address."""
        chain_slug = self._chain_slug(chain)
        data = await self._request(f"/latest/dex/pairs/{chain_slug}/{pair_address}")
        pairs = data.get("pairs") or data.get("pair")
        if isinstance(pairs, list):
            return pairs[0] if pairs else None
        return pairs

    async def search(self, query: str) -> List[Dict[str, Any]]:
        """Search across all chains by token name, symbol, or address."""
        data = await self._request("/latest/dex/search", params={"q": query})
        return data.get("pairs", [])

    async def get_liquidity_info(self, chain: ChainId, token_address: str) -> List[LiquidityInfo]:
        """Convenience wrapper returning core.models.LiquidityInfo directly
        — this is the method to call from EthereumAdapter.get_liquidity_info."""
        raw_pairs = await self.get_pairs_for_token(chain, token_address)
        results = [self._to_liquidity_info(chain, token_address, p) for p in raw_pairs]
        results = [r for r in results if r is not None]
        results.sort(key=lambda r: r.liquidity_usd, reverse=True)
        return results

    # --- helpers -----------------------------------------------------

    @staticmethod
    def _chain_slug(chain: ChainId) -> str:
        try:
            return _CHAIN_ID_MAP[chain]
        except KeyError as exc:
            raise IndexerError(f"No Dexscreener chainId mapping for {chain}") from exc

    @staticmethod
    def _to_liquidity_info(
        chain: ChainId, queried_token_address: str, pair: Dict[str, Any]
    ) -> Optional[LiquidityInfo]:
        """
        IMPORTANT: the queried token can be either side of the pair
        (baseToken or quoteToken) — which side varies by pool and is NOT
        something you can assume. Dexscreener's priceUsd/priceNative/
        liquidity.base fields are always expressed relative to baseToken,
        so if the queried token is actually quoteToken, those numbers
        describe the *other* token unless we explicitly flip them here.
        """
        try:
            liquidity = pair.get("liquidity") or {}
            base_token = pair.get("baseToken") or {}
            quote_token = pair.get("quoteToken") or {}
            volume = pair.get("volume") or {}

            base_address = (base_token.get("address") or "").lower()
            quote_address = (quote_token.get("address") or "").lower()
            queried = queried_token_address.lower()

            is_base = base_address == queried
            is_quote = quote_address == queried
            if not is_base and not is_quote:
                # Defensive: this pair doesn't actually involve the token we
                # asked about. Shouldn't happen given the source query, but
                # silently mislabeling data is worse than dropping the record.
                log.warning(
                    "Pair %s does not contain queried token %s (base=%s, quote=%s); skipping",
                    pair.get("pairAddress"), queried_token_address, base_address, quote_address,
                )
                return None

            price_usd_base = pair.get("priceUsd")
            price_native_base = pair.get("priceNative")  # price of 1 base token, in quote-token units

            if is_base:
                resolved_token_address = base_token.get("address", "")
                pair_partner_symbol = quote_token.get("symbol", "")
                liquidity_native = liquidity.get("base", 0)
                price_usd = Decimal(str(price_usd_base)) if price_usd_base is not None else None
                price_native = Decimal(str(price_native_base)) if price_native_base is not None else None
            else:
                resolved_token_address = quote_token.get("address", "")
                pair_partner_symbol = base_token.get("symbol", "")
                liquidity_native = liquidity.get("quote", 0)
                # Flip base-relative price/native figures to be relative to
                # the queried (quote-side) token instead.
                price_native_base_dec = (
                    Decimal(str(price_native_base)) if price_native_base not in (None, 0, "0") else None
                )
                if price_native_base_dec:
                    price_native = Decimal(1) / price_native_base_dec
                else:
                    price_native = None
                if price_usd_base is not None and price_native_base_dec:
                    # 1 quote token in USD = (1 base token in USD) / (quote units per 1 base)
                    price_usd = Decimal(str(price_usd_base)) / price_native_base_dec
                else:
                    price_usd = None

            return LiquidityInfo(
                chain=chain,
                token_address=resolved_token_address,
                pair_address=pair.get("pairAddress", ""),
                dex=pair.get("dexId", "unknown"),
                base_token_symbol=pair_partner_symbol,
                liquidity_usd=Decimal(str(liquidity.get("usd", 0) or 0)),
                liquidity_native=Decimal(str(liquidity_native or 0)),
                volume_24h_usd=Decimal(str(volume.get("h24", 0) or 0)) if volume.get("h24") is not None else None,
                price_usd=price_usd,
                price_native=price_native,
                pool_created_at=(
                    pair["pairCreatedAt"] / 1000 if pair.get("pairCreatedAt") else None
                ),  # Dexscreener returns ms
            )
        except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
            log.warning("Skipping malformed Dexscreener pair record: %s", exc)
            return None
