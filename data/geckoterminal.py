"""
data/geckoterminal.py

Free, no-API-key client for GeckoTerminal's public API: real OHLCV candle
history (actual per-interval high/low, not point-in-time snapshots), on top
of pool discovery. This replaces the earlier subgraph-based approach — The
Graph's decentralized network requires a paid gateway API key, GeckoTerminal
doesn't.

Rate limit: 30 requests/minute, no key required (per apiguide.geckoterminal.com).
That's noticeably tighter than Dexscreener's 300/min — batch/cache calls
where you can, and don't poll this in a tight loop.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Literal, Optional

import aiohttp

from core.exceptions import IndexerError, IndexerRateLimitError
from core.models import Candle, ChainId
from utils.rate_limiter import RateLimiter

log = logging.getLogger("kenyapump.data.geckoterminal")

BASE_URL = "https://api.geckoterminal.com/api/v2"

# GeckoTerminal's network slugs, for the chains this project targets so far.
_NETWORK_MAP: Dict[ChainId, str] = {
    ChainId.ETHEREUM: "eth",
}

Timeframe = Literal["day", "hour", "minute"]


class GeckoTerminalClient:
    """Free, no-key client. One instance can be shared across the whole
    process — construct it once and reuse. The 30/min limit is enforced
    internally; calls simply queue and wait rather than erroring."""

    def __init__(self, request_timeout_seconds: float = 15.0):
        self._limiter = RateLimiter(max_calls=28, period_seconds=60.0)  # small safety margin under 30
        self._timeout = aiohttp.ClientTimeout(total=request_timeout_seconds)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        await self._limiter.acquire()
        session = await self._get_session()
        url = f"{BASE_URL}{path}"
        try:
            async with session.get(url, params=params, headers={"accept": "application/json"}) as resp:
                if resp.status == 429:
                    raise IndexerRateLimitError(f"GeckoTerminal rate-limited request to {path}")
                if resp.status >= 400:
                    body = await resp.text()
                    raise IndexerError(f"GeckoTerminal {resp.status} on {path}: {body[:200]}")
                return await resp.json()
        except aiohttp.ClientError as exc:
            raise IndexerError(f"GeckoTerminal request to {path} failed: {exc}") from exc

    # --- public API ---------------------------------------------------

    async def get_pools_for_token(self, chain: ChainId, token_address: str) -> List[Dict[str, Any]]:
        """Top pools trading this token (GeckoTerminal ranks by liquidity +
        24h volume, returns up to 20)."""
        network = self._network_slug(chain)
        data = await self._request(f"/networks/{network}/tokens/{token_address}/pools")
        return data.get("data", [])

    async def get_ohlcv(
        self,
        chain: ChainId,
        pool_address: str,
        timeframe: Timeframe = "hour",
        aggregate: int = 1,
        limit: int = 100,
        before_timestamp: Optional[int] = None,
        currency: Literal["usd", "token"] = "usd",
    ) -> List[Candle]:
        """
        Real OHLCV candles for a specific pool.

        timeframe: "day" | "hour" | "minute"
        aggregate: bucket multiplier, e.g. timeframe="minute", aggregate=5 -> 5-minute candles
        before_timestamp: Unix seconds; paginate backward in time by passing
                           the oldest timestamp from the previous page
        """
        network = self._network_slug(chain)
        params: Dict[str, Any] = {"aggregate": aggregate, "limit": limit, "currency": currency}
        if before_timestamp is not None:
            params["before_timestamp"] = before_timestamp

        data = await self._request(
            f"/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}", params=params
        )
        ohlcv_list = (
            data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        )
        return self._parse_candles(ohlcv_list)

    # --- helpers -----------------------------------------------------

    @staticmethod
    def _network_slug(chain: ChainId) -> str:
        try:
            return _NETWORK_MAP[chain]
        except KeyError as exc:
            raise ValueError(f"No GeckoTerminal network mapping for {chain}") from exc

    @staticmethod
    def _parse_candles(ohlcv_list: List[List[float]]) -> List[Candle]:
        """Each row is [timestamp, open, high, low, close, volume] per
        GeckoTerminal's documented ohlcv_list format, newest first — we
        reverse to return oldest-first like the rest of this project's data
        methods."""
        candles = [
            Candle(
                timestamp=int(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume_usd=float(row[5]),
            )
            for row in ohlcv_list
        ]
        candles.reverse()
        return candles
