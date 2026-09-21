"""
utils/rpc_pool.py

Manages multiple RPC endpoints per chain with priority ordering, health
tracking, and automatic failover. A memecoin bot that relies on a single RPC
endpoint will get rate-limited or dropped exactly when it matters most (a
new pair launching, a fast-moving price). This pool exists so a single flaky
provider degrades the bot instead of breaking it.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, TypeVar, Awaitable

from config.settings import RpcEndpointConfig
from core.exceptions import ChainConnectionError, RPCRateLimitError

T = TypeVar("T")


@dataclass
class _EndpointState:
    config: RpcEndpointConfig
    consecutive_failures: int = 0
    last_failure_at: Optional[float] = None
    last_success_at: Optional[float] = None
    disabled_until: Optional[float] = None

    def is_available(self) -> bool:
        if self.disabled_until is None:
            return True
        return time.time() >= self.disabled_until


class RpcPool:
    """
    Wraps a list of RPC endpoints and routes calls through them in priority
    order, skipping endpoints that are temporarily disabled after repeated
    failures (circuit-breaker style).
    """

    def __init__(
        self,
        endpoints: List[RpcEndpointConfig],
        max_retries_per_call: int = 3,
        base_backoff_seconds: float = 0.5,
        disable_after_failures: int = 3,
        disable_duration_seconds: float = 30.0,
    ):
        if not endpoints:
            raise ValueError("RpcPool requires at least one endpoint")
        self._states: List[_EndpointState] = sorted(
            (_EndpointState(config=e) for e in endpoints),
            key=lambda s: s.config.priority,
        )
        self._max_retries = max_retries_per_call
        self._base_backoff = base_backoff_seconds
        self._disable_after_failures = disable_after_failures
        self._disable_duration = disable_duration_seconds
        self._lock = asyncio.Lock()

    def _available_states(self) -> List[_EndpointState]:
        available = [s for s in self._states if s.is_available()]
        # If everything is disabled (worst case), fall back to the full list
        # rather than hard-failing — a stale-but-alive endpoint beats none.
        return available or list(self._states)

    async def _record_success(self, state: _EndpointState) -> None:
        async with self._lock:
            state.consecutive_failures = 0
            state.disabled_until = None
            state.last_success_at = time.time()

    async def _record_failure(self, state: _EndpointState) -> None:
        async with self._lock:
            state.consecutive_failures += 1
            state.last_failure_at = time.time()
            if state.consecutive_failures >= self._disable_after_failures:
                state.disabled_until = time.time() + self._disable_duration

    async def call(self, fn: Callable[[str], Awaitable[T]]) -> T:
        """
        Execute `fn(endpoint_url)` against endpoints in priority order,
        retrying with exponential backoff and failing over to the next
        endpoint on connection errors. Raises ChainConnectionError if every
        endpoint fails.
        """
        last_error: Optional[Exception] = None
        attempt = 0

        for state in self._available_states():
            for local_try in range(self._max_retries):
                attempt += 1
                try:
                    result = await fn(state.config.url)
                    await self._record_success(state)
                    return result
                except RPCRateLimitError as exc:
                    last_error = exc
                    await self._record_failure(state)
                    await asyncio.sleep(self._base_backoff * (2 ** local_try))
                except Exception as exc:  # noqa: BLE001 - deliberately broad at the network boundary
                    last_error = exc
                    await self._record_failure(state)
                    # Don't burn all retries sleeping on a dead endpoint —
                    # break to try the next endpoint immediately after one
                    # quick retry.
                    if local_try == 0:
                        await asyncio.sleep(self._base_backoff)
                    else:
                        break

        raise ChainConnectionError(
            f"All {len(self._states)} RPC endpoint(s) failed after {attempt} attempt(s). "
            f"Last error: {last_error}"
        )

    def endpoint_count(self) -> int:
        return len(self._states)
