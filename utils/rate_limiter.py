"""
utils/rate_limiter.py

Minimal async token-bucket limiter. Free public APIs (Dexscreener,
GeckoTerminal) enforce hard rate limits with no paid tier to fall back on
for this project's needs — staying under the limit isn't optional, it's how
you avoid getting temporarily blocked. Deliberately simple (no external
dependency) since the need here is "don't exceed N calls per minute,"
not a full distributed rate-limiting system.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque


class RateLimiter:
    """Allows at most `max_calls` within any rolling `period_seconds` window.
    Callers `await limiter.acquire()` before making a request; it sleeps as
    long as needed to stay under the limit, never raises."""

    def __init__(self, max_calls: int, period_seconds: float = 60.0):
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self._call_times: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            while self._call_times and now - self._call_times[0] > self.period_seconds:
                self._call_times.popleft()

            if len(self._call_times) >= self.max_calls:
                sleep_for = self.period_seconds - (now - self._call_times[0])
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
                now = time.monotonic()
                while self._call_times and now - self._call_times[0] > self.period_seconds:
                    self._call_times.popleft()

            self._call_times.append(time.monotonic())
