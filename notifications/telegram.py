"""
notifications/telegram.py

Async Telegram notifier. Fire-and-forget sink — send failures never crash
the bot. Rate-limited to stay well under Telegram's API ceiling.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from typing import Deque, Optional

import aiohttp

log = logging.getLogger("kenyapump.notifications.telegram")

TELEGRAM_API = "https://api.telegram.org"


class TelegramNotifier:
    """Send alerts to a Telegram chat."""

    MAX_MSGS_PER_MINUTE = 18

    def __init__(self):
        self._token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
        self._chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
        self._session: Optional[aiohttp.ClientSession] = None
        self._enabled = bool(self._token and self._chat_id)
        self._times: Deque[float] = deque()
        self._lock = asyncio.Lock()

        if not self._enabled:
            log.warning(
                "Telegram disabled — set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env"
            )

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def send(self, text: str) -> bool:
        """Send a single message. Returns True on success. Never raises."""
        if not self._enabled:
            return False

        # Rate limit
        async with self._lock:
            now = time.monotonic()
            while self._times and now - self._times[0] > 60.0:
                self._times.popleft()
            if len(self._times) >= self.MAX_MSGS_PER_MINUTE:
                wait = 60.0 - (now - self._times[0])
                if wait > 0:
                    await asyncio.sleep(wait)
            self._times.append(time.monotonic())

        url = f"{TELEGRAM_API}/bot{self._token}/sendMessage"
        payload = {
            "chat_id": self._chat_id,
            "text": text[:4000],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            session = await self._get_session()
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return True
                body = await resp.text()
                log.warning(f"Telegram send failed ({resp.status}): {body[:200]}")
                return False
        except Exception as exc:
            log.warning(f"Telegram send exception: {exc}")
            return False

    # --- high-level helpers -------------------------------------------

    async def startup(self, chains: list[str], mode: str) -> None:
        await self.send(
            f"<b>🚀 KenyaPump started</b>\n"
            f"Chains: {', '.join(chains)}\n"
            f"Mode: {mode}"
        )

    async def alert_pass(
        self,
        chain: str,
        symbol: str,
        address: str,
        liquidity: str,
        price: str,
        dex: str,
        holders: int,
        top_pct: float,
        reason: str,
    ) -> None:
        chain_slug = "base" if chain == "base" else "mainnet"
        uniswap = (
            f"https://app.uniswap.org/#/swap?outputCurrency={address}&chain={chain_slug}"
        )
        dexscreener = f"https://dexscreener.com/{chain}/{address}"

        msg = (
            f"<b>🟢 PASS</b> [{chain}]\n"
            f"<b>{symbol}</b>\n"
            f"<code>{address}</code>\n\n"
            f"💰 Liq: {liquidity}\n"
            f"📊 Price: {price}\n"
            f"🏊 DEX: {dex}\n"
            f"👥 Holders: {holders}\n"
            f"🎯 Top: {top_pct:.1%}\n\n"
            f"<i>{reason}</i>\n\n"
            f"🔗 <a href=\"{uniswap}\">Buy on Uniswap</a>\n"
            f"📈 <a href=\"{dexscreener}\">DexScreener</a>"
        )
        await self.send(msg)

    async def alert_danger(
        self,
        chain: str,
        symbol: str,
        address: str,
        risk_score: int,
        flags: str,
    ) -> None:
        dexscreener = f"https://dexscreener.com/{chain}/{address}"
        msg = (
            f"<b>🚨 DANGER</b> [{chain}]\n"
            f"<b>{symbol}</b>\n"
            f"<code>{address}</code>\n\n"
            f"Risk: {risk_score}/100\n"
            f"{flags}\n\n"
            f"📈 <a href=\"{dexscreener}\">DexScreener</a>"
        )
        await self.send(msg)
