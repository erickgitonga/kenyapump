"""
data/solana_scraper.py

Watches Pump.fun for token creation events and exposes them through the
same NewPairEvent shape used by the Ethereum scraper.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Mapping
from typing import Any, Awaitable, Callable, List, Optional

import sol_parser
import websockets

from data.scraper import NewPairEvent


logger = logging.getLogger("kenyapump.data.solana_scraper")

PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
KEEPALIVE_INTERVAL_SECONDS = 60.0
MAX_RECONNECT_DELAY_SECONDS = 60.0


class SolanaTokenScraper:
    """
    Watches Pump.fun for PumpFunCreate events over Helius WebSocket logs.

    Pump.fun creates a bonding curve rather than a conventional two-token
    pool during its create event. The NewPairEvent schema has no deployer,
    name, or bonding-curve fields, so those values are logged and the schema
    is populated as follows:

    - token_address: mint address
    - pair_address: bonding curve address
    - dex: pumpfun
    - quote_symbol: token symbol
    - block_number: Solana slot
    """

    WS_URL_ENV_VAR = "SOLANA_WS_URL"

    def __init__(self) -> None:
        ws_url = os.getenv(self.WS_URL_ENV_VAR)
        if not ws_url:
            raise RuntimeError(f"{self.WS_URL_ENV_VAR} is not set")

        self.ws_url = ws_url
        self._stopped = False
        self._subscription_id: Optional[str] = None
        self._websocket: Optional[Any] = None

    def stop(self) -> None:
        """Request graceful shutdown."""
        self._stopped = True
        if self._websocket is not None:
            self._websocket.close()

    async def run_forever(
        self,
        on_tokens_found: Optional[
            Callable[[List[NewPairEvent]], Awaitable[None]]
        ] = None,
    ) -> None:
        """Continuously connect, subscribe, and process Pump.fun events."""
        reconnect_delay = 1.0

        while not self._stopped:
            ping_task: Optional[asyncio.Task[None]] = None

            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None,
                    close_timeout=5,
                ) as websocket:
                    self._websocket = websocket
                    self._subscription_id = None
                    reconnect_delay = 1.0

                    await self._subscribe_to_pumpfun(websocket)
                    ping_task = asyncio.create_task(self._keepalive(websocket))

                    async for message in websocket:
                        if self._stopped:
                            break

                        events = await self._process_message(message)
                        if events and on_tokens_found:
                            await on_tokens_found(events)

            except asyncio.CancelledError:
                logger.info("SolanaTokenScraper stopping (cancelled)")
                raise
            except Exception as exc:
                if not self._stopped:
                    logger.warning(
                        "Solana Pump.fun WebSocket disconnected; "
                        "reconnecting in %.0fs: %s",
                        reconnect_delay,
                        exc,
                    )

            finally:
                if ping_task is not None:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

                self._websocket = None

            if self._stopped:
                break

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(
                reconnect_delay * 2,
                MAX_RECONNECT_DELAY_SECONDS,
            )

    async def _subscribe_to_pumpfun(self, websocket: Any) -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                {
                    "all": True,
                    "programId": PUMP_FUN_PROGRAM_ID,
                },
                {
                    "commitment": "confirmed",
                },
            ],
        }

        await websocket.send(json.dumps(payload))
        logger.info(
            "Subscribed to Pump.fun logs using program %s",
            PUMP_FUN_PROGRAM_ID,
        )

    async def _keepalive(self, websocket: Any) -> None:
        """Send protocol-level pings before Helius closes idle connections."""
        try:
            while not self._stopped:
                await asyncio.sleep(KEEPALIVE_INTERVAL_SECONDS)
                pong = await websocket.ping()
                await pong
        except asyncio.CancelledError:
            raise
        except Exception:
            # The main subscription loop will observe the resulting disconnect.
            return

    async def _process_message(self, message: Any) -> List[NewPairEvent]:
        if not isinstance(message, Mapping):
            return []

        method = message.get("method")

        if method == "logsSubscription":
            params = message.get("params")
            if not isinstance(params, Mapping):
                return []

            result = params.get("result")
            if isinstance(result, Mapping):
                subscription_id = result.get("subscription")
                if subscription_id is not None:
                    self._subscription_id = str(subscription_id)

            return []

        if method != "logsNotification":
            return []

        if self._subscription_id is None:
            return []

        params = message.get("params")
        if not isinstance(params, Mapping):
            return []

        notification_subscription = params.get("subscription")
        if str(notification_subscription) != self._subscription_id:
            return []

        result = params.get("result")
        if not isinstance(result, Mapping):
            return []

        log_message = result.get("logMessage")
        if not isinstance(log_message, str) or not log_message:
            return []

        try:
            slot = int(result.get("slot")) if result.get("slot") is not None else 0
        except (TypeError, ValueError):
            slot = 0

        try:
            decoded_events = sol_parser.parse_logs_only(
                [log_message],
                program_id=PUMP_FUN_PROGRAM_ID,
            )
        except Exception as exc:
            logger.warning("Failed to parse Pump.fun log message: %s", exc)
            return []

        events = self._iter_decoded_events(decoded_events)
        new_pairs: List[NewPairEvent] = []

        for event in events:
            parsed = self._extract_pump_fun_create(
                event,
                slot=slot,
            )
            if parsed is not None:
                new_pairs.append(parsed)

        return new_pairs

    @staticmethod
    def _iter_decoded_events(decoded_events: Any) -> List[Any]:
        if decoded_events is None:
            return []

        if isinstance(decoded_events, Mapping) or hasattr(
            decoded_events,
            "event_type",
        ):
            return [decoded_events]

        if isinstance(decoded_events, (str, bytes, bytearray)):
            return []

        try:
            return list(decoded_events)
        except TypeError:
            return []

    @classmethod
    def _extract_pump_fun_create(
        cls,
        event: Any,
        slot: int,
    ) -> Optional[NewPairEvent]:
        event_type = cls._lookup(
            event,
            "event_type",
            "event_name",
            "type",
        )

        if str(event_type) != "PumpFunCreate":
            return None

        args = cls._lookup(event, "args", "decoded_args")
        source = args if args is not None else event

        mint = cls._lookup(
            source,
            "mint",
            "mint_address",
            "token_mint",
        )
        deployer = cls._lookup(
            source,
            "user",
            "deployer",
        )
        bonding_curve = cls._lookup(
            source,
            "bonding_curve",
            "bondingCurve",
            "pool_address",
            "pair_address",
        )
        name = cls._decode_text(
            cls._lookup(source, "name", "brand")
        )
        symbol = cls._decode_text(
            cls._lookup(source, "symbol", "ticker")
        )

        if not mint or not bonding_curve or not name or not symbol:
            logger.warning(
                "Ignoring incomplete PumpFunCreate event: "
                "mint=%s bonding_curve=%s name=%s symbol=%s user=%s slot=%d",
                mint,
                bonding_curve,
                name,
                symbol,
                deployer,
                slot,
            )
            return None

        logger.info(
            "New Pump.fun pair: mint=%s user=%s bonding_curve=%s "
            "name=%s symbol=%s slot=%d",
            mint,
            deployer,
            bonding_curve,
            name,
            symbol,
            slot,
        )

        return NewPairEvent(
            token_address=mint,
            pair_address=bonding_curve,
            dex="pumpfun",
            quote_symbol=symbol,
            block_number=slot,
        )

    @staticmethod
    def _lookup(source: Any, *names: str) -> Any:
        if source is None:
            return None

        for name in names:
            if isinstance(source, Mapping):
                value = source.get(name)
            else:
                value = getattr(source, name, None)

            if value is not None:
                return value

        return None

    @staticmethod
    def _decode_text(value: Any) -> Optional[str]:
        if value is None:
            return None

        if isinstance(value, (bytes, bytearray)):
            return bytes(value).decode("utf-8", errors="replace")

        if isinstance(value, (list, tuple)) and value:
            try:
                return bytes(value).decode("utf-8", errors="replace")
            except (TypeError, ValueError):
                return None

        return str(value)
