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
                        logger.info("RAW WS: %s", str(message)[:200])
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
                    "mentions": [PUMP_FUN_PROGRAM_ID],
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
        # websockets delivers text frames as `str` — parse JSON first
        if isinstance(message, (str, bytes, bytearray)):
            try:
                message = json.loads(message)
            except Exception:
                return []

        if not isinstance(message, Mapping):
            return []

        method = message.get("method")

        # Subscription confirmation — capture the subscription ID
        if method == "logsSubscribe":
            params = message.get("params")
            if isinstance(params, Mapping):
                sub_id = params.get("result")
                if sub_id is not None:
                    self._subscription_id = str(sub_id)
            return []

        if method != "logsNotification":
            return []

        params = message.get("params")
        if not isinstance(params, Mapping):
            return []

        # Optional strict subscription ID check
        notification_subscription = params.get("subscription")
        if (
            self._subscription_id is not None
            and notification_subscription is not None
            and str(notification_subscription) != self._subscription_id
        ):
            return []

        result = params.get("result")
        if not isinstance(result, Mapping):
            return []

        # logsSubscribe format: result.value.logs (list) + result.value.signature
        value = result.get("value")
        if not isinstance(value, Mapping):
            return []

        logs = value.get("logs")
        if not isinstance(logs, list) or not logs:
            return []

        signature = value.get("signature", "")
        slot_raw = result.get("context", {}).get("slot")
        try:
            slot = int(slot_raw) if slot_raw is not None else 0
        except (TypeError, ValueError):
            slot = 0

        try:
            decoded_events = sol_parser.parse_logs_only(
                logs,
                signature,
                slot,
                None,
            )
        except Exception as exc:
            logger.warning("parse_logs_only failed: %s", exc, exc_info=True)
            return []


        events = self._iter_decoded_events(decoded_events)
        new_pairs: List[NewPairEvent] = []

        for event in events:
            parsed = self._extract_pump_fun_create(event, slot=slot)
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

        # event_type is an Enum — compare its value, not its repr
        if hasattr(event_type, "value"):
            event_type_value = str(event_type.value)
        else:
            event_type_value = str(event_type).rsplit(".", 1)[-1]

        if event_type_value != "PumpFunCreate":
            return None

        # TEMP: dump the first PumpFunCreate event structure
        if not getattr(cls, "_dumped_once", False):
            cls._dumped_once = True
            logger.warning("DUMP PumpFunCreate event:")
            logger.warning("  dir: %s", [a for a in dir(event) if not a.startswith("_")])
            data_field = getattr(event, "data", None)
            if isinstance(data_field, dict):
                logger.warning("  data keys: %s", list(data_field.keys()))
                for k, v in data_field.items():
                    logger.warning("    %s = %r", k, v)

        # PumpFunCreate puts all fields inside event.data (a PumpFunCreateEvent
        # dataclass), not directly on the DexEvent wrapper.
        args = cls._lookup(event, "args", "decoded_args")
        if args is None:
            args = cls._lookup(event, "data")
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
            quote_symbol="SOL",
            block_number=slot,
            deployer=deployer,
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
