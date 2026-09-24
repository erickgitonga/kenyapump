"""
data/scraper.py

Detects newly created liquidity pools on Ethereum and turns them into
scoreable TokenInfo records. This is the module that makes the bot
"aware" rather than just "connected."

HOW NEW-PAIR DETECTION ACTUALLY WORKS HERE (read this before assuming it
works differently):

Dexscreener's free API has no "give me every new pair on Ethereum" endpoint
- only lookups for pairs/tokens you already know the address of. So this
does NOT poll Dexscreener for new pairs. Instead it listens directly to the
blockchain for PairCreated (Uniswap V2) and PoolCreated (Uniswap V3) events
via eth_getLogs on your existing RPC connection - free, and the same
mechanism real sniping bots use. Dexscreener is then used to *enrich* each
freshly-discovered pair with liquidity/price, which is often available
within seconds but occasionally lags - pairs not yet indexed there are
retried for a few cycles before being dropped (see ScraperConfig).

IMPORTANT CAVEAT on min_liquidity_usd filtering: liquidity in a pool is
lowest right at creation and ramps up over the following blocks/minutes as
the deployer and early buyers add to it. Filtering at discovery time by
RiskConfig.min_liquidity_usd means you will see almost nothing in the first
few seconds after a pair is created - which may be exactly the window a
"sniping" strategy cares about most. If that matters for your strategy,
call scan_once() with a liquidity threshold of 0 and apply the real
liquidity filter later, right before you'd actually trade - not here.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Optional, Set

from eth_abi import decode as abi_decode
from web3 import Web3

from chains.ethereum import EthereumAdapter
from config.settings import ScraperConfig
from core.models import ChainId, TokenInfo
from data.dexscreener import DexscreenerClient
from data.persistence import ScraperStore

log = logging.getLogger("kenyapump.data.scraper")

# --- Factory addresses (Ethereum mainnet) -----------------------------

# Ethereum mainnet factories (kept for backward compat; use
# FACTORIES_BY_CHAIN[chain_id] instead of these directly).
UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
UNISWAP_V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984"

# Base mainnet factories.
UNISWAP_V2_FACTORY_BASE = "0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6"
UNISWAP_V3_FACTORY_BASE = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"
AERODROME_SLIPSTREAM_FACTORY = "0x420DD381b31aEf6683db6B902084cB0FFECe40Da"

# keccak256 of the event signatures - these are what topics[0] must equal.
# NOTE: use Web3.to_hex(), not .hex() on the raw HexBytes result - whether
# .hex() includes the "0x" prefix depends on the installed hexbytes
# version, and a missing prefix makes eth_getLogs reject the whole request
# with a 400 rather than failing obviously. Web3.to_hex() always includes it.
_PAIR_CREATED_TOPIC = Web3.to_hex(Web3.keccak(text="PairCreated(address,address,address,uint256)"))
_POOL_CREATED_TOPIC = Web3.to_hex(
    Web3.keccak(text="PoolCreated(address,address,uint24,int24,address)")
)

# --- Base / Aerodrome Slipstream (Uniswap V3 fork) --------------------
# Aerodrome's PoolCreated signature differs from Uniswap V3: the third
# argument is `bool stable`, not `uint24 fee`. Different topic hash.

_AERODROME_POOL_CREATED_TOPIC = Web3.to_hex(
    Web3.keccak(text="PoolCreated(address,address,bool,address,uint256)")
)

# Per-chain factory map: chain_id.value -> [(factory_addr, topic, dex_label), ...]
FACTORIES_BY_CHAIN = {
    "ethereum": [
        (UNISWAP_V2_FACTORY, _PAIR_CREATED_TOPIC, "uniswap_v2"),
        (UNISWAP_V3_FACTORY, _POOL_CREATED_TOPIC, "uniswap_v3"),
    ],
    "base": [
        (UNISWAP_V2_FACTORY_BASE, _PAIR_CREATED_TOPIC, "uniswap_v2"),
        (UNISWAP_V3_FACTORY_BASE, _POOL_CREATED_TOPIC, "uniswap_v3"),
        (AERODROME_SLIPSTREAM_FACTORY, _AERODROME_POOL_CREATED_TOPIC, "aerodrome_slipstream"),
    ],
}


@dataclass
class _PendingCandidate:
    token_address: str
    pair_address: str
    dex: str
    quote_symbol: str
    block_number: int
    deployer: Optional[str] = None
    attempts: int = 0


@dataclass
class NewPairEvent:
    """Raw discovery result before liquidity enrichment - useful if you
    want to react to a pair's existence before Dexscreener has indexed it
    (e.g. to queue a honeypot simulation immediately)."""
    token_address: str
    pair_address: str
    dex: str
    quote_symbol: str
    block_number: int
    deployer: Optional[str] = None
    name: Optional[str] = None
    symbol: Optional[str] = None


class TokenScraper:
    """
    Watches Uniswap V2/V3 factories for new pools and turns qualifying ones
    into TokenInfo records. Ethereum-specific by design - see the
    EVM-specific extensions note in chains/ethereum.py for why this isn't
    built against the chain-agnostic BaseChainAdapter interface.
    """

    def __init__(
        self,
        eth_adapter: EthereumAdapter,
        dexscreener: DexscreenerClient,
        config: ScraperConfig,
        min_liquidity_usd: float = 0.0,
        store: Optional[ScraperStore] = None,
    ):
        self._eth = eth_adapter
        self._dex = dexscreener
        self._config = config
        self._min_liquidity_usd = min_liquidity_usd
        self._store = store

        self._quote_assets: Set[str] = {a.lower() for a in eth_adapter.config.known_quote_assets}
        self._last_scanned_block: Optional[int] = None
        self._seen_pairs: Set[str] = set()
        self._pending: Dict[str, _PendingCandidate] = {}  # pair_address -> candidate
        self._stopped = False
        # Learned from the RPC provider's actual behavior - starts at
        # config.max_block_range_per_query but only ever shrinks and stays
        # shrunk, so a provider's real limit is discovered once rather than
        # re-discovered (via repeated failed requests) every poll cycle.
        self._effective_chunk_size = config.max_block_range_per_query

    async def load_state(self) -> None:
        """Call once after construction, before scan_once()/run_forever(),
        to resume from persisted state instead of starting cold. Safe to
        skip (or call with no store configured) - falls back to
        in-memory-only behavior, same as before persistence existed."""
        if self._store is None:
            return

        self._last_scanned_block = await self._store.get_last_scanned_block(ChainId.ETHEREUM)
        self._seen_pairs = await self._store.get_all_seen_pair_addresses()
        persisted_pending = await self._store.get_pending_candidates()
        for p in persisted_pending:
            self._pending[p.pair_address.lower()] = _PendingCandidate(
                token_address=p.token_address,
                pair_address=p.pair_address,
                dex=p.dex,
                quote_symbol=p.quote_symbol,
                block_number=p.block_number,
                attempts=p.attempts,
            )
        log.info(
            "Resumed from persisted state: last_scanned_block=%s, %d seen pairs, %d pending candidates",
            self._last_scanned_block, len(self._seen_pairs), len(self._pending),
        )

    async def scan_once(self) -> List[TokenInfo]:
        """Run one discovery cycle: scan new blocks for factory events,
        retry anything pending enrichment, and return newly qualifying
        tokens. Safe to call repeatedly (e.g. from run_forever or your own
        loop) - internally tracks what's already been scanned/seen."""
        events = await self._scan_new_blocks()
        await self._process_new_events(events)
        return await self._retry_pending()

    async def _process_new_events(self, events: List[NewPairEvent]) -> None:
        """Register newly discovered events into the pending queue
        (deduped against self._seen_pairs) and persist them. Shared by both
        polling (run_forever) and push-based (run_forever_ws) detection -
        detection mechanism differs, but what happens after "we found a new
        pair" is identical either way."""
        for event in events:
            if event.pair_address.lower() in self._seen_pairs:
                continue
            self._seen_pairs.add(event.pair_address.lower())
            self._pending[event.pair_address.lower()] = _PendingCandidate(
                token_address=event.token_address,
                pair_address=event.pair_address,
                dex=event.dex,
                quote_symbol=event.quote_symbol,
                block_number=event.block_number,
            )
            if self._store:
                await self._store.mark_pending(
                    event.token_address, event.pair_address, event.dex,
                    event.quote_symbol, event.block_number, attempts=0,
                )
            log.info(
                "New pair detected: %s (dex=%s, paired_with=%s, block=%d) - awaiting liquidity data",
                event.token_address, event.dex, event.quote_symbol, event.block_number,
            )

    async def _retry_pending(self) -> List[TokenInfo]:
        """Re-attempt Dexscreener enrichment for everything currently
        pending. Shared by both detection modes - WebSocket mode still
        needs this on a timer, since detecting a pair instantly doesn't
        make Dexscreener index it any faster."""
        found: List[TokenInfo] = []
        resolved_this_cycle: List[str] = []
        for pair_addr, candidate in list(self._pending.items()):
            candidate.attempts += 1
            token_info = await self._try_enrich(candidate)
            if token_info is not None:
                found.append(token_info)
                resolved_this_cycle.append(pair_addr)
                if self._store:
                    await self._store.mark_qualified(
                        pair_addr, token_info.symbol,
                        token_info.metadata.get("liquidity_usd", ""),
                        token_info.metadata.get("price_usd"),
                    )
            elif candidate.attempts >= self._config.pending_max_attempts:
                log.info(
                    "Giving up on %s after %d attempts - never indexed by Dexscreener or liquidity stayed below threshold",
                    candidate.token_address, candidate.attempts,
                )
                resolved_this_cycle.append(pair_addr)
                if self._store:
                    await self._store.mark_given_up(pair_addr)
            elif self._store:
                await self._store.mark_pending(
                    candidate.token_address, candidate.pair_address, candidate.dex,
                    candidate.quote_symbol, candidate.block_number, candidate.attempts,
                )

        for pair_addr in resolved_this_cycle:
            self._pending.pop(pair_addr, None)

        return found

    async def run_forever(
        self, on_tokens_found: Optional[Callable[[List[TokenInfo]], Awaitable[None]]] = None
    ) -> None:
        """Poll scan_once() on ScraperConfig.poll_interval_seconds until
        cancelled. Call stop() or cancel the containing task for graceful
        shutdown."""
        log.info(
            "TokenScraper starting (poll every %.0fs, v2=%s, v3=%s)",
            self._config.poll_interval_seconds,
            self._config.watch_uniswap_v2,
            self._config.watch_uniswap_v3,
        )
        try:
            while not self._stopped:
                try:
                    tokens = await self.scan_once()
                    if tokens and on_tokens_found:
                        await on_tokens_found(tokens)
                except Exception:  # noqa: BLE001 - a bad cycle shouldn't kill the scanner
                    log.exception("Error during scraper poll cycle; will retry next cycle")
                await asyncio.sleep(self._config.poll_interval_seconds)
        except asyncio.CancelledError:
            log.info("TokenScraper stopping (cancelled)")
            raise

    async def run_forever_ws(
        self, ws_url: str, on_tokens_found: Optional[Callable[[List[TokenInfo]], Awaitable[None]]] = None
    ) -> None:
        """WebSocket-based alternative to run_forever(): subscribes
        directly to PairCreated/PoolCreated events instead of polling
        eth_getLogs on an interval, so detection happens within roughly a
        block time instead of up to poll_interval_seconds late.

        IMPORTANT: uses TWO SEPARATE subscriptions (one per factory), not
        one combined filter with an address array and OR'd topics. That
        combined form is accepted without error by Alchemy but silently
        matches nothing - confirmed empirically, not a guess. Don't
        "simplify" this back to one subscription without testing against
        a live provider first.

        A background timer still retries pending Dexscreener enrichment on
        poll_interval_seconds - instant detection doesn't make Dexscreener
        index any faster, so that part is unchanged from run_forever().

        Requires the `web3` extras this project already depends on; the
        WebSocket-specific classes are imported locally here (not at module
        level) so the polling path (run_forever) keeps working even in
        environments/web3 versions where the WS provider name differs.
        """
        from web3 import AsyncWeb3, Web3, WebSocketProvider

        log.info(
            "TokenScraper starting in WebSocket mode (v2=%s, v3=%s)",
            self._config.watch_uniswap_v2, self._config.watch_uniswap_v3,
        )

        async def retry_loop() -> None:
            while not self._stopped:
                await asyncio.sleep(self._config.poll_interval_seconds)
                try:
                    tokens = await self._retry_pending()
                    if tokens and on_tokens_found:
                        await on_tokens_found(tokens)
                except Exception:  # noqa: BLE001 - a bad retry cycle shouldn't kill the listener
                    log.exception("Error during WS retry cycle; will retry next cycle")

        retry_task = asyncio.create_task(retry_loop())

        try:
            async with AsyncWeb3(WebSocketProvider(ws_url)) as w3:
                chain_key = self._eth.chain_id.value
                factories = FACTORIES_BY_CHAIN.get(chain_key, [])
                log.info(
                    "Subscribing to %d %s factory event stream(s)",
                    len(factories), chain_key,
                )
                if not factories:
                    log.error(
                        "No factories configured for chain=%s - subscription will be empty",
                        chain_key,
                    )

                subs: Dict[str, str] = {}
                for factory_addr, topic, dex_label in factories:
                    sub_id = await w3.eth.subscribe("logs", {
                        "address": Web3.to_checksum_address(factory_addr),
                        "topics": [topic],
                    })
                    subs[sub_id] = dex_label
                log.info("Subscribed to %d factory event stream(s)", len(subs))

                async for message in w3.socket.process_subscriptions():
                    if self._stopped:
                        break

                    # WebSocketProvider.process_subscriptions() yields
                    # ALREADY-UNWRAPPED payloads - no "params" envelope:
                    #   {'subscription': '<sub_id>', 'result': {...log...}}
                    sub_id = message.get("subscription")
                    dex_label = subs.get(sub_id)
                    if dex_label is None:
                        continue

                    raw_log = message.get("result")
                    if raw_log is None:
                        continue
                    raw_log = dict(raw_log)

                    event = self._parse_log(raw_log, dex_label)
                    if event is None:
                        continue

                    await self._process_new_events([event])
                    # One immediate enrichment attempt for the fast path -
                    # the retry_loop background task picks up anything that
                    # isn't indexed yet on its own schedule.
                    tokens = await self._retry_pending()
                    if tokens and on_tokens_found:
                        await on_tokens_found(tokens)

                for sub_id in subs:
                    try:
                        await w3.eth.unsubscribe(sub_id)
                    except Exception:  # noqa: BLE001 - best-effort cleanup
                        pass
        except asyncio.CancelledError:
            log.info("TokenScraper (WebSocket mode) stopping (cancelled)")
            raise
        finally:
            retry_task.cancel()
            try:
                await retry_task
            except asyncio.CancelledError:
                pass

    def stop(self) -> None:
        self._stopped = True

    # --- internals ---------------------------------------------------

    async def _scan_new_blocks(self) -> List[NewPairEvent]:
        latest = await self._eth.get_latest_block_number()
        safe_tip = latest - self._config.confirmation_blocks
        if safe_tip < 0:
            return []

        if self._last_scanned_block is None:
            from_block = max(0, safe_tip - self._config.initial_lookback_blocks)
        else:
            from_block = self._last_scanned_block + 1

        if from_block > safe_tip:
            return []  # nothing new since last scan

        events: List[NewPairEvent] = []
        chain_key = self._eth.chain_id.value
        factories = FACTORIES_BY_CHAIN.get(chain_key, [])
        for factory_addr, topic, dex_label in factories:
            events.extend(
                await self._scan_factory(
                    factory_addr, topic, dex_label, from_block, safe_tip
                )
            )

        self._last_scanned_block = safe_tip
        if self._store:
            await self._store.set_last_scanned_block(ChainId.ETHEREUM, safe_tip)
        return events

    async def _scan_factory(
        self, factory_address: str, topic: str, dex_label: str, from_block: int, to_block: int
    ) -> List[NewPairEvent]:
        """Adaptive-chunking wrapper around get_logs: starts at the current
        learned chunk size (self._effective_chunk_size), halves - and keeps
        that smaller size for all future calls, not just this one - on
        failure, and gives up on a sub-range only once it's down to a
        single block. Persisting the learned size means a provider's real
        limit gets discovered once, not re-discovered via failed requests
        every poll cycle."""
        results: List[NewPairEvent] = []
        cursor = from_block

        while cursor <= to_block:
            chunk_end = min(cursor + self._effective_chunk_size - 1, to_block)
            try:
                logs = await self._eth.get_logs(
                    address=factory_address,
                    topics=[topic],
                    from_block=cursor,
                    to_block=chunk_end,
                )
                for raw_log in logs:
                    parsed = self._parse_log(raw_log, dex_label)
                    if parsed is not None:
                        results.append(parsed)
                cursor = chunk_end + 1
            except Exception as exc:  # noqa: BLE001 - provider-specific range errors vary in shape
                if self._effective_chunk_size <= 1:
                    log.error(
                        "get_logs failed on a single block (%d) for %s; skipping block. Error: %s",
                        cursor, dex_label, exc,
                    )
                    cursor += 1
                    continue
                self._effective_chunk_size = max(1, self._effective_chunk_size // 2)
                log.warning(
                    "get_logs failed for %s range [%d, %d], permanently reducing chunk size to %d and retrying: %s",
                    dex_label, cursor, chunk_end, self._effective_chunk_size, exc,
                )

        return results

    def _parse_log(self, raw_log: dict, dex_label: str) -> Optional[NewPairEvent]:
        try:
            topics = raw_log["topics"]
            token0 = self._address_from_topic(topics[1])
            token1 = self._address_from_topic(topics[2])
            block_number = raw_log["blockNumber"]
            data = raw_log["data"]
            data_bytes = bytes.fromhex(data.hex()[2:]) if hasattr(data, "hex") else bytes.fromhex(data[2:])

            if dex_label == "uniswap_v2":
                pair_address, _all_pairs_length = abi_decode(
                    ["address", "uint256"], data_bytes
                )
            elif dex_label == "uniswap_v3":
                _tick_spacing, pair_address = abi_decode(
                    ["int24", "address"], data_bytes
                )
            elif dex_label == "aerodrome_slipstream":
                # PoolCreated(address indexed token0, address indexed token1,
                #             bool stable, address pool, uint256)
                # Non-indexed args: (bool stable, address pool, uint256)
                stable, pair_address, _pool_id = abi_decode(
                    ["bool", "address", "uint256"], data_bytes
                )
                # Stable pairs are stablecoin-to-stablecoin - skip them.
                if stable:
                    return None
            else:
                log.warning("Unknown dex_label in _parse_log: %s", dex_label)
                return None

            token0_l, token1_l = token0.lower(), token1.lower()
            token0_is_quote = token0_l in self._quote_assets
            token1_is_quote = token1_l in self._quote_assets

            if token0_is_quote and not token1_is_quote:
                candidate, quote = token1, token0
            elif token1_is_quote and not token0_is_quote:
                candidate, quote = token0, token1
            else:
                # Both or neither side is a known quote asset - either a
                # boring stable/WETH pair, or two unknown tokens we have no
                # reliable way to price. Skip either way.
                return None

            quote_symbol = self._symbol_for_quote_asset(quote.lower())
            return NewPairEvent(
                token_address=candidate,
                pair_address=pair_address,
                dex=dex_label,
                quote_symbol=quote_symbol,
                block_number=block_number,
            )
        except Exception as exc:
            # eth_abi can throw NonEmptyPaddingBytes / DecodingError for
            # malformed logs (spam contracts emit garbage topics).
            # Broad catch here prevents one bad log from killing the scraper.
            log.warning("Failed to parse %s factory log: %s", dex_label, exc)
            return None

    def _symbol_for_quote_asset(self, address_lower: str) -> str:
        # Small, fixed mapping for the well-known quote assets configured in
        # EthereumConfig - good enough here since this list is short and
        # stable; avoids an extra RPC round-trip just to label a log line.
        known = {
            # Ethereum
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
            "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
            "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
            # Base
            "0x4200000000000000000000000000000000000006": "WETH",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "USDC",
            "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": "USDbC",
        }
        return known.get(address_lower, address_lower[:10])

    @staticmethod
    def _address_from_topic(topic) -> str:
        topic_hex = topic.hex() if hasattr(topic, "hex") else topic
        return Web3.to_checksum_address("0x" + topic_hex[-40:])

    async def _try_enrich(self, candidate: _PendingCandidate) -> Optional[TokenInfo]:
        try:
            pools = await self._dex.get_liquidity_info(self._eth.chain_id, candidate.token_address)
        except Exception as exc:  # noqa: BLE001 - Dexscreener hiccup shouldn't drop the candidate permanently
            log.info("Dexscreener lookup failed for %s (attempt %d): %s",
                      candidate.token_address, candidate.attempts, exc)
            return None

        matching = next(
            (p for p in pools if p.pair_address.lower() == candidate.pair_address.lower()),
            pools[0] if pools else None,
        )
        if matching is None:
            return None  # not indexed yet - will retry next cycle, up to pending_max_attempts

        if float(matching.liquidity_usd) < self._min_liquidity_usd:
            log.debug(
                "%s liquidity $%.2f below threshold $%.2f (attempt %d)",
                candidate.token_address, matching.liquidity_usd, self._min_liquidity_usd, candidate.attempts,
            )
            return None

        try:
            token_info = await self._eth.get_token_info(candidate.token_address)
        except Exception as exc:  # noqa: BLE001 - non-standard/malicious contracts can revert on basic calls
            log.warning("get_token_info failed for %s, skipping: %s", candidate.token_address, exc)
            return None

        token_info.metadata.update({
            "pair_address": candidate.pair_address,
            "dex": candidate.dex,
            "paired_with": candidate.quote_symbol,
            "discovered_at_block": candidate.block_number,
            "liquidity_usd": str(matching.liquidity_usd),
            "price_usd": str(matching.price_usd) if matching.price_usd is not None else None,
        })

        log.info(
            "Qualified: %s (%s) | liquidity=$%.2f | price=$%s | dex=%s | paired_with=%s",
            token_info.symbol, candidate.token_address, matching.liquidity_usd,
            matching.price_usd, candidate.dex, candidate.quote_symbol,
        )
        return token_info
