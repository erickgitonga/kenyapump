# main.py
import asyncio
import logging

# === Core ===
from core.chain_router import ChainRouter
from core.models import TokenInfo, ChainId
from config.settings import BotConfig
from chains.ethereum import EthereumAdapter

# === Data ===
from data.dexscreener import DexscreenerClient
from data.geckoterminal import GeckoTerminalClient
from data.scraper import TokenScraper
from data.solana_scraper import SolanaTokenScraper
from ai.honeypot import GoPlusClient, HoneypotDetector
from ai.quick_screen import QuickScreen
from notifications.telegram import TelegramNotifier
from intelligence.reputation import ReputationStore
from chains.base import BaseAdapter
from chains.solana_adapter import SolanaAdapter
from config.settings import BotConfig


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("KenyaPump")

# WETH on Ethereum mainnet — used here just as a known-liquid token to
# smoke-test the data layer. Swap for whatever token you actually want to
# watch once this confirms everything's wired correctly.
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"


async def _wrap_scraper_task(name: str, coro):
    """Run a scraper coroutine and log any crash with full traceback."""
    try:
        await coro
    except asyncio.CancelledError:
        log.info("[%s] scraper cancelled", name)
        raise
    except Exception as exc:
        log.exception("[%s] scraper crashed: %s", name, exc)
        raise


def _solana_event_to_token_info(event) -> TokenInfo:
    """Convert a Solana NewPairEvent into a chain-agnostic TokenInfo."""
    return TokenInfo(
        chain=ChainId.SOLANA,
        address=event.token_address,
        symbol=event.symbol or "UNKNOWN",
        decimals=6,   # Pump.fun SPL tokens use 6 decimals
        name=event.name,
        metadata={
            "pair_address": event.pair_address,
            "dex": event.dex,
            "paired_with": event.quote_symbol,
            "discovered_at_block": event.block_number,
            "deployer": event.deployer,
        },
    )


async def main():
    log.info("KenyaPump starting...")

    # ─────────────────────────────────────────────
    # 1. CONFIG
    # ─────────────────────────────────────────────
    config = BotConfig.from_env()  # .from_env(), not bare BotConfig() — dry_run/log_level need it
    log.info(f"Config loaded (dry_run={config.dry_run})")

    # ─────────────────────────────────────────────
    # 2. CHAIN LAYER
    # ─────────────────────────────────────────────
    router = ChainRouter()
    router.register(EthereumAdapter(config.ethereum))
    router.register(BaseAdapter(config.base)) 
    router.register(SolanaAdapter())
    log.info(f"Chains registered: {router.registered_chains()}")
    

    await router.connect_all()
    health = await router.health_check_all()
    for chain_id, status in health.items():
        log.info(f"{chain_id.value}: healthy={status.is_healthy} "
                 f"block={status.latest_block} latency={status.latency_ms:.0f}ms")

    # ─────────────────────────────────────────────
    # 3. DATA LAYER
    # ─────────────────────────────────────────────
    dexscreener = DexscreenerClient()
    geckoterminal = GeckoTerminalClient()
    log.info("Data clients initialized")

    ethereum_adapter = router.get(ChainId.ETHEREUM)
    base_adapter = router.get(ChainId.BASE)

    # One scraper per chain. Same enrichment pipeline, same filters — the
    # only difference is which factory events each one subscribes to.
    eth_scraper = TokenScraper(
        eth_adapter=ethereum_adapter,
        dexscreener=dexscreener,
        config=config.scraper,
        # 0 here on purpose: filtering by min_liquidity_usd at discovery time
        # hides pairs in the first seconds after creation, when liquidity is
        # naturally lowest — see the caveat in data/scraper.py's docstring.
        # Apply config.risk.min_liquidity_usd later, at the point you'd
        # actually decide to trade, not at discovery.
        min_liquidity_usd=0.0,
    )
    base_scraper = TokenScraper(
        eth_adapter=base_adapter,
        dexscreener=dexscreener,
        config=config.scraper,
        min_liquidity_usd=0.0,
    )
    solana_scraper = SolanaTokenScraper()

    # Initialize screener + honeypot detector once, reuse across all tokens
    goplus = GoPlusClient()
    honeypot_detector = HoneypotDetector(goplus)
    notifier = TelegramNotifier()
    reputation = ReputationStore()
    # QuickScreen needs the chain adapter — build one per chain on demand
    _quick_screens = {}

    def _get_quick_screen(chain):
        if chain not in _quick_screens:
            adapter = router.get(chain)
            _quick_screens[chain] = QuickScreen(adapter)
        return _quick_screens[chain]

    async def on_tokens_found(tokens):
        for t in tokens:
            chain_label = t.chain.value if hasattr(t, "chain") else "?"
            base_msg = (
                f"{t.symbol} ({t.address}) | "
                f"liq=${t.metadata.get('liquidity_usd')} | "
                f"price=${t.metadata.get('price_usd')} | "
                f"dex={t.metadata.get('dex')}"
            )

            # ─── Layer 1: Quick screen (2-3s) — reject obvious junk ───
            try:
                launch_block = int(t.metadata.get("discovered_at_block", 0))
                if launch_block > 0:
                    qs = _get_quick_screen(t.chain)
                    qv = await qs.check(t.address, launch_block)
                    if qv.verdict == "REJECT":
                        log.warning(
                            f"[REJECT] QUICK REJECT [{chain_label}]: {base_msg} | "
                            f"({qv.latency_ms:.0f}ms) {qv.reason}"
                        )
                        # Record even rejected tokens — the deployer is the signal
                        if qv.deployer_address:
                            try:
                                await reputation.record_edge(
                                    wallet_address=qv.deployer_address,
                                    token_address=t.address,
                                    token_symbol=t.symbol,
                                    chain=chain_label,
                                    role="deployer",
                                    block_number=launch_block,
                                )
                            except Exception as e:
                                log.debug(f"reputation record failed: {e}")
                        continue  # skip honeypot + deep analysis
                    elif qv.verdict == "WATCH":
                        # Wallet tracker integration for WATCH
                        try:
                            from data.wallet_tracker import get_top_holders
                            holders = await get_top_holders(
                                adapter=qs._eth,
                                token_address=t.address,
                                launch_block=launch_block,
                                top_n=5,
                            )
                            for h in holders:
                                await reputation.record_edge(
                                    wallet_address=h.wallet,
                                    token_address=t.address,
                                    token_symbol=t.symbol,
                                    chain=chain_label,
                                    role="top_holder",
                                    block_number=launch_block,
                                )
                        except Exception as e:
                            log.warning(f"Wallet tracker failed for {t.symbol}: {e}")

                        log.warning(
                            f"[WATCH] QUICK WATCH [{chain_label}]: {base_msg} | "
                            f"holders={qv.unique_receivers} "
                            f"largest={qv.largest_receiver_pct:.1%} | {qv.reason}"
                        )
                        if qv.deployer_address:
                            try:
                                await reputation.record_edge(
                                    wallet_address=qv.deployer_address,
                                    token_address=t.address,
                                    token_symbol=t.symbol,
                                    chain=chain_label,
                                    role="deployer",
                                    block_number=launch_block,
                                )
                            except Exception as e:
                                log.debug(f"reputation record failed: {e}")
                        await notifier.send(
                            f"<b>⚠️ WATCH</b> [{chain_label}] <code>{t.symbol}</code>\n"
                            f"Liq: ${t.metadata.get('liquidity_usd', '?')}\n"
                            f"Holders: {qv.unique_receivers}\n"
                            f"Top: {qv.largest_receiver_pct:.1%}\n"
                            f"<i>{qv.reason}</i>"
                        )
                    else:
                        # Wallet tracker integration for PASS
                        try:
                            from data.wallet_tracker import get_top_holders
                            holders = await get_top_holders(
                                adapter=qs._eth,
                                token_address=t.address,
                                launch_block=launch_block,
                                top_n=5,
                            )
                            for h in holders:
                                await reputation.record_edge(
                                    wallet_address=h.wallet,
                                    token_address=t.address,
                                    token_symbol=t.symbol,
                                    chain=chain_label,
                                    role="top_holder",
                                    block_number=launch_block,
                                )
                        except Exception as e:
                            log.warning(f"Wallet tracker failed for {t.symbol}: {e}")

                        log.info(
                            f"[PASS] QUICK PASS [{chain_label}]: {base_msg} | "
                            f"({qv.latency_ms:.0f}ms) {qv.reason}"
                        )
                        await notifier.alert_pass(
                            chain=chain_label,
                            symbol=t.symbol,
                            address=t.address,
                            liquidity=f"${t.metadata.get('liquidity_usd', '?')}",
                            price=f"${t.metadata.get('price_usd', '?')}",
                            dex=str(t.metadata.get("dex", "?")),
                            holders=qv.unique_receivers,
                            top_pct=float(qv.largest_receiver_pct or 0),
                            reason=qv.reason,
                        )
                else:
                    # launch_block == 0, skip quick screen and wallet tracker
                    pass
            except Exception as exc:
                log.debug(f"quick_screen failed for {t.symbol}: {exc}")

            # ─── Layer 2: Honeypot / holder / safety check ───────────
            try:
                report = await honeypot_detector.assess(t.chain, t.address)
            except Exception as exc:
                log.warning(f"⚠️  [{chain_label}] {t.symbol} — honeypot check failed: {exc}")
                continue

            risk = report.risk_level.name.lower() if hasattr(report.risk_level, "name") else str(report.risk_level)
            flags = " | ".join(report.flags) if report.flags else "no flags"

            if risk in ("danger", "critical"):
                log.warning(
                    f"[DANGER] REJECT [{chain_label}]: {base_msg} | "
                    f"risk={report.risk_score}/100 | {flags}"
                )
                await notifier.alert_danger(
                    chain=chain_label,
                    symbol=t.symbol,
                    address=t.address,
                    risk_score=report.risk_score,
                    flags=flags,
                )
            elif risk == "medium":
                log.warning(
                    f"[CAUTION] CAUTION [{chain_label}]: {base_msg} | "
                    f"risk={report.risk_score}/100 | holders={report.holder_count} | "
                    f"top_holder={report.top_holder_pct} | {flags}"
                )
            else:
                log.info(
                    f"[SAFE] SCORE ME [{chain_label}]: {base_msg} | "
                    f"risk={report.risk_score}/100 | holders={report.holder_count} | "
                    f"top_holder={report.top_holder_pct} | {flags}"
                )

    # ─────────────────────────────────────────────
    # 4. MAIN LOOP — one scraper task per chain
    # ─────────────────────────────────────────────
    # Solana bridge: convert NewPairEvent -> TokenInfo before shared handler
    async def solana_on_tokens_found(events):
        if not events:
            return
        token_infos = [_solana_event_to_token_info(e) for e in events]
        await on_tokens_found(token_infos)

    scraper_tasks = []

    # Ethereum scraper
    if config.scraper.mode == "websocket" and config.ethereum.ws_endpoint:
        log.info("Starting Ethereum scraper (WebSocket mode)")
        scraper_tasks.append(asyncio.create_task(
            _wrap_scraper_task(
                'eth',
                eth_scraper.run_forever_ws(
                    config.ethereum.ws_endpoint, on_tokens_found=on_tokens_found
                ),
            )
        ))
    else:
        log.info("Starting Ethereum scraper (polling mode)")
        scraper_tasks.append(asyncio.create_task(
            _wrap_scraper_task(
                'eth',
                eth_scraper.run_forever(on_tokens_found=on_tokens_found),
            )
        ))

    # Base scraper
    if config.scraper.mode == "websocket" and config.base.ws_endpoint:
        log.info("Starting Base scraper (WebSocket mode)")
        scraper_tasks.append(asyncio.create_task(
            _wrap_scraper_task(
                'base',
                base_scraper.run_forever_ws(
                    config.base.ws_endpoint, on_tokens_found=on_tokens_found
                ),
            )
        ))
    else:
        log.info("Starting Base scraper (polling mode)")
        scraper_tasks.append(asyncio.create_task(
            _wrap_scraper_task(
                'base',
                base_scraper.run_forever(on_tokens_found=on_tokens_found),
            )
        ))

    # Solana scraper (Pump.fun only for v1)
    if config.scraper.mode == "websocket":
        log.info("Starting Solana scraper (WebSocket mode, Pump.fun)")
        scraper_tasks.append(asyncio.create_task(
            _wrap_scraper_task(
                'solana',
                solana_scraper.run_forever(on_tokens_found=solana_on_tokens_found),
            )
        ))

    # Send startup notification
    await notifier.startup(
        chains=[c.value for c in router.registered_chains()],
        mode=config.scraper.mode,
    )

    try:
        await asyncio.gather(*scraper_tasks)

    except KeyboardInterrupt:
        log.info("Shutdown signal received")

    finally:
        # ─────────────────────────────────────────
        # 5. CLEANUP (important — close aiohttp sessions)
        # ─────────────────────────────────────────
        log.info("Cleaning up...")
        for s in (eth_scraper, base_scraper):
            s.stop()
        for task in scraper_tasks:
            task.cancel()
        for task in scraper_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        await reputation.close()
        await notifier.close()
        await goplus.close()
        await dexscreener.close()
        await geckoterminal.close()
        await router.disconnect_all()
        log.info("Cleanup complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped.")
