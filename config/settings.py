"""
config/settings.py

Central configuration. Loads from environment variables / .env so secrets
(RPC keys, wallet private keys) never live in source control.

SECURITY NOTE: Never hardcode private keys or API keys here or anywhere in
source. Use environment variables locally and a proper secrets manager
(AWS Secrets Manager, HashiCorp Vault, etc.) in production. This file only
defines *where* to look for them.
"""

from __future__ import annotations
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()

import os
from dataclasses import dataclass, field
from typing import List, Optional


def _env_list(key: str, default: Optional[List[str]] = None) -> List[str]:
    raw = os.getenv(key)
    if not raw:
        return default or []
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass
class RpcEndpointConfig:
    url: str
    ws_url: Optional[str] = None
    priority: int = 0            # lower = tried first
    max_requests_per_second: float = 25.0
    weight: float = 1.0          # for load balancing across healthy endpoints


@dataclass
class EthereumConfig:
    """
    Populate ETHEREUM_RPC_URLS with one or more comma-separated HTTPS RPC
    endpoints (e.g. from Alchemy, Infura, your own node) for automatic
    failover. Order = priority.
    """
    
    chain_id_numeric: int = 1
    rpc_endpoints: List[RpcEndpointConfig] = field(default_factory=list)
    ws_endpoint: Optional[str] = None
    native_symbol: str = "ETH"
    average_block_time_seconds: float = 12.0
    wrapped_native_address: str = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # WETH mainnet
    default_router_address: Optional[str] = None  # e.g. Uniswap V2/V3 router
    multicall_address: str = "0xcA11bde05977b3631167028862bE2a173976CA11"
    known_quote_assets: List[str] = field(default_factory=lambda: [
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
        "0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
    ])

    @classmethod
    def from_env(cls) -> "EthereumConfig":
        urls = _env_list("ETHEREUM_RPC_URLS")
        endpoints = [
            RpcEndpointConfig(url=url, priority=i) for i, url in enumerate(urls)
        ]
        return cls(
            rpc_endpoints=endpoints,
            ws_endpoint=os.getenv("ETHEREUM_WS_URL"),
            default_router_address=os.getenv("ETHEREUM_DEFAULT_ROUTER"),
        )


@dataclass
class BaseConfig(EthereumConfig):
    """Base chain config. Inherits EthereumConfig; overrides defaults."""
    chain_id_numeric: int = 8453
    native_symbol: str = "ETH"
    average_block_time_seconds: float = 2.0
    wrapped_native_address: str = "0x4200000000000000000000000000000000000006"  # WETH on Base
    default_router_address: str = "0x2626664c2603336E57B271c5C0b26F421741e481"  # Uniswap V3 Router on Base
    known_quote_assets: list = field(default_factory=lambda: [
        "0x4200000000000000000000000000000000000006",  # WETH
        "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC
    ])

    @classmethod
    def from_env(cls) -> "BaseConfig":
        """
        Load Base config from environment. Reads:
          BASE_HTTP_URL  — comma-separated HTTPS RPC endpoints (Alchemy)
          BASE_WSS_URL   — WebSocket endpoint for eth_subscribe
        """
        http_url = os.getenv("BASE_HTTP_URL", "").strip()
        ws_url   = os.getenv("BASE_WSS_URL", "").strip() or None

        rpc_urls = [u.strip() for u in http_url.split(",") if u.strip()] if http_url else []
        endpoints = [
            RpcEndpointConfig(url=u, priority=i, weight=1.0)
            for i, u in enumerate(rpc_urls)
        ]

        return cls(
            rpc_endpoints=endpoints,
            ws_endpoint=ws_url,
            default_router_address=os.getenv(
                "BASE_DEFAULT_ROUTER",
                "0x2626664c2603336E57B271c5C0b26F421741e481",
            ),
        )



@dataclass
class RiskConfig:
    """
    Defaults are deliberately conservative. This is a placeholder for the
    full risk-management module you'll be adding later — wiring it here now
    so the chain adapters have somewhere to report data (liquidity, honeypot
    sim results, contract verification) that risk checks will consume.
    """
    min_liquidity_usd: float = 100.0
    max_position_size_usd: float = 500.0
    max_slippage_bps: int = 300          # 3%
    require_verified_contract: bool = True
    require_honeypot_sim_pass: bool = True
    max_gas_cost_pct_of_position: float = 5.0

    @classmethod
    def from_env(cls) -> "RiskConfig":
        return cls(
            min_liquidity_usd=float(os.getenv("MIN_LIQUIDITY_USD", cls.min_liquidity_usd)),
            max_position_size_usd=float(os.getenv("MAX_POSITION_SIZE_USD", cls.max_position_size_usd)),
            max_slippage_bps=int(os.getenv("MAX_SLIPPAGE_BPS", cls.max_slippage_bps)),
        )

@dataclass
class ScraperConfig:
    mode: str = "polling"
    poll_interval_seconds: float = 60.0
    initial_lookback_blocks: int = 50
    max_block_range_per_query: int = 10
    confirmation_blocks: int = 2
    watch_uniswap_v2: bool = True
    watch_uniswap_v3: bool = True
    watch_aerodrome: bool = True
    pending_max_attempts: int = 20

    @classmethod
    def from_env(cls) -> "ScraperConfig":
        return cls(
            poll_interval_seconds=float(os.getenv("SCRAPER_POLL_INTERVAL_SECONDS", cls.poll_interval_seconds)),
            initial_lookback_blocks=int(os.getenv("SCRAPER_INITIAL_LOOKBACK_BLOCKS", cls.initial_lookback_blocks)),
            max_block_range_per_query=int(os.getenv("SCRAPER_MAX_BLOCK_RANGE", cls.max_block_range_per_query)),
            confirmation_blocks=int(os.getenv("SCRAPER_CONFIRMATION_BLOCKS", cls.confirmation_blocks)),
            mode=os.getenv("SCRAPER_MODE", cls.mode),
            pending_max_attempts=int(os.getenv("SCRAPER_PENDING_MAX_ATTEMPTS", cls.pending_max_attempts)),
         
        )

@dataclass
class BotConfig:
    ethereum: EthereumConfig = field(default_factory=EthereumConfig.from_env)
    base: BaseConfig = field(default_factory=BaseConfig.from_env)
    scraper: ScraperConfig = field(default_factory=ScraperConfig.from_env)
    risk: RiskConfig = field(default_factory=RiskConfig.from_env)
    dry_run: bool = True   # IMPORTANT: default to simulation-only; require explicit opt-in to trade live
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "BotConfig":
        return cls(
            ethereum=EthereumConfig.from_env(),
            base=BaseConfig.from_env(),
            scraper=ScraperConfig.from_env(),
            risk=RiskConfig.from_env(),
            dry_run=os.getenv("BOT_DRY_RUN", "true").lower() != "false",
            log_level=os.getenv("BOT_LOG_LEVEL", "INFO"),
        )
