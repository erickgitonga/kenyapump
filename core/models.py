"""
core/models.py

Shared data models used across every chain adapter. Keeping these chain-agnostic
is what lets the rest of the system (risk engine, execution engine, analytics)
work identically regardless of which chain a token lives on.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any
import time


class ChainId(str, Enum):
    ETHEREUM = "ethereum"
    BASE = "base"
    
    # Placeholders for future modules you mentioned you'll add next.
  
    SOLANA = "solana"
    # ARBITRUM = "arbitrum"
    # BSC = "bsc"


class TxStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    DROPPED = "dropped"
    REPLACED = "replaced"


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class GasEstimate:
    """
    Normalized gas estimate. EVM chains express this as base_fee + priority_fee
    (EIP-1559) or a flat gas_price (legacy). Non-EVM chains (future modules)
    will map their fee models onto this same shape so the execution engine
    never has to branch on chain type.
    """
    chain: ChainId
    gas_limit: int
    max_fee_per_gas_wei: Optional[int] = None       # EIP-1559
    max_priority_fee_per_gas_wei: Optional[int] = None
    legacy_gas_price_wei: Optional[int] = None       # pre-1559 fallback
    estimated_cost_native: Decimal = Decimal("0")    # in ETH / SOL / BNB etc.
    estimated_cost_usd: Optional[Decimal] = None
    confidence: str = "medium"                       # low/medium/high
    timestamp: float = field(default_factory=time.time)


@dataclass
class TokenInfo:
    chain: ChainId
    address: str                 # contract/mint address, checksummed for EVM
    symbol: str
    decimals: int
    name: Optional[str] = None
    is_verified_contract: bool = False
    creation_timestamp: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LiquidityInfo:
    chain: ChainId
    token_address: str
    pair_address: str
    dex: str                     # e.g. "uniswap_v2", "uniswap_v3"
    base_token_symbol: str       # usually WETH/SOL/etc
    liquidity_usd: Decimal
    liquidity_native: Decimal
    volume_24h_usd: Optional[Decimal] = None
    price_usd: Optional[Decimal] = None
    price_native: Optional[Decimal] = None
    pool_created_at: Optional[float] = None
    lp_tokens_locked: Optional[bool] = None
    lp_lock_expiry: Optional[float] = None


@dataclass
class WalletBalance:
    chain: ChainId
    address: str
    native_balance: Decimal
    token_balances: Dict[str, Decimal] = field(default_factory=dict)  # token_addr -> amount
    timestamp: float = field(default_factory=time.time)


@dataclass
class TransactionRequest:
    chain: ChainId
    from_address: str
    to_address: str
    value_wei: int = 0
    data: str = "0x"
    gas: Optional[GasEstimate] = None
    nonce: Optional[int] = None
    chain_id_numeric: Optional[int] = None  # e.g. 1 for ETH mainnet


@dataclass
class TransactionResult:
    chain: ChainId
    tx_hash: str
    status: TxStatus
    block_number: Optional[int] = None
    gas_used: Optional[int] = None
    effective_gas_price_wei: Optional[int] = None
    error_message: Optional[str] = None
    submitted_at: float = field(default_factory=time.time)
    confirmed_at: Optional[float] = None


@dataclass
class SwapQuote:
    chain: ChainId
    dex: str
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out_expected: Decimal
    amount_out_min: Decimal          # after slippage tolerance applied
    price_impact_pct: Decimal
    route: list = field(default_factory=list)
    gas_estimate: Optional[GasEstimate] = None
    quote_timestamp: float = field(default_factory=time.time)
    valid_for_seconds: float = 15.0

    def is_stale(self) -> bool:
        return (time.time() - self.quote_timestamp) > self.valid_for_seconds


@dataclass
class ChainHealthStatus:
    chain: ChainId
    is_healthy: bool
    latency_ms: float
    latest_block: Optional[int] = None
    checked_at: float = field(default_factory=time.time)
    error_message: Optional[str] = None



# --- OHLCV candle -----------------------------------------------------------

@dataclass
class Candle:
    """A single OHLCV bar from a price feed. `timestamp` is unix seconds
    (UTC), consistent with how both GeckoTerminal and Dexscreener report it."""
    timestamp: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_usd: Decimal = Decimal("0")
    tx_count: int = 0
