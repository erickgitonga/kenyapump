"""
core/chain_interface.py

The unified abstraction layer. Every chain (Ethereum today; Base, Solana,
Arbitrum, BSC in later modules) implements this same interface. Nothing
upstream of this layer — risk engine, strategy, execution scheduler — should
ever import a chain-specific SDK directly. That's what makes "automatic
chain switching based on opportunity" tractable: switching chains means
swapping which adapter instance handles the call, not rewriting logic.

Design notes:
- All methods are async: RPC calls dominate latency and everything in a
  trading bot happens concurrently (watching multiple chains, multiple
  pools, multiple pending txs).
- Adapters are expected to be long-lived (one instance per chain, reused
  across the process), not constructed per-call.
- Adapters must be safe to use concurrently (internal locking/pooling is the
  adapter's responsibility, not the caller's).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional, List

from core.models import (
    ChainId,
    GasEstimate,
    TokenInfo,
    LiquidityInfo,
    WalletBalance,
    TransactionRequest,
    TransactionResult,
    SwapQuote,
    ChainHealthStatus,
)


class BaseChainAdapter(ABC):
    """Contract that every chain-specific adapter must fulfill."""

    chain_id: ChainId

    # --- lifecycle -----------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Establish RPC connection(s). Should support failover across
        multiple configured endpoints. Must raise ChainConnectionError on
        total failure, never hang indefinitely."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Clean up sockets/sessions. Called on shutdown."""

    @abstractmethod
    async def health_check(self) -> ChainHealthStatus:
        """Cheap liveness probe (e.g. latest block + RPC latency). The chain
        router polls this to decide whether a chain is currently usable and
        to compare latency/responsiveness across chains when deciding where
        to route an opportunity."""

    # --- read operations -------------------------------------------------

    @abstractmethod
    async def get_native_balance(self, address: str) -> Decimal:
        """Native currency balance (ETH, SOL, BNB, ...) in human units."""

    @abstractmethod
    async def get_wallet_balance(self, address: str, token_addresses: List[str]) -> WalletBalance:
        """Native + specified token balances in one call where the chain
        supports batching (e.g. multicall on EVM)."""

    @abstractmethod
    async def get_token_info(self, token_address: str) -> TokenInfo:
        """Symbol, decimals, verification status, creation time, etc."""

    @abstractmethod
    async def get_liquidity_info(self, token_address: str) -> List[LiquidityInfo]:
        """All known liquidity pools for a token across supported DEXes on
        this chain, sorted by liquidity_usd descending."""

    @abstractmethod
    async def get_current_gas_estimate(self, urgency: str = "medium") -> GasEstimate:
        """urgency in {"low", "medium", "high"} maps to fee percentile the
        adapter should target. This is what the router compares across
        chains to pick the cheapest viable execution venue."""

    # --- simulation / safety --------------------------------------------

    @abstractmethod
    async def simulate_sell(self, token_address: str, amount: Decimal, wallet_address: str) -> bool:
        """Dry-run whether a sell would succeed (honeypot check). Must
        return False rather than raise when simulation itself fails, so
        callers can treat 'unknown' the same as 'unsafe' by default."""

    @abstractmethod
    async def get_swap_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int,
    ) -> SwapQuote:
        """Fetch a fresh quote. Callers must check quote.is_stale() before
        executing — never execute against a quote you didn't just fetch."""

    # --- write operations -------------------------------------------------

    @abstractmethod
    async def build_swap_transaction(self, quote: SwapQuote, wallet_address: str) -> TransactionRequest:
        """Build (but do not sign/send) the transaction for a given quote."""

    @abstractmethod
    async def sign_and_send_transaction(self, tx: TransactionRequest, private_key: str) -> TransactionResult:
        """Sign and broadcast. Returns immediately with PENDING status;
        caller uses wait_for_confirmation for the final result.

        Key handling: adapters must never log, cache, or persist
        private_key. Callers should source keys from a secrets manager /
        encrypted keystore, never plaintext config."""

    @abstractmethod
    async def wait_for_confirmation(self, tx_hash: str, timeout_seconds: float = 60.0) -> TransactionResult:
        """Poll until the tx is confirmed, reverted, or the timeout elapses.
        Must distinguish 'dropped from mempool' from 'still pending' where
        the chain allows it."""

    @abstractmethod
    async def estimate_gas_for_swap(
        self, token_in: str, token_out: str, amount_in: Decimal, wallet_address: str
    ) -> GasEstimate:
        """Chain-specific gas simulation for a prospective swap, used by the
        router/risk engine to compute cost-adjusted expected value *before*
        committing to a chain or a trade."""

    # --- chain characteristics (used by the router) ------------------------

    @abstractmethod
    def get_average_block_time_seconds(self) -> float:
        """Static/rolling-average characteristic used for opportunity
        scoring (e.g. fast chains favored for time-sensitive snipes)."""

    @abstractmethod
    def get_native_symbol(self) -> str:
        """'ETH', 'SOL', 'BNB', etc."""
