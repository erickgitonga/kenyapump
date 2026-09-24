"""
chains/solana.py

Solana implementation of BaseChainAdapter. Uses Helius free tier
RPC for basic operations and WebSocket URL for potential real-time
subscriptions (though not used in this stub implementation).

Primary DEXes: Raydium CPMM, PumpSwap, Meteora DLMM, Orca Whirlpool,
Raydium AMM v4.
"""

from __future__ import annotations

import logging
import os
import time
from decimal import Decimal
from typing import List, Optional

from solana.rpc.async_api import AsyncClient

from core.chain_interface import BaseChainAdapter
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
    TxStatus,
)
from core.exceptions import ChainConnectionError

log = logging.getLogger("kenyapump.chains.solana")


class SolanaAdapter(BaseChainAdapter):
    """
    Solana chain adapter. Uses Helius RPC for basic operations.
    WebSocket URL is stored for potential use by scraper or other components.
    """

    chain_id = ChainId.SOLANA

    def __init__(self):
        self.ws_url = os.getenv("SOLANA_WS_URL")
        if not self.ws_url:
            raise ChainConnectionError("SOLANA_WS_URL not set")
        # Derive RPC URL from WS URL (assuming standard Helius format)
        if "api-key=" not in self.ws_url:
            raise ChainConnectionError("Invalid Helius WS URL: missing api-key")
        self.api_key = self.ws_url.split("api-key=")[1].split('&')[0]
        self.rpc_url = f"https://mainnet.helius-rpc.com/?api-key={self.api_key}"
        self._rpc_client: Optional[AsyncClient] = None
        log.info("SolanaAdapter initialized")

    async def connect(self) -> None:
        """Establish RPC connection to Helius."""
        try:
            self._rpc_client = AsyncClient(self.rpc_url)
            # Verify connection by fetching latest slot
            await self._rpc_client.get_slot()
            log.info("SolanaAdapter connected (rpc_url=%s)", self.rpc_url)
        except Exception as exc:
            log.error("Failed to connect to Solana RPC: %s", exc)
            raise ChainConnectionError(f"Solana RPC connection failed: {exc}") from exc

    async def disconnect(self) -> None:
        """Close RPC connection."""
        if self._rpc_client:
            await self._rpc_client.close()
            self._rpc_client = None
        log.info("SolanaAdapter disconnected")

    async def health_check(self) -> ChainHealthStatus:
        """Check Solana network health via RPC."""
        start = time.monotonic()
        try:
            if not self._rpc_client:
                await self.connect()
            slot = await self._rpc_client.get_slot()
            latency_ms = (time.monotonic() - start) * 1000
            return ChainHealthStatus(
                chain=self.chain_id,
                is_healthy=True,
                latency_ms=latency_ms,
                latest_block=slot,
            )
        except Exception as exc:
            return ChainHealthStatus(
                chain=self.chain_id,
                is_healthy=False,
                latency_ms=(time.monotonic() - start) * 1000,
                error_message=str(exc),
            )

    async def get_token_info(self, token_address: str) -> TokenInfo:
        """Get token mint info (symbol, decimals, etc.)."""
        if not self._rpc_client:
            await self.connect()
        try:
            mint_info = await self._rpc_client.get_mint_info(token_address)
            if not mint_info:
                raise ValueError("Invalid token address")
            # Note: Symbol and name require metadata lookup (not implemented in this stub)
            return TokenInfo(
                chain=self.chain_id,
                address=token_address,
                symbol=token_address[:10],  # placeholder; replace with metadata lookup
                decimals=mint_info.decimals,
                name=None,  # placeholder
                is_verified_contract=False,  # Solana does not have contract verification
                creation_timestamp=None,  # placeholder
            )
        except Exception as exc:
            log.error("Failed to get token info for %s: %s", token_address, exc)
            raise ChainConnectionError(f"Failed to get token info: {exc}") from exc

    async def get_latest_block_number(self) -> int:
        """Get the latest slot number."""
        if not self._rpc_client:
            await self.connect()
        return await self._rpc_client.get_slot()

    async def get_logs(
        self,
        address: str,
        topics: List[Optional[str]],
        from_block: int,
        to_block: int,
    ) -> List[dict]:
        """
        Stub for get_logs. Returns empty list.
        In a real implementation, this would fetch logs via RPC (e.g., getSignaturesForAddress + getTransaction).
        """
        log.warning("get_logs is stubbed for SolanaAdapter")
        return []

    async def get_native_balance(self, address: str) -> Decimal:
        """Get SOL balance for an address."""
        if not self._rpc_client:
            await self.connect()
        try:
            balance = await self._rpc_client.get_balance(address)
            return Decimal(balance) / Decimal(10 ** 9)  # SOL has 9 decimals
        except Exception as exc:
            log.error("Failed to get native balance for %s: %s", address, exc)
            raise ChainConnectionError(f"Failed to get native balance: {exc}") from exc

    # --- Stubbed methods from BaseChainAdapter (not implemented in this stub) ---

    async def get_wallet_balance(self, address: str, token_addresses: List[str]) -> WalletBalance:
        raise NotImplementedError("get_wallet_balance not implemented for SolanaAdapter")

    async def get_liquidity_info(self, token_address: str) -> List[LiquidityInfo]:
        raise NotImplementedError("get_liquidity_info not implemented for SolanaAdapter")

    async def get_current_gas_estimate(self, urgency: str = "medium") -> GasEstimate:
        raise NotImplementedError("get_current_gas_estimate not implemented for SolanaAdapter")

    async def simulate_sell(self, token_address: str, amount: Decimal, wallet_address: str) -> bool:
        raise NotImplementedError("simulate_sell not implemented for SolanaAdapter")

    async def get_swap_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int,
    ) -> SwapQuote:
        raise NotImplementedError("get_swap_quote not implemented for SolanaAdapter")

    async def build_swap_transaction(self, quote: SwapQuote, wallet_address: str) -> TransactionRequest:
        raise NotImplementedError("build_swap_transaction not implemented for SolanaAdapter")

    async def sign_and_send_transaction(self, tx: TransactionRequest, private_key: str) -> TransactionResult:
        raise NotImplementedError("sign_and_send_transaction not implemented for SolanaAdapter")

    async def wait_for_confirmation(self, tx_hash: str, timeout_seconds: float = 60.0) -> TransactionResult:
        raise NotImplementedError("wait_for_confirmation not implemented for SolanaAdapter")

    async def estimate_gas_for_swap(
        self, token_in: str, token_out: str, amount_in: Decimal, wallet_address: str
    ) -> GasEstimate:
        raise NotImplementedError("estimate_gas_for_swap not implemented for SolanaAdapter")

    def get_average_block_time_seconds(self) -> float:
        """Solana average block time is approximately 0.4 seconds."""
        return 0.4

    def get_native_symbol(self) -> str:
        """Native token for Solana is SOL."""
        return "SOL"
