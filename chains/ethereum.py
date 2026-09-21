"""
chains/ethereum.py

Ethereum implementation of BaseChainAdapter. Uses web3.py (async).

This module intentionally keeps DEX-routing logic minimal and pluggable:
`_router_abi` / `_get_amounts_out` target a Uniswap-V2-style router
(`swapExactTokensForTokens` / `getAmountsOut`), which also covers most V2
forks memecoins launch on. Add V3 quoting as a separate strategy class
later rather than branching inside this adapter — keep one adapter per
chain, one strategy class per DEX protocol.

Install: pip install web3>=6.15
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import List, Optional

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.exceptions import TransactionNotFound, TimeExhausted
from eth_account import Account

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
from core.exceptions import (
    ChainConnectionError,
    RPCRateLimitError,
    InsufficientFundsError,
    GasEstimationError,
    TransactionSubmissionError,
    TransactionRevertedError,
    QuoteExpiredError,
)
from config.settings import EthereumConfig
from utils.rpc_pool import RpcPool

# --- Minimal ABIs (only the functions this adapter actually calls) ---------

ERC20_ABI = [
    {"constant": True, "inputs": [{"name": "account", "type": "address"}],
     "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "decimals",
     "outputs": [{"name": "", "type": "uint8"}], "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol",
     "outputs": [{"name": "", "type": "string"}], "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "name",
     "outputs": [{"name": "", "type": "string"}], "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
     "name": "allowance", "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"constant": False, "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
     "name": "approve", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "nonpayable", "type": "function"},
]

UNISWAP_V2_ROUTER_ABI = [
    {"inputs": [{"name": "amountIn", "type": "uint256"}, {"name": "path", "type": "address[]"}],
     "name": "getAmountsOut", "outputs": [{"name": "amounts", "type": "uint256[]"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [
        {"name": "amountIn", "type": "uint256"},
        {"name": "amountOutMin", "type": "uint256"},
        {"name": "path", "type": "address[]"},
        {"name": "to", "type": "address"},
        {"name": "deadline", "type": "uint256"},
     ],
     "name": "swapExactTokensForTokens",
     "outputs": [{"name": "amounts", "type": "uint256[]"}],
     "stateMutability": "nonpayable", "type": "function"},
]


class EthereumAdapter(BaseChainAdapter):
    chain_id = ChainId.ETHEREUM

    def __init__(self, config: EthereumConfig):
        self.config = config
        if not config.rpc_endpoints:
            raise ValueError(
                "EthereumConfig has no rpc_endpoints configured. "
                "Set ETHEREUM_RPC_URLS (comma-separated) in your environment."
            )
        self._rpc_pool = RpcPool(config.rpc_endpoints)
        self._w3_by_url: dict[str, AsyncWeb3] = {}
        self._connected = False

    # --- lifecycle -------------------------------------------------------

    async def connect(self) -> None:
        # Eagerly validate at least one endpoint responds before declaring
        # ourselves connected — fail fast at startup rather than on the
        # first trade.
        health = await self.health_check()
        if not health.is_healthy:
            raise ChainConnectionError(
                f"Ethereum adapter failed initial connection check: {health.error_message}"
            )
        self._connected = True

    async def disconnect(self) -> None:
        for w3 in self._w3_by_url.values():
            provider = w3.provider
            if hasattr(provider, "disconnect"):
                await provider.disconnect()
        self._w3_by_url.clear()
        self._connected = False

    def _get_w3(self, url: str) -> AsyncWeb3:
        if url not in self._w3_by_url:
            self._w3_by_url[url] = AsyncWeb3(AsyncHTTPProvider(url))
        return self._w3_by_url[url]

    async def health_check(self) -> ChainHealthStatus:
        start = time.monotonic()
        try:
            async def _probe(url: str) -> int:
                w3 = self._get_w3(url)
                try:
                    return await w3.eth.block_number
                except Exception as exc:  # noqa: BLE001
                    msg = str(exc).lower()
                    if "rate" in msg or "429" in msg:
                        raise RPCRateLimitError(str(exc)) from exc
                    raise

            block_number = await self._rpc_pool.call(_probe)
            latency_ms = (time.monotonic() - start) * 1000
            return ChainHealthStatus(
                chain=self.chain_id,
                is_healthy=True,
                latency_ms=latency_ms,
                latest_block=block_number,
            )
        except ChainConnectionError as exc:
            return ChainHealthStatus(
                chain=self.chain_id,
                is_healthy=False,
                latency_ms=(time.monotonic() - start) * 1000,
                error_message=str(exc),
            )

    # --- read operations ---------------------------------------------------

    async def get_native_balance(self, address: str) -> Decimal:
        async def _fetch(url: str) -> Decimal:
            w3 = self._get_w3(url)
            checksum = w3.to_checksum_address(address)
            balance_wei = await w3.eth.get_balance(checksum)
            return Decimal(balance_wei) / Decimal(10 ** 18)

        return await self._rpc_pool.call(_fetch)

    async def get_wallet_balance(self, address: str, token_addresses: List[str]) -> WalletBalance:
        native = await self.get_native_balance(address)
        token_balances: dict[str, Decimal] = {}

        async def _fetch_token(url: str, token_address: str) -> Decimal:
            w3 = self._get_w3(url)
            checksum_owner = w3.to_checksum_address(address)
            checksum_token = w3.to_checksum_address(token_address)
            contract = w3.eth.contract(address=checksum_token, abi=ERC20_ABI)
            raw = await contract.functions.balanceOf(checksum_owner).call()
            decimals = await contract.functions.decimals().call()
            return Decimal(raw) / Decimal(10 ** decimals)

        # Fetch sequentially through the pool to keep RPC call volume
        # predictable; batch via multicall here in a later optimization pass
        # if the token list grows large.
        for token_address in token_addresses:
            token_balances[token_address] = await self._rpc_pool.call(
                lambda url, ta=token_address: _fetch_token(url, ta)
            )

        return WalletBalance(
            chain=self.chain_id,
            address=address,
            native_balance=native,
            token_balances=token_balances,
        )

    async def get_token_info(self, token_address: str) -> TokenInfo:
        async def _fetch(url: str) -> TokenInfo:
            w3 = self._get_w3(url)
            checksum = w3.to_checksum_address(token_address)
            contract = w3.eth.contract(address=checksum, abi=ERC20_ABI)
            symbol = await contract.functions.symbol().call()
            decimals = await contract.functions.decimals().call()
            try:
                name = await contract.functions.name().call()
            except Exception:  # noqa: BLE001 - name() is optional per ERC20
                name = None
            return TokenInfo(
                chain=self.chain_id,
                address=checksum,
                symbol=symbol,
                decimals=decimals,
                name=name,
                # is_verified_contract / creation_timestamp require an
                # explorer API (Etherscan) — wire that up as an injected
                # dependency rather than hardcoding an API key here.
            )

        return await self._rpc_pool.call(_fetch)

    async def get_liquidity_info(self, token_address: str) -> List[LiquidityInfo]:
        # Pool discovery (Uniswap V2/V3 factories, PancakeSwap forks, etc.)
        # and USD pricing genuinely need an indexer — scanning factory
        # events over an RPC connection live is far too slow for trading
        # decisions. Wire this to a service like The Graph, Dexscreener, or
        # your own indexer. Left as a clear extension point rather than a
        # fake implementation that would silently return wrong numbers.
        raise NotImplementedError(
            "get_liquidity_info requires a DEX indexer/aggregator integration "
            "(e.g. Dexscreener API or The Graph). Inject a LiquidityProvider "
            "dependency into EthereumAdapter rather than scanning chain state directly."
        )

    async def get_current_gas_estimate(self, urgency: str = "medium") -> GasEstimate:
        percentile_by_urgency = {"low": 25, "medium": 50, "high": 90}
        percentile = percentile_by_urgency.get(urgency, 50)

        async def _fetch(url: str) -> GasEstimate:
            w3 = self._get_w3(url)
            latest = await w3.eth.get_block("latest")
            base_fee = latest.get("baseFeePerGas")

            try:
                fee_history = await w3.eth.fee_history(10, "latest", [percentile])
                priority_fees = [r[0] for r in fee_history["reward"] if r]
                priority_fee = (
                    int(sum(priority_fees) / len(priority_fees)) if priority_fees else int(1e9)
                )
            except Exception:  # noqa: BLE001 - fall back to a safe default
                priority_fee = int(1.5e9)  # 1.5 gwei fallback

            if base_fee is not None:
                max_fee = base_fee * 2 + priority_fee
                cost_wei = max_fee * 21000  # placeholder gas_limit; refined per-tx
                return GasEstimate(
                    chain=self.chain_id,
                    gas_limit=21000,
                    max_fee_per_gas_wei=max_fee,
                    max_priority_fee_per_gas_wei=priority_fee,
                    estimated_cost_native=Decimal(cost_wei) / Decimal(10 ** 18),
                    confidence="high",
                )
            else:
                # Legacy chain / pre-1559 fallback
                gas_price = await w3.eth.gas_price
                cost_wei = gas_price * 21000
                return GasEstimate(
                    chain=self.chain_id,
                    gas_limit=21000,
                    legacy_gas_price_wei=gas_price,
                    estimated_cost_native=Decimal(cost_wei) / Decimal(10 ** 18),
                    confidence="medium",
                )

        try:
            return await self._rpc_pool.call(_fetch)
        except ChainConnectionError as exc:
            raise GasEstimationError(str(exc)) from exc

    # --- simulation / safety ------------------------------------------------

    async def simulate_sell(self, token_address: str, amount: Decimal, wallet_address: str) -> bool:
        """
        Honeypot check via eth_call simulation: does a sell path through the
        configured router actually return a nonzero output without
        reverting? This catches the common cases (transfer disabled for
        non-owners, sell tax of 100%, blacklist logic) but is NOT a
        substitute for a full bytecode/static analysis honeypot scanner —
        wire in a dedicated service (e.g. Honeypot.is API, GoPlus Security
        API) for the risk-management module and treat this as a fast
        first-pass filter, not the final word.
        """
        if not self.config.default_router_address:
            return False  # conservative: unknown means unsafe, per interface contract

        async def _simulate(url: str) -> bool:
            w3 = self._get_w3(url)
            router = w3.eth.contract(
                address=w3.to_checksum_address(self.config.default_router_address),
                abi=UNISWAP_V2_ROUTER_ABI,
            )
            token = w3.to_checksum_address(token_address)
            weth = w3.to_checksum_address(self.config.wrapped_native_address)
            decimals_contract = w3.eth.contract(
                address=token, abi=ERC20_ABI
            )
            try:
                decimals = await decimals_contract.functions.decimals().call()
                amount_wei = int(amount * Decimal(10 ** decimals))
                amounts = await router.functions.getAmountsOut(
                    amount_wei, [token, weth]
                ).call()
                return len(amounts) == 2 and amounts[1] > 0
            except Exception:
                return False

        try:
            return await self._rpc_pool.call(_simulate)
        except ChainConnectionError:
            return False  # can't verify safety -> treat as unsafe

    async def get_swap_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int,
    ) -> SwapQuote:
        if not self.config.default_router_address:
            raise GasEstimationError("No default_router_address configured for EthereumAdapter")

        async def _quote(url: str) -> SwapQuote:
            w3 = self._get_w3(url)
            router = w3.eth.contract(
                address=w3.to_checksum_address(self.config.default_router_address),
                abi=UNISWAP_V2_ROUTER_ABI,
            )
            token_in_cs = w3.to_checksum_address(token_in)
            token_out_cs = w3.to_checksum_address(token_out)

            in_contract = w3.eth.contract(address=token_in_cs, abi=ERC20_ABI)
            out_contract = w3.eth.contract(address=token_out_cs, abi=ERC20_ABI)
            in_decimals = await in_contract.functions.decimals().call()
            out_decimals = await out_contract.functions.decimals().call()

            amount_in_wei = int(amount_in * Decimal(10 ** in_decimals))
            path = [token_in_cs, token_out_cs]
            amounts = await router.functions.getAmountsOut(amount_in_wei, path).call()
            amount_out_wei = amounts[-1]
            amount_out = Decimal(amount_out_wei) / Decimal(10 ** out_decimals)

            slippage_factor = Decimal(10_000 - slippage_bps) / Decimal(10_000)
            amount_out_min = amount_out * slippage_factor

            return SwapQuote(
                chain=self.chain_id,
                dex="uniswap_v2_compatible",
                token_in=token_in_cs,
                token_out=token_out_cs,
                amount_in=amount_in,
                amount_out_expected=amount_out,
                amount_out_min=amount_out_min,
                price_impact_pct=Decimal("0"),  # requires pool reserves; compute in risk module with LiquidityInfo
                route=path,
            )

        return await self._rpc_pool.call(_quote)

    # --- write operations ---------------------------------------------------

    async def build_swap_transaction(self, quote: SwapQuote, wallet_address: str) -> TransactionRequest:
        if quote.is_stale():
            raise QuoteExpiredError(
                f"Quote for {quote.token_in}->{quote.token_out} is stale "
                f"(age {time.time() - quote.quote_timestamp:.1f}s, "
                f"valid for {quote.valid_for_seconds}s)"
            )

        async def _build(url: str) -> TransactionRequest:
            w3 = self._get_w3(url)
            router_address = w3.to_checksum_address(self.config.default_router_address)
            router = w3.eth.contract(address=router_address, abi=UNISWAP_V2_ROUTER_ABI)
            wallet_cs = w3.to_checksum_address(wallet_address)

            in_contract = w3.eth.contract(address=quote.token_in, abi=ERC20_ABI)
            in_decimals = await in_contract.functions.decimals().call()
            out_contract = w3.eth.contract(address=quote.token_out, abi=ERC20_ABI)
            out_decimals = await out_contract.functions.decimals().call()

            amount_in_wei = int(quote.amount_in * Decimal(10 ** in_decimals))
            amount_out_min_wei = int(quote.amount_out_min * Decimal(10 ** out_decimals))
            deadline = int(time.time()) + 120  # 2 minute execution window

            call_data = router.encode_abi(
                "swapExactTokensForTokens",
                args=[amount_in_wei, amount_out_min_wei, quote.route, wallet_cs, deadline],
            )

            nonce = await w3.eth.get_transaction_count(wallet_cs, "pending")

            return TransactionRequest(
                chain=self.chain_id,
                from_address=wallet_cs,
                to_address=router_address,
                value_wei=0,
                data=call_data,
                nonce=nonce,
                chain_id_numeric=self.config.chain_id_numeric,
            )

        return await self._rpc_pool.call(_build)

    async def sign_and_send_transaction(self, tx: TransactionRequest, private_key: str) -> TransactionResult:
        """
        SECURITY: private_key is used in-memory only for signing and is
        never logged, cached, or persisted by this method. Caller is
        responsible for sourcing it from a secure secrets store and
        clearing it from memory as soon as practical.
        """
        gas_est = tx.gas or await self.get_current_gas_estimate("high")

        async def _send(url: str) -> str:
            w3 = self._get_w3(url)
            account = Account.from_key(private_key)
            if account.address.lower() != tx.from_address.lower():
                raise TransactionSubmissionError(
                    "private_key does not match tx.from_address"
                )

            native_balance_wei = await w3.eth.get_balance(tx.from_address)
            gas_cost_wei = (gas_est.max_fee_per_gas_wei or gas_est.legacy_gas_price_wei or 0) * gas_est.gas_limit
            if native_balance_wei < tx.value_wei + gas_cost_wei:
                raise InsufficientFundsError(
                    f"Balance {native_balance_wei} wei insufficient for value "
                    f"{tx.value_wei} + estimated gas cost {gas_cost_wei} wei"
                )

            tx_dict = {
                "from": tx.from_address,
                "to": tx.to_address,
                "value": tx.value_wei,
                "data": tx.data,
                "nonce": tx.nonce,
                "chainId": tx.chain_id_numeric or self.config.chain_id_numeric,
                "gas": gas_est.gas_limit,
            }
            if gas_est.max_fee_per_gas_wei is not None:
                tx_dict["maxFeePerGas"] = gas_est.max_fee_per_gas_wei
                tx_dict["maxPriorityFeePerGas"] = gas_est.max_priority_fee_per_gas_wei
                tx_dict["type"] = 2
            else:
                tx_dict["gasPrice"] = gas_est.legacy_gas_price_wei

            signed = account.sign_transaction(tx_dict)
            try:
                tx_hash = await w3.eth.send_raw_transaction(signed.raw_transaction)
            except Exception as exc:  # noqa: BLE001
                msg = str(exc).lower()
                if "insufficient funds" in msg:
                    raise InsufficientFundsError(str(exc)) from exc
                if "underpriced" in msg or "nonce" in msg:
                    raise TransactionSubmissionError(str(exc)) from exc
                raise
            return tx_hash.hex()

        tx_hash_hex = await self._rpc_pool.call(_send)
        return TransactionResult(
            chain=self.chain_id,
            tx_hash=tx_hash_hex,
            status=TxStatus.PENDING,
        )

    async def wait_for_confirmation(self, tx_hash: str, timeout_seconds: float = 60.0) -> TransactionResult:
        async def _wait(url: str) -> TransactionResult:
            w3 = self._get_w3(url)
            try:
                receipt = await w3.eth.wait_for_transaction_receipt(
                    tx_hash, timeout=timeout_seconds
                )
            except TimeExhausted:
                return TransactionResult(
                    chain=self.chain_id,
                    tx_hash=tx_hash,
                    status=TxStatus.PENDING,
                    error_message="Confirmation timeout elapsed; tx may still land later.",
                )
            except TransactionNotFound:
                return TransactionResult(
                    chain=self.chain_id,
                    tx_hash=tx_hash,
                    status=TxStatus.DROPPED,
                    error_message="Transaction not found; likely dropped from mempool.",
                )

            status = TxStatus.CONFIRMED if receipt["status"] == 1 else TxStatus.FAILED
            result = TransactionResult(
                chain=self.chain_id,
                tx_hash=tx_hash,
                status=status,
                block_number=receipt["blockNumber"],
                gas_used=receipt["gasUsed"],
                effective_gas_price_wei=receipt.get("effectiveGasPrice"),
                confirmed_at=time.time(),
            )
            if status == TxStatus.FAILED:
                raise TransactionRevertedError(f"Transaction {tx_hash} reverted on-chain")
            return result

        return await self._rpc_pool.call(_wait)

    async def estimate_gas_for_swap(
        self, token_in: str, token_out: str, amount_in: Decimal, wallet_address: str
    ) -> GasEstimate:
        quote = await self.get_swap_quote(token_in, token_out, amount_in, slippage_bps=300)
        tx = await self.build_swap_transaction(quote, wallet_address)

        async def _estimate(url: str) -> GasEstimate:
            w3 = self._get_w3(url)
            try:
                gas_limit = await w3.eth.estimate_gas(
                    {
                        "from": tx.from_address,
                        "to": tx.to_address,
                        "value": tx.value_wei,
                        "data": tx.data,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                raise GasEstimationError(f"estimate_gas failed: {exc}") from exc

            base_estimate = await self.get_current_gas_estimate("high")
            base_estimate.gas_limit = int(gas_limit * 1.2)  # 20% buffer for state drift
            return base_estimate

        return await self._rpc_pool.call(_estimate)

    # --- chain characteristics ---------------------------------------------
        # --- EVM-specific extensions (not part of BaseChainAdapter) -----------

    async def get_latest_block_number(self) -> int:
        async def _fetch(url: str) -> int:
            w3 = self._get_w3(url)
            return await w3.eth.block_number

        return await self._rpc_pool.call(_fetch)

    async def get_block_timestamp(self, block_number: int) -> int:
        async def _fetch(url: str) -> int:
            w3 = self._get_w3(url)
            block = await w3.eth.get_block(block_number)
            return block["timestamp"]

        return await self._rpc_pool.call(_fetch)

    async def get_logs(
        self,
        address: str,
        topics: List[Optional[str]],
        from_block: int,
        to_block: int,
    ) -> List[dict]:
        async def _fetch(url: str) -> List[dict]:
            w3 = self._get_w3(url)
            filter_params = {
                "address": w3.to_checksum_address(address),
                "topics": topics,
                "fromBlock": from_block,
                "toBlock": to_block,
            }
            logs = await w3.eth.get_logs(filter_params)
            return list(logs)

        return await self._rpc_pool.call(_fetch)

    def get_average_block_time_seconds(self) -> float:
        return self.config.average_block_time_seconds

    def get_native_symbol(self) -> str:
        return self.config.native_symbol
