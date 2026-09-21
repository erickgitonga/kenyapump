"""
core/exceptions.py

A dedicated exception hierarchy matters a lot in a trading system: the
execution engine needs to know whether a failure is retryable (RPC timeout),
fatal (insufficient funds), or a risk-policy rejection (should never be
retried, ever). Catching bare `Exception` around trade execution is how bots
end up double-submitting transactions or buying into honeypots.
"""


class BotError(Exception):
    """Base class for all bot-raised errors."""


# --- Connectivity / infrastructure -----------------------------------------

class ChainConnectionError(BotError):
    """RPC unreachable, timed out, or returned malformed data. Retryable."""


class RPCRateLimitError(ChainConnectionError):
    """Provider rate-limited us. Retryable with backoff."""


class ChainNotSupportedError(BotError):
    """Requested a ChainId with no registered adapter."""


# --- Transaction / execution -------------------------------------------------

class InsufficientFundsError(BotError):
    """Not enough native token to cover value + gas."""


class GasEstimationError(BotError):
    """Could not produce a reliable gas estimate."""


class TransactionSubmissionError(BotError):
    """Node rejected the transaction (nonce, underpriced, etc). May be retryable."""


class TransactionRevertedError(BotError):
    """Transaction mined but reverted on-chain. Not retryable as-is."""


class SlippageExceededError(BotError):
    """Actual execution price moved beyond configured tolerance."""


class QuoteExpiredError(BotError):
    """SwapQuote used after its validity window elapsed."""


# --- Risk / safety (never silently swallow these) ---------------------------

class RiskPolicyViolation(BotError):
    """A trade was blocked by risk management. This must never be bypassed
    programmatically — if you find yourself catching this and retrying the
    same trade, that's a bug, not a resilience feature."""


class HoneypotDetectedError(RiskPolicyViolation):
    """Token simulated as unsellable or has malicious transfer logic."""


class LiquidityTooLowError(RiskPolicyViolation):
    """Pool liquidity below configured minimum threshold."""


class ContractUnverifiedError(RiskPolicyViolation):
    """Token contract has no verified source and policy requires one."""


class PositionLimitExceededError(RiskPolicyViolation):
    """Would breach max position size / exposure limits."""



# --- Data layer / external indexers -----------------------------------------

class IndexerError(BotError):
    """Base error for external data indexer failures (Dexscreener,
    GeckoTerminal). Most indexer failures are retryable — but a 404
    (token doesn't exist) is not."""


class IndexerRateLimitError(IndexerError):
    """Indexer returned HTTP 429. Retryable with backoff. Note: our clients
    already rate-limit internally, so hitting this means the provider's
    ceiling is lower than documented or we have a bug in the limiter."""


class IndexerNotFoundError(IndexerError):
    """Indexer returned 404 or empty result set. NOT retryable — the
    token/pool genuinely doesn't exist on that chain."""


class IndexerResponseError(IndexerError):
    """Indexer returned 200 but the payload was malformed or missing
    expected fields. Retryable once, then treat as fatal."""


class IndexerAuthError(IndexerError):
    """Missing/invalid API key for a keyed indexer. Not retryable."""
