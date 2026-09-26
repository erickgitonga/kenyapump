"""
data/solana_outcomes.py

Background outcome tracker for Solana tokens.
Snapshots every sampled token at T+90s, T+15m, and T+24h.
Captures liquidity, price, holder count, top-holder %, and status classification.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import aiosqlite

from core.models import ChainId, TokenInfo
from data.dexscreener import DexscreenerClient
from data.solana_holders import get_solana_top_holders, SolanaHolder
from ai.honeypot import HoneypotDetector

log = logging.getLogger("kenyapump.data.solana_outcomes")

def _sqlite_native(v):
    """Convert Decimal / enum / other non-SQLite types to primitives."""
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return v
    if isinstance(v, str):
        return v
    if isinstance(v, bytes):
        return v
    # Decimal, enum, or anything else with a value attribute
    if hasattr(v, "value"):
        inner = v.value
        return _sqlite_native(inner)
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
    except ImportError:
        pass
    return float(v)



# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────

SAMPLE_RATE = 0.05  # 5% of detected tokens
SNAPSHOT_INTERVALS = [90, 15 * 60, 24 * 60 * 60]  # seconds: 90s, 15m, 24h
DB_PATH = Path("data/outcomes.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Status classifications
class TokenStatus:
    ALIVE = "alive"
    DISTRIBUTING = "distributing"
    DUMPED = "dumped"
    DEAD = "dead"
    RUGGED = "rugged"
    UNKNOWN = "unknown"


# ──────────────────────────────────────────────────────────────────────
# Data classes
# ──────────────────────────────────────────────────────────────────────

@dataclass
class Snapshot:
    token_address: str
    block_number: int
    timestamp: int
    liquidity_usd: Optional[float]
    price_usd: Optional[float]
    holder_count: int
    top_holder_pct: float
    status: str


@dataclass
class PendingToken:
    token_address: str
    symbol: str
    chain: str
    first_seen_block: int
    first_seen_timestamp: int
    next_snapshot_idx: int  # which interval we're waiting for (0, 1, 2)
    attempts: int
    sampled: bool
    peak_price_usd: Optional[float] = None


# ──────────────────────────────────────────────────────────────────────
# OutcomeTracker
# ──────────────────────────────────────────────────────────────────────

class OutcomeTracker:
    """
    Tracks outcomes for sampled Solana tokens.
    - Persists queue in SQLite (survives restarts)
    - Takes snapshots at T+90s, T+15m, T+24h
    - Classifies status: alive/distributing/dumped/dead/rugged
    """

    def __init__(
        self,
        dexscreener: DexscreenerClient,
        honeypot_detector: HoneypotDetector,
        solana_rpc_url: str,
        db_path: Path = DB_PATH,
        sample_rate: float = SAMPLE_RATE,
        poll_interval: int = 30,  # seconds
    ):
        self._dex = dexscreener
        self._honeypot = honeypot_detector
        self._solana_rpc_url = solana_rpc_url
        self._db_path = db_path
        self._sample_rate = sample_rate
        self._poll_interval = poll_interval
        self._stopped = False
        self._db: Optional[aiosqlite.Connection] = None

    # ─── DB initialization ────────────────────────────────────────────

    async def initialize(self) -> None:
        """Create tables, indices, enable WAL mode."""
        self._db = await aiosqlite.connect(self._db_path)
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA busy_timeout=5000;")
        await self._db.execute("PRAGMA foreign_keys=ON;")

        # tokens table - canonical state per token
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT UNIQUE NOT NULL,
                symbol TEXT,
                chain TEXT NOT NULL,
                first_seen_block INTEGER NOT NULL,
                first_seen_timestamp INTEGER NOT NULL,
                last_snapshot_block INTEGER,
                last_snapshot_timestamp INTEGER,
                status TEXT NOT NULL DEFAULT 'unknown',
                sampled BOOLEAN NOT NULL DEFAULT 0,
                sampled_at_block INTEGER,
                peak_price_usd REAL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )
        """)

        # snapshots table - immutable history
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_id INTEGER NOT NULL,
                block_number INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                liquidity_usd REAL,
                price_usd REAL,
                holder_count INTEGER NOT NULL DEFAULT 0,
                top_holder_pct REAL NOT NULL DEFAULT 0.0,
                status TEXT NOT NULL,
                FOREIGN KEY(token_id) REFERENCES tokens(id) ON DELETE CASCADE,
                UNIQUE(token_id, block_number)
            )
        """)

        # pending table - queue of tokens awaiting snapshots
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS pending (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT UNIQUE NOT NULL,
                symbol TEXT,
                chain TEXT NOT NULL,
                first_seen_block INTEGER NOT NULL,
                first_seen_timestamp INTEGER NOT NULL,
                next_snapshot_idx INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                sampled BOOLEAN NOT NULL DEFAULT 0,
                sampled_at_block INTEGER,
                peak_price_usd REAL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            )
        """)

        # Indices
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_tokens_status ON tokens(status);")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_token_id ON snapshots(token_id);")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON snapshots(timestamp);")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_pending_next_idx ON pending(next_snapshot_idx);")
        await self._db.execute("CREATE INDEX IF NOT EXISTS idx_pending_sampled ON pending(sampled);")

        await self._db.commit()
        log.info("OutcomeTracker DB initialized at %s", self._db_path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    # ─── Public API ───────────────────────────────────────────────────

    async def record_detection(self, token: TokenInfo, launch_block: int) -> None:
        """
        Called when a new Solana token is detected.
        Decides whether to sample (5%) and queues it if so.
        """
        if not self._db:
            raise RuntimeError("OutcomeTracker not initialized")

        token_addr = token.address.lower()
        now_ts = int(time.time())

        # Check if already in tokens or pending
        async with self._db.execute(
            "SELECT 1 FROM tokens WHERE token_address = ?", (token_addr,)
        ) as cur:
            if await cur.fetchone():
                return  # already tracked

        async with self._db.execute(
            "SELECT 1 FROM pending WHERE token_address = ?", (token_addr,)
        ) as cur:
            if await cur.fetchone():
                return  # already queued

        # Decide sampling
        sampled = random.random() < self._sample_rate

        if sampled:
            log.info("[outcome] Sampling token %s (%s)", token.symbol, token_addr[:12])
            await self._db.execute(
                """
                INSERT INTO pending (
                    token_address, symbol, chain, first_seen_block,
                    first_seen_timestamp, next_snapshot_idx, attempts,
                    sampled, sampled_at_block
                ) VALUES (?, ?, ?, ?, ?, 0, 0, 1, ?)
                """,
                (
                    token_addr,
                    token.symbol,
                    token.chain.value if hasattr(token.chain, "value") else str(token.chain),
                    launch_block,
                    now_ts,
                    launch_block,
                ),
            )
            # Also create tokens row with sampled=1
            await self._db.execute(
                """
                INSERT INTO tokens (
                    token_address, symbol, chain, first_seen_block,
                    first_seen_timestamp, status, sampled, sampled_at_block
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    token_addr,
                    token.symbol,
                    token.chain.value if hasattr(token.chain, "value") else str(token.chain),
                    launch_block,
                    now_ts,
                    TokenStatus.UNKNOWN,
                    launch_block,
                ),
            )
        else:
            # Not sampled - still track in tokens but not in pending
            await self._db.execute(
                """
                INSERT INTO tokens (
                    token_address, symbol, chain, first_seen_block,
                    first_seen_timestamp, status, sampled
                ) VALUES (?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    token_addr,
                    token.symbol,
                    token.chain.value if hasattr(token.chain, "value") else str(token.chain),
                    launch_block,
                    now_ts,
                    TokenStatus.UNKNOWN,
                ),
            )

        await self._db.commit()

    async def run_forever(self) -> None:
        """Main background loop: check pending tokens, take due snapshots."""
        log.info("OutcomeTracker starting (poll every %ds)", self._poll_interval)
        try:
            while not self._stopped:
                try:
                    await self._process_due_snapshots()
                except Exception as exc:  # noqa: BLE001
                    log.exception("Error in outcome tracker cycle: %s", exc)
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            log.info("OutcomeTracker stopping (cancelled)")
            raise
        finally:
            await self.close()

    def stop(self) -> None:
        self._stopped = True

    # ─── Internal processing ──────────────────────────────────────────

    async def _process_due_snapshots(self) -> None:
        """Find pending tokens whose next snapshot is due and process them."""
        if not self._db:
            return

        log.info("[outcome] Loop tick")

        now_ts = int(time.time())

        async with self._db.execute(
            """
            SELECT id, token_address, symbol, chain, first_seen_block,
                   first_seen_timestamp, next_snapshot_idx, attempts,
                   peak_price_usd
            FROM pending
            WHERE sampled = 1
            ORDER BY first_seen_timestamp
            """
        ) as cur:
            rows = await cur.fetchall()

        for row in rows:
            (pid, token_addr, symbol, chain, first_seen_block,
             first_seen_ts, next_idx, attempts, peak_price) = row

            if next_idx >= len(SNAPSHOT_INTERVALS):
                await self._db.execute("DELETE FROM pending WHERE id = ?", (pid,))
                await self._db.commit()
                continue

            target_ts = first_seen_ts + SNAPSHOT_INTERVALS[next_idx]
            if now_ts < target_ts:
                continue

            log.info(
                "[outcome] Taking snapshot %d/3 for %s (%s) at T+%ds",
                next_idx + 1, symbol, token_addr[:12], SNAPSHOT_INTERVALS[next_idx]
            )

            try:
                snapshot = await self._take_snapshot(
                    token_addr, symbol, chain, next_idx, peak_price
                )
                if snapshot:
                    await self._store_snapshot(token_addr, snapshot, next_idx)
                    new_idx = next_idx + 1
                    if peak_price is not None:
                        new_peak = max(peak_price, _sqlite_native(snapshot.price_usd) or 0)
                    else:
                        new_peak = _sqlite_native(snapshot.price_usd)
                    await self._db.execute(
                        "UPDATE pending SET next_snapshot_idx = ?, attempts = 0, peak_price_usd = ? WHERE id = ?",
                        (new_idx, new_peak, pid),
                    )
                else:
                    new_attempts = attempts + 1
                    await self._db.execute(
                        "UPDATE pending SET attempts = ? WHERE id = ?",
                        (new_attempts, pid),
                    )
                    log.warning(
                        "[outcome] Snapshot failed for %s, attempt %d",
                        token_addr[:12], new_attempts,
                    )
            except Exception as exc:
                log.exception("Failed to process snapshot for %s: %s", token_addr[:12], exc)
                new_attempts = attempts + 1
                await self._db.execute(
                    "UPDATE pending SET attempts = ? WHERE id = ?",
                    (new_attempts, pid),
                )

            await self._db.commit()

    async def _take_snapshot(
        self,
        token_addr: str,
        symbol: str,
        chain: str,
        interval_idx: int,
        peak_price: Optional[float],
    ) -> Optional[Snapshot]:
        """Fetch all data for a single snapshot."""
        now_ts = int(time.time())
        block_number = now_ts  # use timestamp so each snapshot is unique

        # 1. Price & liquidity from Dexscreener
        liquidity_usd = None
        price_usd = None
        try:
            pools = await self._dex.get_liquidity_info(ChainId.SOLANA, token_addr)
            if pools:
                # Use the pool with highest liquidity
                best_pool = max(pools, key=lambda p: p.liquidity_usd or 0)
                liquidity_usd = best_pool.liquidity_usd
                price_usd = best_pool.price_usd
        except Exception as exc:  # noqa: BLE001
            log.warning("Dexscreener lookup failed for %s: %s", token_addr[:12], exc)

        # 2. Holder data from Solana RPC
        holder_count = 0
        top_holder_pct = 0.0
        try:
            holders = await get_solana_top_holders(
                rpc_url=self._solana_rpc_url,
                mint_address=token_addr,
                top_n=20,
                exclude=None,  # Don't exclude bonding curve - it's the main holder early on
            )
            holder_count = len(holders)
            if holders:
                total_supply = sum(h.balance_ui for h in holders)  # approximate
                if total_supply > 0:
                    top_holder_pct = holders[0].balance_ui / total_supply
        except Exception as exc:  # noqa: BLE001
            log.warning("Holder fetch failed for %s: %s", token_addr[:12], exc)

        # 3. Honeypot check (only on first snapshot to save credits)
        status = TokenStatus.UNKNOWN
        if interval_idx == 0:
            try:
                report = await self._honeypot.assess(ChainId.SOLANA, token_addr)
                if report.risk_level.name.lower() in ("danger", "critical"):
                    status = TokenStatus.RUGGED
            except Exception as exc:  # noqa: BLE001
                log.warning("Honeypot check failed for %s: %s", token_addr[:12], exc)

        # 4. Classification (if not already rugged)
        if status != TokenStatus.RUGGED:
            status = self._classify_status(
                interval_idx=interval_idx,
                liquidity_usd=liquidity_usd,
                price_usd=price_usd,
                holder_count=holder_count,
                top_holder_pct=top_holder_pct,
                peak_price_usd=peak_price,
            )

        return Snapshot(
            token_address=token_addr,
            block_number=block_number,
            timestamp=now_ts,
            liquidity_usd=liquidity_usd,
            price_usd=price_usd,
            holder_count=holder_count,
            top_holder_pct=top_holder_pct,
            status=status,
        )

    def _classify_status(
        self,
        interval_idx: int,
        liquidity_usd: Optional[float],
        price_usd: Optional[float],
        holder_count: int,
        top_holder_pct: float,
        peak_price_usd: Optional[float],
    ) -> str:
        """
        Classify token status based on snapshot data.
        Rules:
        - rugged: already handled before calling this
        - dead: liquidity=0 and price=0 for 2 consecutive snapshots (15m and 24h)
        - dumped: price dropped >30% from peak AND top_holder_pct dropped sharply
        - distributing: top_holder_pct rising >10% over previous snapshot AND price stable/up
        - alive: otherwise
        """
        # Normalize types — DexScreener returns Decimal, we use float elsewhere
        liquidity_usd = float(liquidity_usd) if liquidity_usd is not None else None
        price_usd = float(price_usd) if price_usd is not None else None
        peak_price_usd = float(peak_price_usd) if peak_price_usd is not None else None
        top_holder_pct = float(top_holder_pct) if top_holder_pct is not None else 0.0

        # For first snapshot (90s), default to alive unless rugged
        if interval_idx == 0:
            return TokenStatus.ALIVE

        # Dead: no liquidity and no price at 15m or 24h
        if liquidity_usd is not None and liquidity_usd == 0 and price_usd is not None and price_usd == 0:
            return TokenStatus.DEAD

        # Dumped: price dropped >30% from peak
        if peak_price_usd and price_usd and peak_price_usd > 0:
            drop_pct = (peak_price_usd - price_usd) / peak_price_usd
            if drop_pct > 0.30:
                return TokenStatus.DUMPED

        # Distributing: top holder % rising significantly (would need previous snapshot)
        # For simplicity, if top_holder_pct > 0.5 (50% held by top holder) and price stable
        if top_holder_pct > 0.5 and price_usd and peak_price_usd:
            change = (price_usd - peak_price_usd) / peak_price_usd if peak_price_usd > 0 else 0
            if change >= -0.10:  # price not down more than 10%
                return TokenStatus.DISTRIBUTING

        return TokenStatus.ALIVE

    async def _store_snapshot(self, token_addr: str, snapshot: Snapshot, interval_idx: int) -> None:
        """Insert snapshot row and update tokens table."""
        if not self._db:
            return

        # Get token_id
        async with self._db.execute(
            "SELECT id FROM tokens WHERE token_address = ?", (token_addr,)
        ) as cur:
            row = await cur.fetchone()
            if not row:
                log.error("Token %s not found in tokens table", token_addr[:12])
                return
            token_id = row[0]

        # Insert snapshot
        await self._db.execute(
            """
            INSERT INTO snapshots (
                token_id, block_number, timestamp, liquidity_usd,
                price_usd, holder_count, top_holder_pct, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                token_id,
                snapshot.block_number,
                snapshot.timestamp,
                _sqlite_native(snapshot.liquidity_usd),
                _sqlite_native(snapshot.price_usd),
                snapshot.holder_count,
                _sqlite_native(snapshot.top_holder_pct),
                snapshot.status,
            ),
        )

        # Update tokens table
        await self._db.execute(
            """
            UPDATE tokens
            SET last_snapshot_block = ?,
                last_snapshot_timestamp = ?,
                status = ?,
                peak_price_usd = CASE
                    WHEN peak_price_usd IS NULL OR ? > peak_price_usd THEN ?
                    ELSE peak_price_usd
                END
            WHERE id = ?
            """,
            (
                snapshot.block_number,
                snapshot.timestamp,
                snapshot.status,
                _sqlite_native(snapshot.price_usd) or 0,
                _sqlite_native(snapshot.price_usd) or 0,
                token_id,
            ),
        )

        log.info(
            "[outcome] Snapshot stored for %s: liq=$%s price=$%s holders=%d top=%.1f%% status=%s",
            token_addr[:12],
            _sqlite_native(snapshot.liquidity_usd),
            _sqlite_native(snapshot.price_usd),
            snapshot.holder_count,
            _sqlite_native(snapshot.top_holder_pct) * 100,
            snapshot.status,
        )
