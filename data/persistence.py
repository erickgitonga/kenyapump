"""
data/persistence.py

Async SQLite-backed store for scraper state. Survives restarts so the
scraper doesn't re-emit signals for pairs it already processed, and so
the block cursor resumes where it left off.

Design notes:
  - Uses a single connection with a lock — SQLite writes are serialized
    anyway, and asyncio.to_thread() keeps the event loop unblocked.
  - All public methods are async so callers can `await` uniformly.
  - The DB file lives at data/scraper.db by default.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from core.models import ChainId

log = logging.getLogger("kenyapump.data.persistence")

DEFAULT_DB_PATH = "data/scraper.db"


class ScraperStore:
    """Async SQLite-backed scraper state."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self._db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._init_schema()
        log.info("ScraperStore ready at %s", db_path)

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scraper_state (
                chain_id            TEXT PRIMARY KEY,
                last_scanned_block  INTEGER NOT NULL,
                updated_at          TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seen_pairs (
                pair_address TEXT PRIMARY KEY,
                token_address TEXT,
                dex           TEXT,
                status        TEXT NOT NULL,   -- 'pending' | 'qualified' | 'given_up'
                attempts      INTEGER DEFAULT 0,
                candidate_json TEXT,
                seen_at       TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_seen_status ON seen_pairs(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_seen_at ON seen_pairs(seen_at)")
        self._conn.commit()

    # --- cursor -------------------------------------------------------

    async def get_last_scanned_block(self, chain_id: ChainId) -> Optional[int]:
        async with self._lock:
            return await asyncio.to_thread(self._get_last_scanned_block_sync, chain_id)

    def _get_last_scanned_block_sync(self, chain_id: ChainId) -> Optional[int]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT last_scanned_block FROM scraper_state WHERE chain_id = ?",
            (chain_id.value,),
        )
        row = cur.fetchone()
        return row["last_scanned_block"] if row else None

    async def set_last_scanned_block(self, chain_id: ChainId, block: int) -> None:
        async with self._lock:
            await asyncio.to_thread(self._set_last_scanned_block_sync, chain_id, block)

    def _set_last_scanned_block_sync(self, chain_id: ChainId, block: int) -> None:
        cur = self._conn.cursor()
        cur.execute("""
            INSERT INTO scraper_state (chain_id, last_scanned_block, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chain_id) DO UPDATE SET
                last_scanned_block = excluded.last_scanned_block,
                updated_at         = excluded.updated_at
        """, (chain_id.value, block, datetime.utcnow().isoformat()))
        self._conn.commit()

    # --- seen pairs ---------------------------------------------------

    async def get_all_seen_pair_addresses(self) -> Set[str]:
        async with self._lock:
            return await asyncio.to_thread(self._get_all_seen_sync)

    def _get_all_seen_sync(self) -> Set[str]:
        cur = self._conn.cursor()
        cur.execute("SELECT pair_address FROM seen_pairs")
        return {row["pair_address"] for row in cur.fetchall()}

    async def get_pending_candidates(self) -> List[Dict[str, Any]]:
        """Return pending rows in the shape the scraper expects.

        The scraper reads `persisted_pending` on startup to rebuild its
        in-memory pending queue. Each entry should expose:
          - pair_address
          - candidate  (dict with token_address, pair_address, dex,
                        quote_symbol, block_number)
          - attempts
        """
        async with self._lock:
            return await asyncio.to_thread(self._get_pending_sync)

    def _get_pending_sync(self) -> List[Dict[str, Any]]:
        cur = self._conn.cursor()
        cur.execute("""
            SELECT pair_address, candidate_json, attempts
            FROM seen_pairs
            WHERE status = 'pending'
        """)
        result: List[Dict[str, Any]] = []
        for row in cur.fetchall():
            try:
                candidate = json.loads(row["candidate_json"]) if row["candidate_json"] else {}
            except json.JSONDecodeError:
                candidate = {}
            result.append({
                "pair_address": row["pair_address"],
                "candidate": candidate,
                "attempts": row["attempts"],
            })
        return result

    # --- lifecycle transitions ----------------------------------------

    async def mark_pending(
        self,
        pair_address: str,
        token_address: str = "",
        dex: str = "",
        candidate: Optional[Dict[str, Any]] = None,
        attempts: int = 0,
    ) -> None:
        """Record a newly discovered pair as pending enrichment.

        The scraper calls this both for fresh pairs and when re-saving
        an already-known pending pair with an updated attempt count.
        """
        async with self._lock:
            await asyncio.to_thread(
                self._upsert_pair_sync,
                pair_address, token_address, dex, "pending",
                attempts, candidate,
            )

    async def mark_qualified(self, pair_address: str) -> None:
        """Mark a pair as successfully enriched (score signal emitted)."""
        async with self._lock:
            await asyncio.to_thread(self._set_status_sync, pair_address, "qualified")

    async def mark_given_up(self, pair_address: str) -> None:
        """Mark a pair as permanently failed (retries exhausted)."""
        async with self._lock:
            await asyncio.to_thread(self._set_status_sync, pair_address, "given_up")

    # --- sync helpers -------------------------------------------------

    def _upsert_pair_sync(
        self,
        pair_address: str,
        token_address: str,
        dex: str,
        status: str,
        attempts: int,
        candidate: Optional[Dict[str, Any]],
    ) -> None:
        now = datetime.utcnow().isoformat()
        candidate_json = json.dumps(candidate) if candidate else None
        cur = self._conn.cursor()
        cur.execute("""
            INSERT INTO seen_pairs
                (pair_address, token_address, dex, status, attempts,
                 candidate_json, seen_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pair_address) DO UPDATE SET
                status         = excluded.status,
                attempts       = excluded.attempts,
                candidate_json = COALESCE(excluded.candidate_json,
                                          seen_pairs.candidate_json),
                token_address  = COALESCE(excluded.token_address,
                                          seen_pairs.token_address),
                dex            = COALESCE(excluded.dex, seen_pairs.dex),
                updated_at     = excluded.updated_at
        """, (
            pair_address.lower(),
            token_address.lower() if token_address else None,
            dex or None,
            status,
            attempts,
            candidate_json,
            now,  # seen_at (only used on first insert)
            now,
        ))
        self._conn.commit()

    def _set_status_sync(self, pair_address: str, status: str) -> None:
        cur = self._conn.cursor()
        cur.execute("""
            UPDATE seen_pairs
            SET status = ?, updated_at = ?
            WHERE pair_address = ?
        """, (status, datetime.utcnow().isoformat(), pair_address.lower()))
        self._conn.commit()

    # --- lifecycle ----------------------------------------------------

    async def close(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._conn.close)
