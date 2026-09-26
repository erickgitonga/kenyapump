"""
intelligence/reputation.py

Persistent adversarial memory for wallets seen across token launches.
Uses its own SQLite file so it doesn't interfere with scraper state.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

log = logging.getLogger("kenyapump.intelligence.reputation")

def _norm_addr(addr: str, chain: str = "") -> str:
    """
    Normalize an address for storage.
    EVM (hex) is case-insensitive → lowercase.
    Solana (base58) is case-sensitive → preserve.
    """
    if not addr:
        return addr
    # Solana addresses are base58, case-sensitive, and don't start with 0x
    if chain == "solana" or not addr.startswith("0x"):
        return addr
    return addr.lower()



DEFAULT_DB = "data/reputation.db"


class ReputationStore:
    """SQLite-backed persistent wallet reputation store."""

    def __init__(self, db_path: str = DEFAULT_DB):
        self._db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._init_schema()
        log.info(f"ReputationStore ready at {db_path}")

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wallets (
                address     TEXT PRIMARY KEY,
                first_seen  TEXT NOT NULL,
                role        TEXT,
                last_seen   TEXT NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wallet_token_edges (
                wallet_address  TEXT NOT NULL,
                token_address   TEXT NOT NULL,
                token_symbol    TEXT,
                chain           TEXT,
                role            TEXT NOT NULL,
                block_number    INTEGER,
                seen_at         TEXT NOT NULL,
                PRIMARY KEY (wallet_address, token_address, role)
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_edges_wallet ON wallet_token_edges(wallet_address)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_edges_token ON wallet_token_edges(token_address)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_edges_role ON wallet_token_edges(role)")
        self._conn.commit()

    async def close(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._conn.close)

    async def record_edge(
        self,
        wallet_address: str,
        token_address: str,
        token_symbol: str,
        chain: str,
        role: str,
        block_number: int = 0,
    ) -> None:
        async with self._lock:
            await asyncio.to_thread(
                self._record_edge_sync,
                wallet_address, token_address, token_symbol,
                chain, role, block_number,
            )

    def _record_edge_sync(self, wallet_address, token_address, token_symbol, chain, role, block_number):
        now = datetime.utcnow().isoformat()
        cur = self._conn.cursor()
        cur.execute("""
            INSERT INTO wallets (address, first_seen, role, last_seen)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET last_seen = excluded.last_seen
        """, (_norm_addr(wallet_address, chain), now, role, now))
        cur.execute("""
            INSERT OR IGNORE INTO wallet_token_edges
                (wallet_address, token_address, token_symbol, chain,
                 role, block_number, seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (_norm_addr(wallet_address, chain), _norm_addr(token_address, chain),
              token_symbol, chain, role, block_number, now))
        self._conn.commit()

    async def get_deployer_history(self, deployer_address: str) -> list:
        async with self._lock:
            return await asyncio.to_thread(self._deployer_history_sync, deployer_address)

    def _deployer_history_sync(self, address: str) -> list:
        cur = self._conn.cursor()
        cur.execute("""
            SELECT token_address, token_symbol, chain, seen_at
            FROM wallet_token_edges
            WHERE wallet_address = ? AND role = 'deployer'
            ORDER BY seen_at DESC
        """, (_norm_addr(address, "ethereum"),))
        return [dict(r) for r in cur.fetchall()]

    async def is_known_rugger(self, deployer_address: str) -> bool:
        history = await self.get_deployer_history(deployer_address)
        return len(history) >= 3

    async def top_repeat_deployers(self, min_launches: int = 3, limit: int = 20) -> list:
        async with self._lock:
            return await asyncio.to_thread(self._top_repeat_sync, min_launches, limit)

    def _top_repeat_sync(self, min_launches: int, limit: int) -> list:
        cur = self._conn.cursor()
        cur.execute("""
            SELECT wallet_address, COUNT(DISTINCT token_address) as launches
            FROM wallet_token_edges
            WHERE role = 'deployer'
            GROUP BY wallet_address
            HAVING launches >= ?
            ORDER BY launches DESC LIMIT ?
        """, (min_launches, limit))
        return [dict(r) for r in cur.fetchall()]

    async def top_repeated_symbols(self, limit: int = 20) -> list:
        async with self._lock:
            return await asyncio.to_thread(self._top_symbols_sync, limit)

    def _top_symbols_sync(self, limit: int) -> list:
        cur = self._conn.cursor()
        cur.execute("""
            SELECT token_symbol, COUNT(DISTINCT token_address) as appearances
            FROM wallet_token_edges
            WHERE role = 'deployer' AND token_symbol IS NOT NULL
            GROUP BY token_symbol
            HAVING appearances >= 2
            ORDER BY appearances DESC LIMIT ?
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]

    async def stats(self) -> dict:
        async with self._lock:
            return await asyncio.to_thread(self._stats_sync)

    def _stats_sync(self) -> dict:
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) FROM wallets")
        wallets = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM wallet_token_edges")
        edges = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT token_address) FROM wallet_token_edges")
        tokens = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT wallet_address) FROM wallet_token_edges WHERE role = 'deployer'")
        deployers = cur.fetchone()[0]
        return {"wallets": wallets, "edges": edges, "tokens": tokens, "deployers": deployers}
