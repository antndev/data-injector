from contextlib import asynccontextmanager
from typing import AsyncIterator

import aiosqlite

from app.config import settings

_CREATE = """
CREATE TABLE IF NOT EXISTS files (
    id                 TEXT PRIMARY KEY,
    filename           TEXT NOT NULL,
    file_hash          TEXT,
    file_size_bytes    INTEGER,
    status             TEXT DEFAULT 'queued',
    error_message      TEXT,
    qdrant_collection  TEXT,
    qdrant_chunk_count INTEGER,
    ingested_at        TIMESTAMP,
    deleted_at         TIMESTAMP,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_status     ON files(status);
CREATE INDEX IF NOT EXISTS idx_hash       ON files(file_hash);
-- Dashboard pagination orders by created_at DESC; without this every page
-- query did a full-table scan + temp sort.
CREATE INDEX IF NOT EXISTS idx_created_at ON files(created_at);
"""


_BUSY_TIMEOUT_MS = 5000


async def init_db() -> None:
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.executescript(_CREATE)
        await db.commit()


@asynccontextmanager
async def connect() -> AsyncIterator[aiosqlite.Connection]:
    """Open a connection to the main DB with sane defaults baked in."""
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        yield db
