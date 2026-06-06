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


# 5 second busy_timeout: SQLite blocks until the lock frees, instead of
# erroring with "database is locked" the moment two connections collide.
# WAL mode allows concurrent readers + a single writer with no readers
# blocking each other — the right default for a dashboard + worker setup.
_BUSY_TIMEOUT_MS = 5000


async def init_db() -> None:
    async with aiosqlite.connect(settings.db_path) as db:
        # Mode persists across restarts once set on the database file.
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")  # safe + faster with WAL
        await db.executescript(_CREATE)
        await db.commit()


@asynccontextmanager
async def connect() -> AsyncIterator[aiosqlite.Connection]:
    """Open a connection to the main DB with sane defaults baked in."""
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        yield db


@asynccontextmanager
async def owui_connect() -> AsyncIterator[aiosqlite.Connection]:
    """Open a connection to the shared OpenWebUI database with the same
    busy_timeout — both we and openwebui itself write to webui.db, and
    without this writes can collide and error immediately.

    Opened in read-write mode via a file: URI so that a wrong/unmounted path
    raises instead of silently creating an empty webui.db that the writer
    would then happily write into while files never appear in the real KB.
    """
    uri = f"file:{settings.openwebui_db_path}?mode=rw"
    async with aiosqlite.connect(uri, uri=True) as db:
        await db.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        yield db
