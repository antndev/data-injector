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
CREATE INDEX IF NOT EXISTS idx_created_at ON files(created_at);

CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running',
    total       INTEGER NOT NULL DEFAULT 0,
    done        INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    pending     TEXT NOT NULL DEFAULT '[]',
    error       TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
"""

_COLUMNS = [("files", "openwebui_file_id", "TEXT")]


_BUSY_TIMEOUT_MS = 5000


async def init_db() -> None:
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.executescript(_CREATE)
        for table, column, decl in _COLUMNS:
            cur = await db.execute(f"PRAGMA table_info({table})")
            if column not in [r[1] for r in await cur.fetchall()]:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        await db.commit()


@asynccontextmanager
async def connect() -> AsyncIterator[aiosqlite.Connection]:
    """Open a connection to the main DB with sane defaults baked in."""
    async with aiosqlite.connect(settings.db_path) as db:
        await db.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
        yield db
