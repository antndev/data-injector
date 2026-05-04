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
CREATE INDEX IF NOT EXISTS idx_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_hash   ON files(file_hash);
"""


async def init_db():
    async with aiosqlite.connect(settings.db_path) as db:
        await db.executescript(_CREATE)
        await db.commit()


def connect():
    return aiosqlite.connect(settings.db_path)
