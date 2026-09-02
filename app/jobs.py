"""Long running bulk operations that outlive the browser tab that started them.

Deleting a few thousand files used to be a loop in the dashboard: the page
fetched the ids and posted them back in batches. Closing the tab or hitting
reload stopped it halfway, and nothing said so. A job here lives in the
database instead. The remaining ids are stored with it, so the work survives a
refresh, a closed browser and a restart of the container.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

from app.database import connect

logger = logging.getLogger(__name__)

CHUNK = 25

_tasks: dict = {}
_cancelled: set = set()
_handlers: dict = {}


def register(kind: str, handler) -> None:
    _handlers[kind] = handler


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def create(kind: str, ids: list) -> str:
    if kind not in _handlers:
        raise ValueError(f"unknown job kind: {kind}")
    job_id = uuid.uuid4().hex
    async with connect() as db:
        await db.execute(
            "INSERT INTO jobs (id, kind, status, total, done, failed, pending, created_at)"
            " VALUES (?,?,'running',?,0,0,?,?)",
            (job_id, kind, len(ids), json.dumps(ids), _now()),
        )
        await db.commit()
    _spawn(job_id, kind)
    return job_id


async def get(job_id: str) -> dict | None:
    async with connect() as db:
        db.row_factory = __import__("aiosqlite").Row
        cur = await db.execute(
            "SELECT id, kind, status, total, done, failed, error, created_at, finished_at"
            " FROM jobs WHERE id=?",
            (job_id,),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def active() -> dict | None:
    async with connect() as db:
        db.row_factory = __import__("aiosqlite").Row
        cur = await db.execute(
            "SELECT id, kind, status, total, done, failed, error, created_at, finished_at"
            " FROM jobs WHERE status='running' ORDER BY created_at DESC LIMIT 1"
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def cancel(job_id: str) -> bool:
    _cancelled.add(job_id)
    task = _tasks.get(job_id)
    if task and not task.done():
        task.cancel()
    async with connect() as db:
        cur = await db.execute(
            "UPDATE jobs SET status='cancelled', finished_at=? WHERE id=? AND status='running'",
            (_now(), job_id),
        )
        await db.commit()
    return cur.rowcount > 0


async def prune(keep_hours: int = 24) -> None:
    async with connect() as db:
        await db.execute(
            "DELETE FROM jobs WHERE status!='running'"
            " AND finished_at IS NOT NULL"
            " AND finished_at < datetime('now', ?)",
            (f"-{keep_hours} hours",),
        )
        await db.commit()


def _spawn(job_id: str, kind: str) -> None:
    if job_id in _tasks and not _tasks[job_id].done():
        return
    _tasks[job_id] = asyncio.create_task(_run(job_id, kind))


async def resume_all() -> int:
    """Picks up jobs the previous process left running."""
    async with connect() as db:
        cur = await db.execute("SELECT id, kind FROM jobs WHERE status='running'")
        rows = await cur.fetchall()
    for job_id, kind in rows:
        if kind in _handlers:
            _spawn(job_id, kind)
            logger.info("resuming %s job %s", kind, job_id[:8])
        else:
            async with connect() as db:
                await db.execute(
                    "UPDATE jobs SET status='error', error=?, finished_at=? WHERE id=?",
                    (f"unknown job kind: {kind}", _now(), job_id),
                )
                await db.commit()
    return len(rows)


async def _load_pending(job_id: str) -> list:
    async with connect() as db:
        cur = await db.execute("SELECT pending FROM jobs WHERE id=?", (job_id,))
        row = await cur.fetchone()
    return json.loads(row[0]) if row and row[0] else []


async def _save(job_id: str, pending: list, done: int, failed: int) -> None:
    async with connect() as db:
        await db.execute(
            "UPDATE jobs SET pending=?, done=?, failed=? WHERE id=?",
            (json.dumps(pending), done, failed, job_id),
        )
        await db.commit()


async def _finish(job_id: str, status: str, error: str = None) -> None:
    async with connect() as db:
        await db.execute(
            "UPDATE jobs SET status=?, error=?, pending='[]', finished_at=? WHERE id=?",
            (status, error, _now(), job_id),
        )
        await db.commit()


async def _run(job_id: str, kind: str) -> None:
    handler = _handlers[kind]
    try:
        pending = await _load_pending(job_id)
        async with connect() as db:
            cur = await db.execute("SELECT done, failed FROM jobs WHERE id=?", (job_id,))
            row = await cur.fetchone()
        done, failed = (row[0], row[1]) if row else (0, 0)

        while pending:
            if job_id in _cancelled:
                await _finish(job_id, "cancelled")
                return
            batch, pending = pending[:CHUNK], pending[CHUNK:]
            results = await asyncio.gather(
                *[handler(i) for i in batch], return_exceptions=True
            )
            for r in results:
                if isinstance(r, Exception) or r is False:
                    failed += 1
                else:
                    done += 1
            await _save(job_id, pending, done, failed)
        await _finish(job_id, "done")
        logger.info("job %s finished: %d done, %d failed", job_id[:8], done, failed)
    except asyncio.CancelledError:
        await _finish(job_id, "cancelled")
        raise
    except Exception as exc:
        logger.exception("job %s failed", job_id[:8])
        await _finish(job_id, "error", str(exc)[:500])
    finally:
        _cancelled.discard(job_id)
        _tasks.pop(job_id, None)
