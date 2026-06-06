"""
Batched writer for the OpenWebUI SQLite database.

webui.db has one global write lock. With WORKER_CONCURRENCY=64, every file
hitting it independently would queue on that lock and serialize the whole
pipeline at the very end of each file's processing — even when embeddings,
Qdrant upserts and the OS-level disk are all idle. That single lock turned
out to be the actual throughput ceiling on this box (ingestor CPU sitting
at ~3 %, GPU at ~10 %, but throughput stuck at ~2 files / s).

This module routes every register / unregister through a single async
writer task: it pulls items off a queue, opens one connection, and
applies them in one transaction every ~200 ms (or as soon as the queue
non-empty).  Each enqueueing coroutine still awaits its own future, so
callers see normal back-pressure and exception propagation.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

from app.config import settings
from app.database import owui_connect

logger = logging.getLogger(__name__)

# Each item is (op_name, args, future).
_queue: asyncio.Queue[tuple[str, tuple, asyncio.Future]] | None = None
_writer_task: asyncio.Task | None = None
_FLUSH_INTERVAL_S = 0.2


def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


async def _apply_register(db, file_id: str, filename: str,
                          file_hash: str, size: int) -> None:
    now = _now()
    meta = json.dumps({"name": filename, "content_type": "text/plain", "size": size})
    data = json.dumps({
        "collection_name": settings.qdrant_knowledge_base_id,
        "content": "",
        "metadata": {"name": filename},
    })
    await db.execute(
        """INSERT OR REPLACE INTO file
           (id, user_id, filename, meta, created_at, hash, data, updated_at, path)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (file_id, settings.openwebui_user_id, filename, meta, now,
         file_hash, data, now, ""),
    )
    await db.execute("DELETE FROM knowledge_file WHERE file_id=?", (file_id,))
    await db.execute(
        """INSERT INTO knowledge_file
           (id, user_id, knowledge_id, file_id, created_at, updated_at)
           VALUES (?,?,?,?,?,?)""",
        (str(uuid.uuid4()), settings.openwebui_user_id,
         settings.qdrant_knowledge_base_id, file_id, now, now),
    )


async def _apply_unregister(db, file_id: str) -> None:
    await db.execute("DELETE FROM knowledge_file WHERE file_id=?", (file_id,))
    await db.execute("DELETE FROM file WHERE id=?", (file_id,))


_HANDLERS = {
    "register": _apply_register,
    "unregister": _apply_unregister,
}


async def _writer_loop() -> None:
    """Drain the queue forever. Coalesce concurrent ops into single
    transactions: as soon as one item arrives, wait briefly for stragglers,
    then flush them all in one commit.

    Each op runs inside its own SAVEPOINT and resolves ITS OWN future with
    success or the exact exception it raised — never the batch-wide outcome.
    Previously a per-op failure (e.g. a webui.db schema mismatch) was logged
    and swallowed while every future was set to success, so the worker marked
    files 'done' that had never actually been written into the Knowledge Base.
    """
    assert _queue is not None
    while True:
        try:
            first = await _queue.get()
        except asyncio.CancelledError:
            raise
        batch = [first]

        # Brief flush window to coalesce concurrent ops
        deadline = asyncio.get_running_loop().time() + _FLUSH_INTERVAL_S
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                item = await asyncio.wait_for(_queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            except asyncio.CancelledError:
                # Resolve everything we already pulled before bailing out, so
                # no caller is left awaiting a future that never completes.
                _fail_batch(batch, asyncio.CancelledError())
                raise
            batch.append(item)

        try:
            async with owui_connect() as db:
                errors: list[Exception | None] = []
                for op, args, _fut in batch:
                    handler = _HANDLERS[op]
                    try:
                        await handler(db, *args)
                        errors.append(None)
                    except Exception as e:
                        # Record the per-op failure; its own caller's future is
                        # resolved with this exception below (the batch as a
                        # whole still commits the ops that succeeded). We avoid
                        # SAVEPOINTs here on purpose: under sqlite3's implicit
                        # transaction handling they interact badly, and a failed
                        # op self-heals on the next register (INSERT OR REPLACE).
                        logger.warning("OWUI %s failed for %r: %s", op, args, e)
                        errors.append(e)
                await db.commit()
        except asyncio.CancelledError:
            _fail_batch(batch, asyncio.CancelledError())
            raise
        except Exception as e:
            # Could not even open/commit the DB (e.g. wrong/unmounted path).
            # Fail every caller loudly rather than reporting phantom success.
            logger.exception("OWUI batch flush failed: %s", e)
            _fail_batch(batch, e)
            continue

        for (_, _, fut), err in zip(batch, errors):
            if fut.done():
                continue
            if err is not None:
                fut.set_exception(err)
            else:
                fut.set_result(None)


def _fail_batch(batch, exc: BaseException) -> None:
    for _, _, fut in batch:
        if not fut.done():
            fut.set_exception(exc)


def start_writer() -> None:
    global _queue, _writer_task
    if _writer_task is not None:
        return
    _queue = asyncio.Queue()
    _writer_task = asyncio.create_task(_writer_loop(), name="owui-writer")


async def stop_writer() -> None:
    global _writer_task
    if _writer_task is not None:
        _writer_task.cancel()
        try:
            await _writer_task
        except (asyncio.CancelledError, Exception):
            pass
        _writer_task = None
    # Drain anything still queued so no caller (e.g. a DELETE request not
    # tracked in the worker's task set) is left awaiting forever.
    if _queue is not None:
        while not _queue.empty():
            try:
                _, _, fut = _queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not fut.done():
                fut.set_exception(RuntimeError("OWUI writer shut down"))


async def register(file_id: str, filename: str, file_hash: str, size: int) -> None:
    assert _queue is not None, "OWUI writer not started"
    fut: asyncio.Future = asyncio.get_running_loop().create_future()
    await _queue.put(("register", (file_id, filename, file_hash, size), fut))
    await fut


async def unregister(file_id: str) -> None:
    assert _queue is not None, "OWUI writer not started"
    fut: asyncio.Future = asyncio.get_running_loop().create_future()
    await _queue.put(("unregister", (file_id,), fut))
    await fut
