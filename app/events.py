"""Live event log: in-memory ring buffer + SSE streaming."""
import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from typing import AsyncIterator

_buffer: deque = deque(maxlen=300)
_subscribers: set[asyncio.Queue] = set()
_loop: asyncio.AbstractEventLoop | None = None


def set_loop(loop: asyncio.AbstractEventLoop):
    global _loop
    _loop = loop


def push(level: str, message: str):
    evt = {
        "t": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "lvl": level,
        "msg": message,
    }
    _buffer.append(evt)
    if _loop is None:
        return
    for q in list(_subscribers):
        _loop.call_soon_threadsafe(_safe_put, q, evt)


def _safe_put(q: asyncio.Queue, evt: dict):
    try:
        q.put_nowait(evt)
    except asyncio.QueueFull:
        pass


def history() -> list[dict]:
    return list(_buffer)


def clear_buffer():
    _buffer.clear()


async def subscribe() -> AsyncIterator[dict]:
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _subscribers.add(q)
    try:
        for evt in list(_buffer):
            yield evt
        while True:
            yield await q.get()
    finally:
        _subscribers.discard(q)


class _Handler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        try:
            push(record.levelname, record.getMessage())
        except Exception:
            pass


def install_log_handler():
    h = _Handler()
    h.setLevel(logging.INFO)
    # capture from our own modules only — skip noisy libs
    for name in ("app", "app.worker", "app.watcher", "app.api.routes"):
        logging.getLogger(name).addHandler(h)
