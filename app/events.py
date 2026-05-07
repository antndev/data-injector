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


async def subscribe(idle_timeout: float | None = None) -> AsyncIterator[dict | None]:
    """Yield events as they arrive. When `idle_timeout` is set, yield `None`
    after that many seconds of silence — the SSE handler turns those into
    keepalive comment lines so intermediate proxies don't kill the
    connection during quiet periods."""
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _subscribers.add(q)
    try:
        for evt in list(_buffer):
            yield evt
        while True:
            if idle_timeout is None:
                yield await q.get()
                continue
            try:
                yield await asyncio.wait_for(q.get(), timeout=idle_timeout)
            except asyncio.TimeoutError:
                yield None  # caller should send a keepalive
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
    # Attach only to the root "app" logger — children (app.worker,
    # app.watcher, app.api.routes) propagate up automatically. Adding to
    # each child as well caused every log line to be emitted twice.
    logging.getLogger("app").addHandler(h)
