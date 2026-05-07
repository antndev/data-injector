import json
import re
import secrets
import shutil
import asyncio
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Literal

import aiosqlite
from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app import events
from app.config import settings
from app.database import connect
from app.worker import _qdrant_client, _qdrant_delete, trigger_scan

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
audit = logging.getLogger("audit")

# Restrict ?status= to known values so typos don't silently return empty.
StatusFilter = Literal[
    "queued", "processing", "done", "failed", "duplicate", "unsupported",
]
_SSE_KEEPALIVE_S = 30.0

# Reject obviously-malformed file IDs with a clean 400 instead of letting
# them produce SQL no-ops or template errors.
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def _check_uuid(file_id: str) -> None:
    if not _UUID_RE.match(file_id):
        raise HTTPException(400, "Invalid file id")


def _client_ip(request: Request) -> str:
    """Real client IP behind the Caddy reverse proxy.

    Caddy sets X-Forwarded-For. Without honouring it the rate limiter
    saw only the proxy IP, so one user's 5 wrong attempts would lock
    out *every* user for 5 minutes. Trusting the header is safe here
    because the app runs only behind Caddy (the session cookie is set
    https_only and the container isn't exposed directly).
    """
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        # Original client is the first hop; subsequent entries are proxies.
        return xff.split(",")[0].strip() or "?"
    return request.client.host if request.client else "?"


_MAX_ATTEMPTS = 5
_LOCKOUT_SECONDS = 300
_RATE_MAX_ENTRIES = 5000  # cap memory growth from spammy probers

# IP → (failure_count, locked_until_timestamp)
_rate: dict[str, tuple[int, float]] = defaultdict(lambda: (0, 0.0))


def _gc_rate(now: float) -> None:
    """Drop stale entries (no failures + not locked) to keep the table
    bounded under attack from many unique IPs."""
    if len(_rate) < _RATE_MAX_ENTRIES:
        return
    for ip in [ip for ip, (c, until) in _rate.items() if c == 0 and until <= now]:
        del _rate[ip]


def _is_locked(ip: str) -> tuple[bool, int]:
    _, locked_until = _rate[ip]
    remaining = int(locked_until - time.time())
    return remaining > 0, max(remaining, 0)


def _record_failure(ip: str) -> int:
    """Record a failed attempt. Returns attempt count; 0 means just locked."""
    count, locked_until = _rate[ip]
    count += 1
    if count >= _MAX_ATTEMPTS:
        _rate[ip] = (0, time.time() + _LOCKOUT_SECONDS)
        return _MAX_ATTEMPTS
    _rate[ip] = (count, locked_until)
    return count


def _reset(ip: str) -> None:
    _rate.pop(ip, None)

@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str | None = None):
    return templates.TemplateResponse(
        request=request, name="login.html", context={"error": error}
    )


@router.post("/login")
async def login(request: Request, password: str = Form(default="")):
    ip = _client_ip(request)
    _gc_rate(time.time())

    locked, remaining = _is_locked(ip)
    if locked:
        audit.warning("login-blocked: %s (locked %ds remaining)", ip, remaining)
        return RedirectResponse(f"/login?error=locked&wait={remaining}", status_code=303)

    # compare_digest prevents timing attacks
    if secrets.compare_digest(password, settings.admin_password):
        _reset(ip)
        request.session["authed"] = True
        request.session["login_time"] = int(time.time())
        audit.info("login-success from %s", ip)
        return RedirectResponse("/", status_code=303)

    count = _record_failure(ip)
    audit.warning("login-failure from %s (attempt %d/%d)", ip, count, _MAX_ATTEMPTS)
    return RedirectResponse("/login?error=1", status_code=303)


@router.post("/logout")
async def logout(request: Request):
    ip = _client_ip(request)
    audit.info("logout from %s", ip)
    request.session.clear()
    return RedirectResponse("/login", status_code=303)



@router.get("/events")
async def event_stream():
    async def gen():
        # Periodic SSE comment lines keep proxies (Caddy, nginx, browser
        # idle-detection) from killing the connection during quiet periods.
        async for evt in events.subscribe(idle_timeout=_SSE_KEEPALIVE_S):
            if evt is None:
                yield ": ping\n\n"  # SSE comment — clients ignore it
            else:
                yield f"data: {json.dumps(evt)}\n\n"
    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/health")
async def health():
    return {"ok": True}


@router.post("/events/clear")
async def clear_events():
    events.clear_buffer()
    return {"ok": True}


@router.get("/files/{file_id}/error")
async def get_file_error(file_id: str):
    """Return the saved error message for a failed file."""
    _check_uuid(file_id)
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT filename, error_message FROM files WHERE id=?", (file_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "File not found")

    msg = row["error_message"] or ""
    # Also pull the disk-side .error sidecar if present (more detail than DB truncation)
    err_path = settings.failed_dir / (row["filename"] + ".error")
    if err_path.exists():
        try:
            disk = err_path.read_text(encoding="utf-8", errors="replace")
            if disk and disk != msg:
                msg = disk
        except OSError:
            pass
    return {"filename": row["filename"], "error": msg}


@router.get("/status")
async def status():
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT status, COUNT(*) AS n FROM files GROUP BY status") as cur:
            rows = await cur.fetchall()
    counts = {r["status"]: r["n"] for r in rows}
    return {
        "queued":      counts.get("queued", 0),
        "processing":  counts.get("processing", 0),
        "done":        counts.get("done", 0),
        "failed":      counts.get("failed", 0),
        "duplicates":  counts.get("duplicate", 0),
        "unsupported": counts.get("unsupported", 0),
        "total":       sum(counts.values()),
    }


@router.get("/files/ids")
async def list_file_ids(status: StatusFilter | None = None, search: str | None = None):
    """Lightweight — returns only IDs (used by select-all to include unloaded rows)."""
    query = "SELECT id FROM files"
    params: list = []
    conds = []
    if status:
        conds.append("status=?"); params.append(status)
    if search:
        conds.append("filename LIKE ?"); params.append(f"%{search}%")
    if conds:
        query += " WHERE " + " AND ".join(conds)
    query += " ORDER BY created_at DESC"
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cur:
            rows = await cur.fetchall()
    return [r["id"] for r in rows]


@router.get("/files")
async def list_files(
    status: StatusFilter | None = None,
    search: str | None = None,
    offset: int = 0,
    limit: int = 200,
):
    query = "SELECT * FROM files"
    params: list = []
    conds = []
    if status:
        conds.append("status=?"); params.append(status)
    if search:
        conds.append("filename LIKE ?"); params.append(f"%{search}%")
    if conds:
        query += " WHERE " + " AND ".join(conds)
    limit = max(1, min(limit, 2000))
    offset = max(0, offset)
    query += f" ORDER BY created_at DESC LIMIT {limit} OFFSET {offset}"

    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


@router.get("/files/{file_id}")
async def get_file(file_id: str):
    _check_uuid(file_id)
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM files WHERE id=?", (file_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "File not found")
    return dict(row)


@router.delete("/files/{file_id}")
async def delete_file(file_id: str, delete_physical: bool = False):
    _check_uuid(file_id)
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM files WHERE id=?", (file_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "File not found")
    row = dict(row)

    await _qdrant_delete(_qdrant_client(), row["id"], row["filename"])

    async with connect() as db:
        await db.execute("DELETE FROM files WHERE id=?", (file_id,))
        await db.commit()

    if delete_physical:
        for d in (settings.done_dir, settings.failed_dir,
                  settings.duplicates_dir, settings.unsupported_dir,
                  settings.processing_dir, settings.inbox_dir):
            p = d / row["filename"]
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass
                err = d / (row["filename"] + ".error")
                if err.exists():
                    try:
                        err.unlink()
                    except OSError:
                        pass

    return {"ok": True, "deleted": row["filename"]}


@router.post("/files/{file_id}/retry")
async def retry_file(file_id: str):
    _check_uuid(file_id)
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM files WHERE id=?", (file_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "File not found")
    row = dict(row)
    if row["status"] != "failed":
        raise HTTPException(400, "Only failed files can be retried")

    src = settings.failed_dir / row["filename"]
    if not src.exists():
        raise HTTPException(404, "Physical file not found in /failed")

    # rename on collision in inbox
    dest = settings.inbox_dir / row["filename"]
    counter = 1
    while dest.exists():
        dest = settings.inbox_dir / f"{src.stem}_{counter}{src.suffix}"
        counter += 1
    try:
        shutil.move(str(src), dest)
    except FileNotFoundError:
        # Concurrent retry / delete won the race — surface 409 instead of 500.
        raise HTTPException(409, "File already moved by a concurrent request")

    err = settings.failed_dir / (row["filename"] + ".error")
    if err.exists():
        try:
            err.unlink()
        except OSError:
            pass

    async with connect() as db:
        await db.execute(
            "UPDATE files SET status='queued', error_message=NULL, filename=? WHERE id=?",
            (dest.name, file_id),
        )
        await db.commit()

    trigger_scan()
    return {"ok": True, "queued": dest.name}


class BulkBody(BaseModel):
    ids: list[str]


@router.post("/files/bulk/delete")
async def bulk_delete(body: BulkBody, delete_physical: bool = False):
    sem = asyncio.Semaphore(20)

    async def _one(fid: str) -> int:
        async with sem:
            try:
                await delete_file(fid, delete_physical)
                return 1
            except HTTPException:
                return 0

    results = await asyncio.gather(*[_one(fid) for fid in body.ids])
    return {"ok": True, "deleted": sum(results)}


@router.post("/files/bulk/retry")
async def bulk_retry(body: BulkBody):
    sem = asyncio.Semaphore(20)

    async def _one(fid: str) -> int:
        async with sem:
            try:
                await retry_file(fid)
                return 1
            except HTTPException:
                return 0

    results = await asyncio.gather(*[_one(fid) for fid in body.ids])
    return {"ok": True, "queued": sum(results)}
