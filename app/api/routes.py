import json
import os
import secrets
import shutil
import asyncio
import logging
import time
import uuid
from collections import defaultdict
from pathlib import Path

import aiosqlite
from fastapi import APIRouter, HTTPException, Request, Form, UploadFile, File
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

_MAX_ATTEMPTS = 5
_LOCKOUT_SECONDS = 300

# IP → (failure_count, locked_until_timestamp)
_rate: dict[str, tuple[int, float]] = defaultdict(lambda: (0, 0.0))


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
    _rate[ip] = (0, 0.0)

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
    ip = request.client.host if request.client else "?"

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
    ip = request.client.host if request.client else "?"
    audit.info("logout from %s", ip)
    request.session.clear()
    return RedirectResponse("/login", status_code=303)



@router.get("/events")
async def event_stream():
    async def gen():
        async for evt in events.subscribe():
            yield f"data: {json.dumps(evt)}\n\n"
            # keepalive comments are sent by clients via reconnect; nothing else needed
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
async def list_file_ids(status: str | None = None, search: str | None = None):
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
    status: str | None = None,
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
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM files WHERE id=?", (file_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "File not found")
    return dict(row)


@router.delete("/files/{file_id}")
async def delete_file(file_id: str, delete_physical: bool = False):
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
    shutil.move(str(src), dest)

    err = settings.failed_dir / (row["filename"] + ".error")
    if err.exists():
        err.unlink()

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
    n = 0
    for fid in body.ids:
        try:
            await retry_file(fid)
            n += 1
        except HTTPException:
            pass
    return {"ok": True, "queued": n}


_UPLOAD_CHUNK = 1 << 20  # 1 MiB read chunks — keeps memory flat for large files
_MAX_NAME_BYTES = 200    # safe under the 255-byte ext4/ntfs limit


def _safe_filename(raw: str | None) -> str:
    """Reduce a browser-supplied filename to a single safe basename."""
    # Browsers may send forward- or backslash paths (e.g. webkitdirectory uploads).
    name = (raw or "").replace("\\", "/").rsplit("/", 1)[-1]
    # strip control chars and DEL
    name = "".join(c for c in name if c >= " " and c != "\x7f")
    # leading/trailing dots and spaces cause grief on Windows shares
    name = name.strip(" .")
    if not name or name in (".", "..") or len(name) > _MAX_NAME_BYTES:
        if len(name.encode("utf-8", errors="ignore")) > _MAX_NAME_BYTES:
            ext = Path(name).suffix[:20]
            stem = Path(name).stem
            stem = stem.encode("utf-8")[: _MAX_NAME_BYTES - len(ext.encode("utf-8"))]
            name = stem.decode("utf-8", errors="ignore").rstrip() + ext
        if not name or name in (".", ".."):
            name = "unnamed"
    return name


@router.post("/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    """
    Streams each upload to a staging dir, then atomically renames into the
    inbox once the full body has been received. The watcher only ever sees
    complete files, so a force-refresh / dropped connection mid-upload leaves
    nothing for the worker to pick up.
    """
    if not files:
        raise HTTPException(400, "No files provided")
    settings.uploads_tmp_dir.mkdir(parents=True, exist_ok=True)
    queued: list[str] = []
    pending: list[Path] = []  # staging files for the in-flight request
    try:
        for upload in files:
            name = _safe_filename(upload.filename)
            tmp = settings.uploads_tmp_dir / f"{uuid.uuid4().hex}.{name}.part"
            pending.append(tmp)
            try:
                with tmp.open("wb") as fh:
                    while chunk := await upload.read(_UPLOAD_CHUNK):
                        fh.write(chunk)
            finally:
                await upload.close()

            dest = settings.inbox_dir / name
            counter = 1
            while dest.exists():
                dest = settings.inbox_dir / f"{Path(name).stem}_{counter}{Path(name).suffix}"
                counter += 1
            os.replace(tmp, dest)  # atomic on the same filesystem
            queued.append(dest.name)
            trigger_scan()  # queue this file immediately, don't wait for the rest
    except Exception as exc:
        # Client disconnect or write failure — wipe any staged remains.
        for p in pending:
            p.unlink(missing_ok=True)
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(500, f"Upload failed: {exc}") from exc
    return {"ok": True, "queued": queued}
