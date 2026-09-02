import json
import os
import re
import secrets
import shutil
import asyncio
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import aiosqlite
from fastapi import APIRouter, HTTPException, Request, Form, Header, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app import events
from app.config import settings
from app.database import connect
from app import openwebui
from app import jobs
from app.worker import trigger_scan

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
audit = logging.getLogger("audit")
logger = logging.getLogger(__name__)

StatusFilter = Literal[
    "queued", "processing", "done", "failed", "duplicate", "unsupported",
]
_SSE_KEEPALIVE_S = 30.0

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def _check_uuid(file_id: str) -> None:
    if not _UUID_RE.match(file_id):
        raise HTTPException(400, "Invalid file id")


def _like_escape(s: str) -> str:
    """Escape LIKE wildcards so a search for 'a_b' or '50%' matches literally
    instead of '_' meaning any-char and '%' meaning anything. Paired with
    LIKE ? ESCAPE '\\' in the query."""
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


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
        return xff.split(",")[0].strip() or "?"
    return request.client.host if request.client else "?"


_MAX_ATTEMPTS = 5
_LOCKOUT_SECONDS = 300
_RATE_MAX_ENTRIES = 5000

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
    return templates.TemplateResponse(
        request=request, name="index.html",
        context={
            "upload_chunk_bytes": settings.upload_chunk_bytes,
            "delete_after_ingest": settings.delete_after_ingest,
        },
    )


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

    if secrets.compare_digest(password, settings.admin_password):
        _reset(ip)
        request.session["authed"] = True
        request.session["login_time"] = int(time.time())
        audit.info("login-success from %s", ip)
        return RedirectResponse("/", status_code=303)

    count = _record_failure(ip)
    if count >= _MAX_ATTEMPTS:
        audit.warning("login-locked: %s after %d attempts", ip, _MAX_ATTEMPTS)
        return RedirectResponse(
            f"/login?error=locked&wait={_LOCKOUT_SECONDS}", status_code=303
        )
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
        async for evt in events.subscribe(idle_timeout=_SSE_KEEPALIVE_S):
            if evt is None:
                yield ": ping\n\n"
            else:
                yield f"data: {json.dumps(evt)}\n\n"
    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/health")
async def health(request: Request):
    if not getattr(request.app.state, "worker_alive", True):
        return JSONResponse(
            {"ok": False, "worker": "dead", "version": settings.app_version},
            status_code=503,
        )
    return {"ok": True, "version": settings.app_version}


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
    """Counts overall and split by lane.

    A slide, a scan or a recording has to go through a model, a spreadsheet
    does not, and the two differ by more than an order of magnitude. One
    blended rate over both produces an estimate that swings with whatever
    happens to be in the queue, so the lanes are reported apart and the
    dashboard estimates each on its own."""
    from app.worker import MEDIA_SUFFIXES

    tests = " OR ".join(f"lower(filename) LIKE '%{e}'" for e in sorted(MEDIA_SUFFIXES))
    sql = (
        "SELECT status,"
        f" SUM(CASE WHEN {tests} THEN 1 ELSE 0 END) AS media,"
        " COUNT(*) AS n"
        " FROM files GROUP BY status"
    )
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql) as cur:
            rows = await cur.fetchall()
    counts = {r["status"]: r["n"] for r in rows}
    media = {r["status"]: (r["media"] or 0) for r in rows}

    def lane(key):
        def n(bucket):
            hit = media.get(bucket, 0)
            return hit if key == "media" else counts.get(bucket, 0) - hit

        return {
            "queued": n("queued"),
            "processing": n("processing"),
            "settled": sum(n(st) for st in ("done", "failed", "duplicate", "unsupported")),
        }

    return {
        "queued":      counts.get("queued", 0),
        "processing":  counts.get("processing", 0),
        "done":        counts.get("done", 0),
        "failed":      counts.get("failed", 0),
        "duplicates":  counts.get("duplicate", 0),
        "unsupported": counts.get("unsupported", 0),
        "total":       sum(counts.values()),
        "lanes": {"media": lane("media"), "plain": lane("plain")},
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
        conds.append("filename LIKE ? ESCAPE '\\'"); params.append(f"%{_like_escape(search)}%")
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
        conds.append("filename LIKE ? ESCAPE '\\'"); params.append(f"%{_like_escape(search)}%")
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

    remote = row.get("openwebui_file_id")
    if remote:
        try:
            await openwebui.remove(remote)
        except Exception as exc:
            logger.error("could not remove %s from OpenWebUI: %s", row["filename"], exc)
            raise HTTPException(502, f"OpenWebUI delete failed: {exc}")

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

    dest = settings.inbox_dir / row["filename"]
    counter = 1
    while dest.exists():
        dest = settings.inbox_dir / f"{src.stem}_{counter}{src.suffix}"
        counter += 1
    try:
        shutil.move(str(src), dest)
    except FileNotFoundError:
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


class JobStart(BaseModel):
    kind: Literal["delete", "retry"]
    ids: list = []


async def _job_delete(file_id: str) -> bool:
    await delete_file(file_id, True)
    return True


async def _job_retry(file_id: str) -> bool:
    await retry_file(file_id)
    return True


jobs.register("delete", _job_delete)
jobs.register("retry", _job_retry)


@router.post("/jobs")
async def start_job(body: JobStart):
    if not body.ids:
        raise HTTPException(400, "no ids")
    running = await jobs.active()
    if running:
        raise HTTPException(409, "a job is already running")
    job_id = await jobs.create(body.kind, body.ids)
    audit.info("job %s started: %s over %d files", job_id[:8], body.kind, len(body.ids))
    return {"id": job_id}


@router.get("/jobs/active")
async def active_job():
    return await jobs.active() or {}


@router.get("/jobs/{job_id}")
async def job_status(job_id: str):
    job = await jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    return {"ok": await jobs.cancel(job_id)}


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


_UPLOAD_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_UPLOAD_INNER_BLOCK = 1 << 20
_MAX_NAME_BYTES = 200

_upload_locks: dict[str, asyncio.Lock] = {}


def _safe_filename(raw: str | None) -> str:
    """Reduce a browser-supplied filename to a single safe basename."""
    name = (raw or "").replace("\\", "/").rsplit("/", 1)[-1]
    name = "".join(c for c in name if c >= " " and c != "\x7f")
    name = name.strip(" .")
    if not name or name in (".", "..") or len(name.encode("utf-8", errors="ignore")) > _MAX_NAME_BYTES:
        if len(name.encode("utf-8", errors="ignore")) > _MAX_NAME_BYTES:
            ext = Path(name).suffix[:20]
            stem = Path(name).stem
            stem = stem.encode("utf-8")[: _MAX_NAME_BYTES - len(ext.encode("utf-8"))]
            name = stem.decode("utf-8", errors="ignore").rstrip() + ext
        if not name or name in (".", ".."):
            name = "unnamed"
    return name


def _check_upload_id(upload_id: str) -> None:
    if not _UPLOAD_ID_RE.match(upload_id):
        raise HTTPException(400, "Invalid upload id")


def _staging_paths(upload_id: str) -> tuple[Path, Path]:
    d = settings.uploads_tmp_dir
    return d / f"{upload_id}.part", d / f"{upload_id}.json"


def _lock_for(upload_id: str) -> asyncio.Lock:
    if len(_upload_locks) > 256:
        for uid in [u for u in _upload_locks
                    if not _upload_locks[u].locked() and not _staging_paths(u)[0].exists()]:
            _upload_locks.pop(uid, None)
    lock = _upload_locks.get(upload_id)
    if lock is None:
        lock = asyncio.Lock()
        _upload_locks[upload_id] = lock
    return lock


def _inbox_dest(name: str) -> Path:
    """Collision-safe destination in the inbox (same scheme as worker._move)."""
    dest = settings.inbox_dir / name
    counter = 1
    while dest.exists():
        dest = settings.inbox_dir / f"{Path(name).stem}_{counter}{Path(name).suffix}"
        counter += 1
    return dest


def _read_sidecar(sidecar: Path) -> dict | None:
    try:
        return json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


class UploadCreate(BaseModel):
    filename: str
    size: int
    fingerprint: str = ""


class PrecheckBody(BaseModel):
    hashes: list[str]


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@router.post("/uploads/precheck")
async def precheck_uploads(body: PrecheckBody):
    """Given content SHA-256 hashes, return which are ALREADY ingested (or in
    flight). The dashboard hashes each file locally and calls this before
    uploading, so unchanged files in a re-uploaded folder are skipped entirely
    instead of wasting upload bandwidth only to be flagged a duplicate on
    arrival. Dedup is still enforced server-side for anything that does upload."""
    hashes = list(dict.fromkeys(h for h in body.hashes if _SHA256_RE.match(h or "")))[:2000]
    if not hashes:
        return {"known": []}
    placeholders = ",".join("?" * len(hashes))
    q = (f"SELECT DISTINCT file_hash FROM files WHERE file_hash IN ({placeholders}) "
         "AND status NOT IN ('deleted','failed','duplicate','unsupported')")
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, hashes) as cur:
            rows = await cur.fetchall()
    return {"known": [r["file_hash"] for r in rows]}


@router.post("/uploads")
async def create_upload(body: UploadCreate):
    """Create — or resume — an upload. Returns the upload_id and how many bytes
    the server already holds (0 for a fresh upload)."""
    if body.size <= 0:
        raise HTTPException(400, "size must be > 0")
    if settings.upload_max_bytes and body.size > settings.upload_max_bytes:
        raise HTTPException(413, "File exceeds the maximum upload size")

    name = _safe_filename(body.filename)
    settings.uploads_tmp_dir.mkdir(parents=True, exist_ok=True)

    if body.fingerprint:
        for sidecar in settings.uploads_tmp_dir.glob("*.json"):
            meta = _read_sidecar(sidecar)
            if not meta:
                continue
            if meta.get("fingerprint") == body.fingerprint and meta.get("size") == body.size:
                uid = sidecar.stem
                part, _ = _staging_paths(uid)
                return {"upload_id": uid, "offset": part.stat().st_size if part.exists() else 0}

    upload_id = uuid.uuid4().hex
    part, sidecar = _staging_paths(upload_id)
    part.touch()
    sidecar.write_text(json.dumps({
        "filename": name,
        "size": body.size,
        "fingerprint": body.fingerprint,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }), encoding="utf-8")
    return {"upload_id": upload_id, "offset": 0}


@router.head("/uploads/{upload_id}")
async def head_upload(upload_id: str):
    """Authoritative resume offset for a staged upload."""
    _check_upload_id(upload_id)
    part, sidecar = _staging_paths(upload_id)
    meta = _read_sidecar(sidecar) if sidecar.exists() else None
    if meta is None or not part.exists():
        raise HTTPException(404, "No such upload")
    return Response(status_code=200, headers={
        "Upload-Offset": str(part.stat().st_size),
        "Upload-Length": str(meta.get("size", 0)),
        "Cache-Control": "no-store",
    })


@router.patch("/uploads/{upload_id}")
async def patch_upload(
    upload_id: str,
    request: Request,
    upload_offset: str | None = Header(default=None),
):
    """Append the request body at `Upload-Offset`. Returns 204 + the new
    Upload-Offset; on a stale offset returns 409 + the true offset; on the
    final chunk atomically moves the file into the inbox and adds
    Upload-Complete: 1."""
    _check_upload_id(upload_id)
    if upload_offset is None or not upload_offset.lstrip("-").isdigit():
        raise HTTPException(400, "Missing or invalid Upload-Offset header")
    upload_offset = int(upload_offset)
    part, sidecar = _staging_paths(upload_id)
    meta = _read_sidecar(sidecar) if sidecar.exists() else None
    if meta is None or not part.exists():
        raise HTTPException(404, "No such upload")
    size = int(meta.get("size", 0))

    async with _lock_for(upload_id):
        if not part.exists() or not sidecar.exists():
            raise HTTPException(404, "No such upload")
        real = part.stat().st_size
        if upload_offset != real:
            return Response(status_code=409, headers={"Upload-Offset": str(real)})

        overshoot = False
        with part.open("ab") as fh:
            written = 0
            async for chunk in request.stream():
                if not chunk:
                    continue
                room = size - (real + written)
                if room <= 0 or len(chunk) > room:
                    overshoot = True
                    break
                fh.write(chunk); written += len(chunk)
            fh.flush()
            os.fsync(fh.fileno())

        if overshoot:
            raise HTTPException(400, "Upload exceeded its declared size")

        offset = part.stat().st_size
        if offset < size:
            return Response(status_code=204, headers={"Upload-Offset": str(offset)})

        dest = _inbox_dest(_safe_filename(meta.get("filename")))
        os.replace(part, dest)
        sidecar.unlink(missing_ok=True)
        _upload_locks.pop(upload_id, None)
        events.push("INFO", f"Upload complete: {dest.name}")
        trigger_scan()
        return Response(status_code=204, headers={
            "Upload-Offset": str(size),
            "Upload-Complete": "1",
        })


@router.delete("/uploads/{upload_id}")
async def cancel_upload(upload_id: str):
    """Discard a staged upload."""
    _check_upload_id(upload_id)
    part, sidecar = _staging_paths(upload_id)
    part.unlink(missing_ok=True)
    sidecar.unlink(missing_ok=True)
    _upload_locks.pop(upload_id, None)
    return Response(status_code=204)
