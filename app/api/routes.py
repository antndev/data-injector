import json
import shutil
import asyncio
from datetime import datetime, timezone
from pathlib import Path

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


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str | None = None):
    return templates.TemplateResponse(request=request, name="login.html", context={"error": error})


@router.post("/login")
async def login(request: Request, pin: str = Form(...)):
    if pin == settings.admin_pin:
        request.session["authed"] = True
        return RedirectResponse("/", status_code=303)
    return RedirectResponse("/login?error=1", status_code=303)


@router.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


# ---------------------------------------------------------------------------
# Live event stream (SSE)
# ---------------------------------------------------------------------------

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


@router.get("/files")
async def list_files(status: str | None = None, search: str | None = None):
    query = "SELECT * FROM files"
    params: list = []
    conds = []
    if status:
        conds.append("status=?"); params.append(status)
    if search:
        conds.append("filename LIKE ?"); params.append(f"%{search}%")
    if conds:
        query += " WHERE " + " AND ".join(conds)
    query += " ORDER BY created_at DESC LIMIT 500"

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

    qdrant = _qdrant_client()
    try:
        await _qdrant_delete(qdrant, row["id"], row["filename"])
    finally:
        await qdrant.close()

    async with connect() as db:
        await db.execute(
            "UPDATE files SET status='deleted', deleted_at=? WHERE id=?",
            (datetime.now(timezone.utc).isoformat(), file_id),
        )
        await db.commit()

    if delete_physical:
        for d in (settings.done_dir, settings.failed_dir,
                  settings.duplicates_dir, settings.unsupported_dir):
            p = d / row["filename"]
            if p.exists():
                p.unlink()
                err = d / (row["filename"] + ".error")
                if err.exists():
                    err.unlink()
                break

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
    n = 0
    for fid in body.ids:
        try:
            await delete_file(fid, delete_physical)
            n += 1
        except HTTPException:
            pass
    return {"ok": True, "deleted": n}


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
