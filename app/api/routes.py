from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.config import settings
from app.database import connect
from app.worker import _qdrant_client, _qdrant_delete

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@router.get("/status")
async def status():
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT status, COUNT(*) as n FROM files GROUP BY status"
        ) as cur:
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
    conditions = []

    if status:
        conditions.append("status = ?")
        params.append(status)
    if search:
        conditions.append("filename LIKE ?")
        params.append(f"%{search}%")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

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
        async with db.execute("SELECT * FROM files WHERE id = ?", (file_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="File not found")
    return dict(row)


@router.delete("/files/{file_id}")
async def delete_file(file_id: str, delete_physical: bool = False):
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM files WHERE id = ?", (file_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="File not found")

    row = dict(row)

    qdrant = _qdrant_client()
    await _qdrant_delete(qdrant, row["filename"])
    await qdrant.close()

    async with connect() as db:
        await db.execute(
            "UPDATE files SET status = 'deleted', deleted_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), file_id),
        )
        await db.commit()

    if delete_physical:
        done_path = settings.done_dir / row["filename"]
        if done_path.exists():
            done_path.unlink()

    return {"ok": True, "deleted": row["filename"]}


@router.post("/files/{file_id}/retry")
async def retry_file(file_id: str):
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM files WHERE id = ?", (file_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="File not found")

    row = dict(row)
    if row["status"] not in ("failed",):
        raise HTTPException(status_code=400, detail="Only failed files can be retried")

    src = settings.failed_dir / row["filename"]
    if not src.exists():
        raise HTTPException(status_code=404, detail="Physical file not found in /failed")

    dest = settings.inbox_dir / row["filename"]
    import shutil
    shutil.move(str(src), dest)

    error_file = settings.failed_dir / (row["filename"] + ".error")
    if error_file.exists():
        error_file.unlink()

    async with connect() as db:
        await db.execute(
            "UPDATE files SET status = 'queued', error_message = NULL WHERE id = ?",
            (file_id,),
        )
        await db.commit()

    return {"ok": True, "queued": row["filename"]}
