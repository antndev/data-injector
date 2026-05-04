import asyncio
import hashlib
import json
import logging
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Distance, FieldCondition, Filter, MatchValue, PointStruct, VectorParams
from langchain_text_splitters import RecursiveCharacterTextSplitter
from ollama import AsyncClient as OllamaClient

from app.config import settings
from app.database import connect
from app.watcher import SUPPORTED

logger = logging.getLogger(__name__)

UNSUPPORTED_EXTS = {".strings", ".nib", ".icns", ".plist"}

splitter = RecursiveCharacterTextSplitter(
    chunk_size=settings.chunk_size,
    chunk_overlap=settings.chunk_overlap,
)

# paths currently being processed — prevents duplicate tasks for the same file
active_paths: set[str] = set()
# growing files: re-checked on next worker tick instead of dropped
pending_growth: dict[str, int] = {}

_register_lock = asyncio.Lock()
_inbox_queue: asyncio.Queue | None = None  # set by run_worker; used by retry endpoint


# ---------------------------------------------------------------------------
# Startup recovery
# ---------------------------------------------------------------------------

async def recover_crashed():
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, filename FROM files WHERE status = 'processing'") as cur:
            rows = await cur.fetchall()
    if not rows:
        return

    logger.warning("Recovering %d crashed file(s)", len(rows))
    qdrant = _qdrant_client()
    try:
        for row in rows:
            await _qdrant_delete(qdrant, row["id"], row["filename"])
            async with connect() as db:
                await db.execute(
                    "UPDATE files SET status='queued', error_message=NULL WHERE id=?",
                    (row["id"],),
                )
                await db.commit()
    finally:
        await qdrant.close()


# ---------------------------------------------------------------------------
# Register new file as 'queued' immediately on detection
# ---------------------------------------------------------------------------

async def _register_as_queued(path: Path) -> str:
    async with _register_lock:
        async with connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT id FROM files WHERE filename=? AND status IN ('queued','processing')",
                (path.name,),
            ) as cur:
                existing = await cur.fetchone()
        if existing:
            return existing["id"]

        file_id = str(uuid.uuid4())
        size = path.stat().st_size if path.exists() else 0
        async with connect() as db:
            await db.execute(
                "INSERT INTO files (id, filename, file_size_bytes, status) VALUES (?,?,?,'queued')",
                (file_id, path.name, size),
            )
            await db.commit()
        logger.info("Queued: %s", path.name)
        return file_id


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------

async def run_worker(inbox_queue: asyncio.Queue):
    global _inbox_queue
    _inbox_queue = inbox_queue

    await recover_crashed()
    await _ensure_qdrant_collection()

    sem = asyncio.Semaphore(settings.worker_concurrency)

    # Resume queued files in processing dir (crashed between move and status flip)
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, filename FROM files WHERE status='queued'") as cur:
            queued = await cur.fetchall()
    for row in queued:
        proc = settings.processing_dir / row["filename"]
        if proc.exists() and str(proc) not in active_paths:
            active_paths.add(str(proc))
            asyncio.create_task(_process_with_sem(sem, proc, row["id"]))

    # Initial inbox sweep
    inbox_queue.put_nowait(None)

    while True:
        await inbox_queue.get()
        while not inbox_queue.empty():
            inbox_queue.get_nowait()

        for path in settings.inbox_dir.iterdir():
            if not path.is_file():
                continue
            key = str(path)
            if key in active_paths:
                continue
            active_paths.add(key)
            file_id = await _register_as_queued(path)
            asyncio.create_task(_process_with_sem(sem, path, file_id))


async def _process_with_sem(sem: asyncio.Semaphore, path: Path, file_id: str):
    async with sem:
        await _process_file(path, file_id)


def trigger_scan():
    """Wake worker (used by retry endpoint after moving a file back to inbox)."""
    if _inbox_queue is not None:
        _inbox_queue.put_nowait(None)


# ---------------------------------------------------------------------------
# File processing
# ---------------------------------------------------------------------------

async def _process_file(path: Path, file_id: str):
    original_path = path
    ext = path.suffix.lower()
    tmpdir = None
    proc_path: Path | None = None  # path inside processing dir (for cleanup)

    try:
        if not path.exists():
            return

        # 1. Stability check (inbox only)
        if path.parent == settings.inbox_dir:
            size1 = path.stat().st_size
            await asyncio.sleep(10)
            if not path.exists():
                return
            if path.stat().st_size != size1:
                logger.info("Still growing, will retry: %s", path.name)
                # release lock so the next worker tick can pick it up again
                active_paths.discard(str(original_path))
                # nudge worker to loop again after a short delay
                asyncio.create_task(_delayed_nudge(15))
                return

        # 2. Unsupported
        if ext in UNSUPPORTED_EXTS or ext not in SUPPORTED:
            _move(path, settings.unsupported_dir)
            await _set_status(file_id, "unsupported")
            logger.info("Unsupported: %s", path.name)
            return

        # 3. Duplicate check
        if path.parent == settings.inbox_dir:
            file_hash = _sha256(path)
            async with connect() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT id FROM files WHERE file_hash=? AND status NOT IN ('deleted','failed','duplicate','unsupported') AND id<>?",
                    (file_hash, file_id),
                ) as cur:
                    existing = await cur.fetchone()
            if existing:
                _move(path, settings.duplicates_dir)
                await _set_status(file_id, "duplicate")
                logger.info("Duplicate: %s", path.name)
                return

        # 4. Move to processing dir (atomic, with collision rename)
        if path.parent == settings.inbox_dir:
            proc_path = _move(path, settings.processing_dir)
            if proc_path.name != path.name:
                # collision rename happened — sync DB
                async with connect() as db:
                    await db.execute(
                        "UPDATE files SET filename=? WHERE id=?",
                        (proc_path.name, file_id),
                    )
                    await db.commit()
            path = proc_path
            file_hash = _sha256(path)
        else:
            proc_path = path
            file_hash = _sha256(path)

        # 5. Mark as processing
        async with connect() as db:
            await db.execute(
                "UPDATE files SET status='processing', file_hash=?, file_size_bytes=? WHERE id=?",
                (file_hash, path.stat().st_size, file_id),
            )
            await db.commit()
        logger.info("Processing: %s", path.name)

        # 6. Legacy conversion
        if ext == ".doc":
            from app.parsers.legacy import convert
            converted, tmpdir = await asyncio.get_event_loop().run_in_executor(None, convert, path, "docx")
            extracted_path, ext = converted, ".docx"
        elif ext == ".ppt":
            from app.parsers.legacy import convert
            converted, tmpdir = await asyncio.get_event_loop().run_in_executor(None, convert, path, "pptx")
            extracted_path, ext = converted, ".pptx"
        else:
            extracted_path = path

        # 7. Extract
        text = await asyncio.get_event_loop().run_in_executor(None, _extract_text, extracted_path, ext)
        if not text.strip():
            raise ValueError("No text extracted")

        # 8. Chunk
        chunks = splitter.split_text(text)
        if not chunks:
            raise ValueError("No chunks produced")

        # 9. Embed + upload
        await _embed_and_upload(file_id, proc_path.name, chunks)
        await _register_openwebui_file(file_id, proc_path.name, file_hash, proc_path.stat().st_size)

        # 10. Move to done
        if proc_path.exists():
            _move(proc_path, settings.done_dir)

        async with connect() as db:
            await db.execute(
                "UPDATE files SET status='done', qdrant_collection=?, qdrant_chunk_count=?, ingested_at=? WHERE id=?",
                (settings.qdrant_collection, len(chunks), datetime.now(timezone.utc).isoformat(), file_id),
            )
            await db.commit()
        logger.info("Done: %s (%d chunks)", proc_path.name, len(chunks))

    except Exception as exc:
        logger.exception("Failed: %s — %s", original_path.name, exc)
        # move whatever's in processing to failed
        if proc_path is not None and proc_path.exists():
            failed = _move(proc_path, settings.failed_dir)
            (settings.failed_dir / (failed.name + ".error")).write_text(str(exc), encoding="utf-8")
        async with connect() as db:
            await db.execute(
                "UPDATE files SET status='failed', error_message=? WHERE id=?",
                (str(exc)[:2000], file_id),
            )
            await db.commit()
    finally:
        active_paths.discard(str(original_path))
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)


async def _delayed_nudge(seconds: int):
    await asyncio.sleep(seconds)
    if _inbox_queue is not None:
        _inbox_queue.put_nowait(None)


async def _set_status(file_id: str, status: str):
    async with connect() as db:
        await db.execute("UPDATE files SET status=? WHERE id=?", (status, file_id))
        await db.commit()


# ---------------------------------------------------------------------------
# Text extraction dispatcher
# ---------------------------------------------------------------------------

def _extract_text(path: Path, ext: str) -> str:
    from app.parsers import pdf, office, text, msg
    if ext == ".pdf":                          return pdf.extract(path)
    if ext in {".pptx", ".pptm", ".ppsx"}:     return office.extract_pptx(path)
    if ext in {".docx", ".docm", ".dotm"}:     return office.extract_docx(path)
    if ext in {".xlsx", ".xlsm"}:              return office.extract_xlsx(path)
    if ext == ".xls":                          return office.extract_xls(path)
    if ext == ".xlsb":                         return office.extract_xlsb(path)
    if ext == ".csv":                          return text.extract_csv(path)
    if ext in {".txt", ".md"}:                 return text.extract_txt(path)
    if ext == ".html":                         return text.extract_html(path)
    if ext == ".xml":                          return text.extract_xml(path)
    if ext == ".msg":                          return msg.extract(path)
    raise ValueError(f"No parser for: {ext}")


# ---------------------------------------------------------------------------
# Embedding + Qdrant
# ---------------------------------------------------------------------------

async def _embed_and_upload(file_id: str, filename: str, chunks: list[str]):
    ollama = OllamaClient(host=settings.ollama_host)
    qdrant = _qdrant_client()
    try:
        await _qdrant_delete(qdrant, file_id, filename)

        points: list[PointStruct] = []
        now = datetime.now(timezone.utc).isoformat()
        for i in range(0, len(chunks), 32):
            batch = chunks[i : i + 32]
            resp = await ollama.embed(model=settings.embedding_model, input=batch)
            for j, vec in enumerate(resp.embeddings):
                points.append(
                    PointStruct(
                        id=str(uuid.uuid4()),
                        vector=vec,
                        payload={
                            "text": batch[j],
                            "metadata": {
                                "knowledge_base_id": settings.qdrant_knowledge_base_id,
                                "source_file": filename,
                                "file_id": file_id,
                                "chunk_index": i + j,
                                "ingested_at": now,
                            },
                            "tenant_id": settings.qdrant_knowledge_base_id,
                        },
                    )
                )
        for i in range(0, len(points), 100):
            await qdrant.upsert(
                collection_name=settings.qdrant_collection,
                points=points[i : i + 100],
            )
    finally:
        await qdrant.close()


async def _qdrant_delete(qdrant: AsyncQdrantClient, file_id: str, filename: str):
    """Delete vectors and openwebui rows for a given file_id (precise)."""
    try:
        await qdrant.delete(
            collection_name=settings.qdrant_collection,
            points_selector=Filter(
                must=[FieldCondition(key="metadata.file_id", match=MatchValue(value=file_id))]
            ),
        )
    except Exception as e:
        logger.warning("Qdrant delete failed for %s: %s", file_id, e)
    try:
        await _unregister_openwebui_file(file_id)
    except Exception as e:
        logger.warning("OpenWebUI unregister failed for %s: %s", file_id, e)


async def _ensure_qdrant_collection():
    qdrant = _qdrant_client()
    try:
        names = [c.name for c in (await qdrant.get_collections()).collections]
        if settings.qdrant_collection not in names:
            await qdrant.create_collection(
                collection_name=settings.qdrant_collection,
                vectors_config=VectorParams(
                    size=settings.embedding_dimensions, distance=Distance.COSINE
                ),
            )
            logger.info("Created Qdrant collection: %s", settings.qdrant_collection)
    finally:
        await qdrant.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _qdrant_client() -> AsyncQdrantClient:
    return AsyncQdrantClient(
        host=settings.qdrant_host,
        port=settings.qdrant_port,
        api_key=settings.qdrant_api_key,
        https=False,
    )


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def _move(src: Path, dest_dir: Path) -> Path:
    """Move src into dest_dir, renaming on collision. Returns final path."""
    dest = dest_dir / src.name
    counter = 1
    while dest.exists():
        dest = dest_dir / f"{src.stem}_{counter}{src.suffix}"
        counter += 1
    shutil.move(str(src), dest)
    return dest


# ---------------------------------------------------------------------------
# OpenWebUI integration
# ---------------------------------------------------------------------------

async def _register_openwebui_file(file_id: str, filename: str, file_hash: str, size: int):
    now = int(datetime.now(timezone.utc).timestamp())
    meta = json.dumps({"name": filename, "content_type": "text/plain", "size": size})
    data = json.dumps({
        "collection_name": settings.qdrant_knowledge_base_id,
        "content": "",
        "metadata": {"name": filename},
    })

    async with aiosqlite.connect("/openwebui-data/webui.db") as db:
        await db.execute(
            """INSERT OR REPLACE INTO file
               (id, user_id, filename, meta, created_at, hash, data, updated_at, path)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (file_id, settings.openwebui_user_id, filename, meta, now, file_hash, data, now, ""),
        )
        # remove any stale knowledge_file rows for this file_id, then insert fresh
        await db.execute("DELETE FROM knowledge_file WHERE file_id=?", (file_id,))
        await db.execute(
            """INSERT INTO knowledge_file
               (id, user_id, knowledge_id, file_id, created_at, updated_at)
               VALUES (?,?,?,?,?,?)""",
            (str(uuid.uuid4()), settings.openwebui_user_id,
             settings.qdrant_knowledge_base_id, file_id, now, now),
        )
        await db.commit()


async def _unregister_openwebui_file(file_id: str):
    async with aiosqlite.connect("/openwebui-data/webui.db") as db:
        await db.execute("DELETE FROM knowledge_file WHERE file_id=?", (file_id,))
        await db.execute("DELETE FROM file WHERE id=?", (file_id,))
        await db.commit()
