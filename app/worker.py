import asyncio
import hashlib
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

    for row in rows:
        await _qdrant_delete(qdrant, row["filename"])
        async with connect() as db:
            await db.execute(
                "UPDATE files SET status = 'queued', error_message = NULL WHERE id = ?",
                (row["id"],),
            )
            await db.commit()

    await qdrant.close()


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------

async def run_worker(inbox_queue: asyncio.Queue):
    await recover_crashed()
    await _ensure_qdrant_collection()

    semaphore = asyncio.Semaphore(settings.worker_concurrency)

    # Also pick up any leftover 'queued' files from previous runs
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id FROM files WHERE status = 'queued'") as cur:
            queued = await cur.fetchall()
    for row in queued:
        await inbox_queue.put(None)  # signal: something to process

    while True:
        await inbox_queue.get()
        # Drain all pending signals into one processing pass
        while not inbox_queue.empty():
            inbox_queue.get_nowait()

        # Scan inbox for new files
        for path in settings.inbox_dir.iterdir():
            if path.is_file():
                asyncio.create_task(_process_with_semaphore(semaphore, path))

        # Also pick up queued DB entries (crash recovery or retry)
        async with connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT id, filename FROM files WHERE status = 'queued'"
            ) as cur:
                rows = await cur.fetchall()
        for row in rows:
            proc_path = settings.processing_dir / row["filename"]
            if proc_path.exists():
                asyncio.create_task(_process_with_semaphore(semaphore, proc_path, row["id"]))


async def _process_with_semaphore(sem: asyncio.Semaphore, path: Path, file_id: str | None = None):
    async with sem:
        await _process_file(path, file_id)


# ---------------------------------------------------------------------------
# File processing
# ---------------------------------------------------------------------------

async def _process_file(path: Path, file_id: str | None = None):
    ext = path.suffix.lower()

    # 1. File stability: check size twice
    if path.parent == settings.inbox_dir:
        size1 = path.stat().st_size
        await asyncio.sleep(10)
        if not path.exists():
            return
        if path.stat().st_size != size1:
            logger.info("File still growing, skipping for now: %s", path.name)
            return

    # 2. Unsupported format
    if ext in UNSUPPORTED_EXTS or (ext not in SUPPORTED and ext.lower() not in SUPPORTED):
        _move(path, settings.unsupported_dir)
        logger.info("Unsupported: %s", path.name)
        return

    # 3. Hash + duplicate check (only for inbox files)
    if path.parent == settings.inbox_dir:
        file_hash = _sha256(path)
        async with connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT id FROM files WHERE file_hash = ? AND status NOT IN ('deleted')",
                (file_hash,),
            ) as cur:
                existing = await cur.fetchone()
        if existing:
            _move(path, settings.duplicates_dir)
            logger.info("Duplicate: %s", path.name)
            return

    # 4. Atomic move to processing (if coming from inbox)
    if path.parent == settings.inbox_dir:
        dest = settings.processing_dir / path.name
        shutil.move(str(path), dest)
        path = dest
        file_hash = _sha256(path)
    else:
        file_hash = _sha256(path)

    # 5. Insert or update DB record
    if file_id is None:
        file_id = str(uuid.uuid4())
        async with connect() as db:
            await db.execute(
                """INSERT INTO files (id, filename, file_hash, file_size_bytes, status, qdrant_collection)
                   VALUES (?, ?, ?, ?, 'processing', ?)""",
                (file_id, path.name, file_hash, path.stat().st_size, settings.qdrant_collection),
            )
            await db.commit()
    else:
        async with connect() as db:
            await db.execute(
                "UPDATE files SET status = 'processing' WHERE id = ?", (file_id,)
            )
            await db.commit()

    logger.info("Processing: %s", path.name)

    tmpdir = None
    try:
        # 6. Legacy conversion
        if ext == ".doc":
            from app.parsers.legacy import convert
            path, tmpdir = await asyncio.get_event_loop().run_in_executor(
                None, convert, path, "docx"
            )
            ext = ".docx"
        elif ext == ".ppt":
            from app.parsers.legacy import convert
            path, tmpdir = await asyncio.get_event_loop().run_in_executor(
                None, convert, path, "pptx"
            )
            ext = ".pptx"

        # 7. Extract text
        text = await asyncio.get_event_loop().run_in_executor(None, _extract_text, path, ext)
        if not text.strip():
            raise ValueError("No text extracted from file")

        # 8. Chunk
        chunks = splitter.split_text(text)
        if not chunks:
            raise ValueError("No chunks produced")

        # 9. Embed + upload
        await _embed_and_upload(file_id, path.name, chunks)
        await _register_openwebui_file(file_id, path.name, file_hash, path.stat().st_size)

        # 10. Done
        done_path = settings.done_dir / path.name
        # Move the original processing file (before legacy conversion)
        orig_processing = settings.processing_dir / path.name if tmpdir else path
        if tmpdir:
            orig_processing = settings.processing_dir / _original_name(path.name, ext)
        if orig_processing.exists():
            shutil.move(str(orig_processing), done_path)

        async with connect() as db:
            await db.execute(
                """UPDATE files SET status = 'done', qdrant_chunk_count = ?,
                   ingested_at = ? WHERE id = ?""",
                (len(chunks), datetime.now(timezone.utc).isoformat(), file_id),
            )
            await db.commit()
        logger.info("Done: %s (%d chunks)", path.name, len(chunks))

    except Exception as exc:
        logger.exception("Failed: %s — %s", path.name, exc)
        orig = settings.processing_dir / path.name
        if orig.exists():
            _move(orig, settings.failed_dir)
            (settings.failed_dir / (path.name + ".error")).write_text(str(exc))
        async with connect() as db:
            await db.execute(
                "UPDATE files SET status = 'failed', error_message = ? WHERE id = ?",
                (str(exc), file_id),
            )
            await db.commit()
    finally:
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Text extraction dispatcher
# ---------------------------------------------------------------------------

def _extract_text(path: Path, ext: str) -> str:
    from app.parsers import pdf, office, text, msg

    if ext == ".pdf":
        return pdf.extract(path)
    if ext in {".pptx", ".pptm", ".ppsx"}:
        return office.extract_pptx(path)
    if ext in {".docx", ".docm", ".dotm"}:
        return office.extract_docx(path)
    if ext in {".xlsx", ".xlsm"}:
        return office.extract_xlsx(path)
    if ext == ".xls":
        return office.extract_xls(path)
    if ext == ".xlsb":
        return office.extract_xlsb(path)
    if ext == ".csv":
        return text.extract_csv(path)
    if ext in {".txt", ".md"}:
        return text.extract_txt(path)
    if ext == ".html":
        return text.extract_html(path)
    if ext == ".xml":
        return text.extract_xml(path)
    if ext == ".msg":
        return msg.extract(path)
    raise ValueError(f"No parser for extension: {ext}")


# ---------------------------------------------------------------------------
# Embedding + Qdrant upload
# ---------------------------------------------------------------------------

async def _embed_and_upload(file_id: str, filename: str, chunks: list[str]):
    ollama = OllamaClient(host=settings.ollama_host)
    qdrant = _qdrant_client()

    # Clean existing vectors for this file before inserting (idempotent)
    await _qdrant_delete(qdrant, filename)

    points = []
    batch_size = 32
    now = datetime.now(timezone.utc).isoformat()

    for i in range(0, len(chunks), batch_size):
        batch = chunks[i : i + batch_size]
        resp = await ollama.embed(model=settings.embedding_model, input=batch)
        for j, embedding in enumerate(resp.embeddings):
            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=embedding,
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

    await qdrant.close()


async def _qdrant_delete(qdrant: AsyncQdrantClient, filename: str):
    try:
        await qdrant.delete(
            collection_name=settings.qdrant_collection,
            points_selector=Filter(
                must=[FieldCondition(key="metadata.source_file", match=MatchValue(value=filename))]
            ),
        )
    except Exception:
        pass  # collection may not exist yet


async def _ensure_qdrant_collection():
    qdrant = _qdrant_client()
    try:
        collections = await qdrant.get_collections()
        names = [c.name for c in collections.collections]
        if settings.qdrant_collection not in names:
            await qdrant.create_collection(
                collection_name=settings.qdrant_collection,
                vectors_config=VectorParams(
                    size=settings.embedding_dimensions,
                    distance=Distance.COSINE,
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


def _move(src: Path, dest_dir: Path):
    dest = dest_dir / src.name
    counter = 1
    while dest.exists():
        dest = dest_dir / f"{src.stem}_{counter}{src.suffix}"
        counter += 1
    shutil.move(str(src), dest)


def _original_name(converted_name: str, converted_ext: str) -> str:
    stem = Path(converted_name).stem
    if converted_ext == ".docx":
        return stem + ".doc"
    if converted_ext == ".pptx":
        return stem + ".ppt"
    return converted_name

async def _register_openwebui_file(file_id: str, filename: str, file_hash: str, size: int):
    now = int(datetime.now(timezone.utc).timestamp())
    async with aiosqlite.connect("/openwebui-data/webui.db") as db:
        await db.execute(
            """INSERT OR REPLACE INTO file
               (id, user_id, filename, meta, created_at, hash, data, updated_at, path)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                file_id,
                settings.openwebui_user_id,
                filename,
                "{}",
                now,
                file_hash,
                '{"collection_name":"%s"}' % settings.qdrant_knowledge_base_id,
                now,
                "",
            ),
        )
        await db.execute(
            """INSERT OR REPLACE INTO knowledge_file
               (id, user_id, knowledge_id, file_id, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                str(uuid.uuid4()),
                settings.openwebui_user_id,
                settings.qdrant_knowledge_base_id,
                file_id,
                now,
                now,
            ),
        )
        await db.commit()