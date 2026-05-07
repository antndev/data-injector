import asyncio
import hashlib
import json
import logging
import re
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
from app.database import connect, owui_connect
from app.watcher import SUPPORTED

logger = logging.getLogger(__name__)

UNSUPPORTED_EXTS = {".strings", ".nib", ".icns", ".plist"}

# Strip control characters that some embed models (notably bge-m3 via Ollama)
# choke on and return NaN for. PPTX slide-marker chunks like "--- Slide 5 ---"
# combined with stray control chars are the usual culprits.
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_MIN_CHUNK_CHARS = 3


def _clean_chunk(s: str) -> str:
    return _CONTROL_CHARS.sub("", s).strip()

splitter = RecursiveCharacterTextSplitter(
    chunk_size=settings.chunk_size,
    chunk_overlap=settings.chunk_overlap,
)

# paths currently being processed — prevents duplicate tasks for the same file
active_paths: set[str] = set()

# In-flight per-file processing tasks. Tracked so we can cancel + await them
# cleanly on shutdown instead of leaking orphans into the event loop.
_running_tasks: set[asyncio.Task] = set()

_register_lock = asyncio.Lock()
_inbox_queue: asyncio.Queue | None = None  # set by run_worker; used by retry endpoint

# ---------------------------------------------------------------------------
# Persistent clients — created once at startup, reused across all files.
# Eliminates per-file TCP handshake / connection overhead.
# ---------------------------------------------------------------------------
_qdrant_global: AsyncQdrantClient | None = None
_ollama_global: OllamaClient | None = None

# ---------------------------------------------------------------------------
# GLOBAL concurrency caps. Critically these must be shared across ALL files —
# prior versions created one Semaphore per file, which meant 64 concurrent
# files × 8 embed slots = 512 simultaneous Ollama requests, drowning the
# embedding server. With these at module level the cap is honoured globally.
# ---------------------------------------------------------------------------
_embed_sem: asyncio.Semaphore | None = None
_upsert_sem: asyncio.Semaphore | None = None


def _qdrant_client() -> AsyncQdrantClient:
    """Return the shared Qdrant client (used by routes.py too)."""
    assert _qdrant_global is not None, "Qdrant client not initialised yet"
    return _qdrant_global


def _spawn(coro) -> asyncio.Task:
    """Create a task and track it for clean shutdown."""
    task = asyncio.create_task(coro)
    _running_tasks.add(task)
    task.add_done_callback(_running_tasks.discard)
    return task


async def shutdown() -> None:
    """Cancel + await every in-flight processing task, then close clients."""
    for t in list(_running_tasks):
        t.cancel()
    if _running_tasks:
        await asyncio.gather(*_running_tasks, return_exceptions=True)
    if _qdrant_global is not None:
        try:
            await _qdrant_global.close()
        except Exception:
            pass


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
    for row in rows:
        await _qdrant_delete(_qdrant_global, row["id"], row["filename"])
        async with connect() as db:
            await db.execute(
                "UPDATE files SET status='queued', error_message=NULL WHERE id=?",
                (row["id"],),
            )
            await db.commit()


# ---------------------------------------------------------------------------
# Register new file as 'queued' immediately on detection
# ---------------------------------------------------------------------------

async def _register_as_queued(path: Path) -> str | None:
    """Insert a 'queued' row for `path`, or return the id of the existing
    row if one is already queued/processing. Returns None if the file
    disappeared before we could record its size."""
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

        # File can disappear between the watcher event and this stat call.
        try:
            size = path.stat().st_size
        except OSError:
            return None

        file_id = str(uuid.uuid4())
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
    global _inbox_queue, _qdrant_global, _ollama_global, _embed_sem, _upsert_sem
    _inbox_queue = inbox_queue

    # Initialise persistent clients once — reused for every file
    _qdrant_global = AsyncQdrantClient(
        host=settings.qdrant_host,
        port=settings.qdrant_port,
        api_key=settings.qdrant_api_key,
        https=False,
        # Generous timeout: bge-m3 batch upserts can be a few hundred MB
        # of payload, and Qdrant compaction occasionally pauses requests.
        timeout=60,
    )
    _ollama_global = OllamaClient(host=settings.ollama_host)

    # Global semaphores (see comment at module top — must NOT be per-file).
    _embed_sem = asyncio.Semaphore(settings.embed_concurrency)
    _upsert_sem = asyncio.Semaphore(settings.upsert_concurrency)

    await recover_crashed()
    await _ensure_qdrant_collection()

    sem = asyncio.Semaphore(settings.worker_concurrency)

    # Resume queued files that are already in the processing dir
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, filename FROM files WHERE status='queued'") as cur:
            queued = await cur.fetchall()
    for row in queued:
        proc = settings.processing_dir / row["filename"]
        if proc.exists() and str(proc) not in active_paths:
            active_paths.add(str(proc))
            _spawn(_process_with_sem(sem, proc, row["id"]))

    # Belt-and-braces in case the OS-level watcher misses an event.
    _spawn(_periodic_rescan())

    # Initial inbox sweep
    inbox_queue.put_nowait(None)

    while True:
        await inbox_queue.get()
        while not inbox_queue.empty():
            inbox_queue.get_nowait()

        # Wrap the scan so that a single bad file (e.g. a permission error
        # on stat) can never take the whole worker offline. Without this,
        # any uncaught exception here turns the app into a silent zombie
        # — dashboard up, queue full, nothing being processed.
        try:
            for path in settings.inbox_dir.iterdir():
                if not path.is_file():
                    continue
                key = str(path)
                if key in active_paths:
                    continue
                active_paths.add(key)
                try:
                    file_id = await _register_as_queued(path)
                except Exception:
                    active_paths.discard(key)
                    logger.exception("Failed to register %s — skipping", path.name)
                    continue
                if file_id is None:
                    # File vanished before we could register it.
                    active_paths.discard(key)
                    continue
                _spawn(_process_with_sem(sem, path, file_id))
        except Exception:
            logger.exception("Inbox scan failed — continuing")


async def _process_with_sem(sem: asyncio.Semaphore, path: Path, file_id: str):
    """
    Stability check runs OUTSIDE the semaphore so it doesn't burn a
    concurrency slot just sleeping.  The sem is only held while doing
    real work (hashing, extraction, embedding, Qdrant upsert).
    """
    if settings.stability_wait_s > 0 and path.parent == settings.inbox_dir:
        try:
            size1 = path.stat().st_size
        except OSError:
            active_paths.discard(str(path))
            return
        await asyncio.sleep(settings.stability_wait_s)
        if not path.exists():
            active_paths.discard(str(path))
            return
        try:
            if path.stat().st_size != size1:
                logger.info("Still growing, will retry: %s", path.name)
                active_paths.discard(str(path))
                asyncio.create_task(_delayed_nudge(15))
                return
        except OSError:
            active_paths.discard(str(path))
            return

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
    proc_path: Path | None = None

    try:
        if not path.exists():
            return

        # Bail out immediately if the file was deleted from the DB before we started
        # (e.g. deleted while sitting in the queue)
        async with connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT id FROM files WHERE id=?", (file_id,)) as cur:
                if not await cur.fetchone():
                    return

        loop = asyncio.get_running_loop()

        # 1. Unsupported extension
        if ext in UNSUPPORTED_EXTS or ext not in SUPPORTED:
            _move(path, settings.unsupported_dir)
            await _set_status(file_id, "unsupported")
            logger.info("Unsupported: %s", path.name)
            return

        # 2. Duplicate check (inbox only) — hash in thread pool (non-blocking)
        if path.parent == settings.inbox_dir:
            file_hash = await loop.run_in_executor(None, _sha256, path)
            async with connect() as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(
                    "SELECT id FROM files WHERE file_hash=? AND status NOT IN "
                    "('deleted','failed','duplicate','unsupported') AND id<>?",
                    (file_hash, file_id),
                ) as cur:
                    existing = await cur.fetchone()
            if existing:
                _move(path, settings.duplicates_dir)
                await _set_status(file_id, "duplicate")
                logger.info("Duplicate: %s", path.name)
                return

            # 3. Move to processing dir
            proc_path = _move(path, settings.processing_dir)
            if proc_path.name != path.name:
                async with connect() as db:
                    await db.execute(
                        "UPDATE files SET filename=? WHERE id=?",
                        (proc_path.name, file_id),
                    )
                    await db.commit()
            path = proc_path
            # reuse the hash computed above — content unchanged by a rename/move
        else:
            proc_path = path
            file_hash = await loop.run_in_executor(None, _sha256, path)

        # 4. Mark as processing
        async with connect() as db:
            await db.execute(
                "UPDATE files SET status='processing', file_hash=?, file_size_bytes=? WHERE id=?",
                (file_hash, path.stat().st_size, file_id),
            )
            await db.commit()
        logger.info("Processing: %s", path.name)

        # 5. Legacy format conversion (runs in thread pool).  May fail outright
        # for some .ppt/.doc files (e.g. needs Java filters that aren't available)
        # — in which case we skip the structured path and rely on the LibreOffice
        # txt fallback below.
        extracted_path = path
        extract_ext = ext
        if ext == ".doc":
            try:
                from app.parsers.legacy import convert
                converted, tmpdir = await loop.run_in_executor(None, convert, path, "docx")
                extracted_path, extract_ext = converted, ".docx"
            except Exception as e:
                logger.info("doc→docx failed for %s — will try txt fallback: %s",
                            path.name, str(e)[:120])
                extracted_path = None
        elif ext == ".ppt":
            try:
                from app.parsers.legacy import convert
                converted, tmpdir = await loop.run_in_executor(None, convert, path, "pptx")
                extracted_path, extract_ext = converted, ".pptx"
            except Exception as e:
                logger.info("ppt→pptx failed for %s — will try txt fallback: %s",
                            path.name, str(e)[:120])
                extracted_path = None

        # 6. Extract text — primary structured path
        text = ""
        if extracted_path is not None:
            try:
                text = await loop.run_in_executor(None, _extract_text, extracted_path, extract_ext)
            except Exception as e:
                logger.info("Structured extract failed for %s — will try txt fallback: %s",
                            path.name, str(e)[:120])

        # 6b. LibreOffice txt fallback — catches text in shapes / text boxes
        # that python-docx and python-pptx silently skip, and rescues files
        # whose structured conversion errored above.
        if not text.strip() and ext in {".doc", ".docx", ".ppt", ".pptx", ".pptm",
                                         ".docm", ".dotm", ".ppsx", ".xls", ".xlsx", ".xlsm"}:
            from app.parsers.legacy import convert_to_text
            try:
                text = await loop.run_in_executor(None, convert_to_text, path)
                if text.strip():
                    logger.info("Used txt fallback for %s (%d chars)", path.name, len(text))
            except Exception as e:
                logger.warning("txt fallback failed for %s: %s", path.name, str(e)[:200])

        if not text.strip():
            raise ValueError("No text extracted")

        # 7. Chunk (offloaded — splitting a multi-MB doc otherwise blocks the
        # event loop and stalls every other file's progress for seconds)
        chunks = await loop.run_in_executor(None, splitter.split_text, text)
        if not chunks:
            raise ValueError("No chunks produced")

        # 8. Embed, upsert to Qdrant, and register in OpenWebUI
        # Delete stale data first, then embed, then upsert+register in parallel.
        stored = await _embed_and_upload(file_id, proc_path.name, chunks, file_hash, proc_path.stat().st_size)

        # 9. Move to done
        if proc_path.exists():
            _move(proc_path, settings.done_dir)

        async with connect() as db:
            await db.execute(
                "UPDATE files SET status='done', qdrant_collection=?, qdrant_chunk_count=?, ingested_at=? WHERE id=?",
                (settings.qdrant_collection, stored, datetime.now(timezone.utc).isoformat(), file_id),
            )
            await db.commit()
        logger.info("Done: %s (%d chunks)", proc_path.name, stored)

    except Exception as exc:
        # Check whether the file still exists in the DB.  If it doesn't, the user
        # deleted it while we were processing — not a real failure, just clean up.
        async with connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT id FROM files WHERE id=?", (file_id,)) as cur:
                still_exists = await cur.fetchone()

        if not still_exists:
            logger.debug("Processing cancelled for %s: deleted while in progress", original_path.name)
            # Physical file may still be in processing dir — remove it
            if proc_path is not None and proc_path.exists():
                try:
                    proc_path.unlink()
                except OSError:
                    pass
        else:
            logger.exception("Failed: %s — %s", original_path.name, exc)
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


async def _periodic_rescan(every_s: int = 60):
    """Belt-and-braces: rescan the inbox at a slow tick so files aren't
    stranded if the watchdog Observer thread silently dies (rare but it
    has happened in production with mounted shares / SFTP)."""
    while True:
        try:
            await asyncio.sleep(every_s)
            if _inbox_queue is not None:
                _inbox_queue.put_nowait(None)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Periodic rescan tick failed (continuing)")


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
    if ext in {".pptx", ".pptm", ".ppsx"}:    return office.extract_pptx(path)
    if ext in {".docx", ".docm", ".dotm"}:    return office.extract_docx(path)
    if ext in {".xlsx", ".xlsm"}:             return office.extract_xlsx(path)
    if ext == ".xls":                         return office.extract_xls(path)
    if ext == ".xlsb":                        return office.extract_xlsb(path)
    if ext == ".csv":                         return text.extract_csv(path)
    if ext in {".txt", ".md"}:                return text.extract_txt(path)
    if ext == ".html":                        return text.extract_html(path)
    if ext == ".xml":                         return text.extract_xml(path)
    if ext == ".msg":                         return msg.extract(path)
    raise ValueError(f"No parser for: {ext}")


# ---------------------------------------------------------------------------
# Embedding + Qdrant
# ---------------------------------------------------------------------------

async def _embed_and_upload(
    file_id: str, filename: str, chunks: list[str],
    file_hash: str, file_size: int,
) -> int:
    """
    Full pipeline for one file. Returns the number of chunks actually
    embedded + stored.

    Delete phase:    Qdrant delete  ║  OpenWebUI unregister   (parallel)
    Embed phase:     batches fired concurrently, capped by the GLOBAL
                     embed semaphore so total Ollama load stays bounded.
    Finish phase:    Qdrant upsert  ║  OpenWebUI register      (parallel)
    """
    ollama = _ollama_global
    qdrant = _qdrant_global
    assert _embed_sem is not None and _upsert_sem is not None

    # ── 1. Delete old data (Qdrant vectors + OpenWebUI entry) in parallel ──
    async def _del_qdrant():
        try:
            await qdrant.delete(
                collection_name=settings.qdrant_collection,
                points_selector=Filter(
                    must=[FieldCondition(key="metadata.file_id", match=MatchValue(value=file_id))]
                ),
            )
        except Exception as e:
            logger.warning("Qdrant delete failed for %s: %s", file_id, e)

    async def _del_owui():
        try:
            await _unregister_openwebui_file(file_id)
        except Exception as e:
            logger.warning("OpenWebUI unregister failed for %s: %s", file_id, e)

    await asyncio.gather(_del_qdrant(), _del_owui())

    # ── 2. Clean + filter chunks, then embed all batches concurrently ─────
    # Drop empty / control-char-only / too-short chunks so Ollama doesn't
    # produce NaN embeddings (which then 500 the entire batch).
    chunks = [_clean_chunk(c) for c in chunks]
    chunks = [c for c in chunks if len(c) >= _MIN_CHUNK_CHARS]
    if not chunks:
        raise ValueError("No usable chunks after cleaning (all whitespace / control chars)")

    now = datetime.now(timezone.utc).isoformat()
    batch_size = settings.embedding_batch_size
    batches = [chunks[i : i + batch_size] for i in range(0, len(chunks), batch_size)]

    async def _embed_one(batch: list[str]) -> list[tuple[str, list]]:
        """
        Return list of (chunk, embedding) pairs. On batch failure (e.g. Ollama
        500 NaN), fall back to per-chunk embedding and silently drop chunks
        that still error so the file as a whole still succeeds.
        """
        async with _embed_sem:
            try:
                resp = await ollama.embed(model=settings.embedding_model, input=batch)
                return list(zip(batch, resp.embeddings))
            except Exception as e:
                emsg = str(e)
                if not ("NaN" in emsg or "500" in emsg or "unsupported" in emsg.lower()):
                    raise
                logger.warning(
                    "Embed batch failed for %s (%s) — falling back to per-chunk",
                    filename, emsg[:120],
                )
                pairs: list[tuple[str, list]] = []
                for c in batch:
                    try:
                        r = await ollama.embed(model=settings.embedding_model, input=[c])
                        pairs.append((c, r.embeddings[0]))
                    except Exception as e2:
                        logger.warning(
                            "Skipping unembeddable chunk in %s: %s | %r",
                            filename, str(e2)[:80], c[:60].replace("\n", " "),
                        )
                return pairs

    batch_results = await asyncio.gather(*[_embed_one(b) for b in batches])
    all_pairs: list[tuple[str, list]] = [p for batch in batch_results for p in batch]

    if not all_pairs:
        raise ValueError("All chunks failed to embed (likely all NaN)")

    points: list[PointStruct] = [
        PointStruct(
            id=str(uuid.uuid4()),
            vector=vec,
            payload={
                "text": chunk,
                "metadata": {
                    "knowledge_base_id": settings.qdrant_knowledge_base_id,
                    "source_file": filename,
                    "file_id": file_id,
                    "chunk_index": idx,
                    "ingested_at": now,
                },
                "tenant_id": settings.qdrant_knowledge_base_id,
            },
        )
        for idx, (chunk, vec) in enumerate(all_pairs)
    ]

    # ── 3. Upsert to Qdrant + register in OpenWebUI in parallel ───────────
    upsert_size = 256

    async def _one_upsert(slice_):
        async with _upsert_sem:
            await qdrant.upsert(
                collection_name=settings.qdrant_collection,
                points=slice_,
            )

    async def _do_upsert():
        await asyncio.gather(*[
            _one_upsert(points[i : i + upsert_size])
            for i in range(0, len(points), upsert_size)
        ])

    await asyncio.gather(
        _do_upsert(),
        _register_openwebui_file(file_id, filename, file_hash, file_size),
    )

    return len(points)


async def _qdrant_delete(qdrant: AsyncQdrantClient, file_id: str, filename: str):
    """Delete Qdrant vectors and OpenWebUI rows for a given file_id (in parallel)."""
    async def _del_q():
        try:
            await qdrant.delete(
                collection_name=settings.qdrant_collection,
                points_selector=Filter(
                    must=[FieldCondition(key="metadata.file_id", match=MatchValue(value=file_id))]
                ),
            )
        except Exception as e:
            logger.warning("Qdrant delete failed for %s: %s", file_id, e)

    async def _del_o():
        try:
            await _unregister_openwebui_file(file_id)
        except Exception as e:
            logger.warning("OpenWebUI unregister failed for %s: %s", file_id, e)

    await asyncio.gather(_del_q(), _del_o())


async def _ensure_qdrant_collection():
    qdrant = _qdrant_global
    names = [c.name for c in (await qdrant.get_collections()).collections]
    if settings.qdrant_collection not in names:
        await qdrant.create_collection(
            collection_name=settings.qdrant_collection,
            vectors_config=VectorParams(
                size=settings.embedding_dimensions, distance=Distance.COSINE
            ),
        )
        logger.info("Created Qdrant collection: %s", settings.qdrant_collection)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):  # 1 MiB blocks
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

    async with owui_connect() as db:
        await db.execute(
            """INSERT OR REPLACE INTO file
               (id, user_id, filename, meta, created_at, hash, data, updated_at, path)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (file_id, settings.openwebui_user_id, filename, meta, now, file_hash, data, now, ""),
        )
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
    async with owui_connect() as db:
        await db.execute("DELETE FROM knowledge_file WHERE file_id=?", (file_id,))
        await db.execute("DELETE FROM file WHERE id=?", (file_id,))
        await db.commit()
