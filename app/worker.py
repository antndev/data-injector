import asyncio
import hashlib
import logging
import math
import re
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from app import openwebui
from app.config import settings
from app.database import connect
from app.watcher import SUPPORTED

IMAGE_SUFFIXES = {
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp",
}

logger = logging.getLogger(__name__)

_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_MIN_CHUNK_CHARS = 3


def _clean_chunk(s: str) -> str:
    return _CONTROL_CHARS.sub("", s).strip()


active_paths: set[str] = set()

_running_tasks: set[asyncio.Task] = set()

_register_lock = asyncio.Lock()
_dedup_lock = asyncio.Lock()
_inbox_queue: asyncio.Queue | None = None


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
    await openwebui.stop_writer()


async def recover_crashed():
    async with connect() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, filename FROM files WHERE status = 'processing'") as cur:
            rows = await cur.fetchall()
    if not rows:
        return

    logger.warning("Recovering %d crashed file(s)", len(rows))
    for row in rows:
        async with connect() as db:
            await db.execute(
                "UPDATE files SET status='queued', error_message=NULL WHERE id=?",
                (row["id"],),
            )
            await db.commit()


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


async def run_worker(inbox_queue: asyncio.Queue):
    global _inbox_queue
    _inbox_queue = inbox_queue

    openwebui.start_writer()

    try:
        await recover_crashed()
    except Exception:
        logger.exception("Startup recovery failed — continuing")
    try:
        await _check_openwebui()
    except Exception:
        logger.exception("OpenWebUI probe failed, continuing")

    sem = asyncio.Semaphore(settings.worker_concurrency)

    try:
        async with connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT id, filename FROM files WHERE status='queued'") as cur:
                queued = await cur.fetchall()
        for row in queued:
            proc = settings.processing_dir / row["filename"]
            if proc.exists() and str(proc) not in active_paths:
                active_paths.add(str(proc))
                _spawn(_process_with_sem(sem, proc, row["id"]))
    except Exception:
        logger.exception("Resuming queued files failed — continuing")

    _spawn(_periodic_rescan())

    inbox_queue.put_nowait(None)

    while True:
        await inbox_queue.get()
        while not inbox_queue.empty():
            inbox_queue.get_nowait()

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


async def _process_file(path: Path, file_id: str):
    original_path = path
    ext = path.suffix.lower()
    proc_path: Path | None = None

    try:
        if not path.exists():
            return

        async with connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT id FROM files WHERE id=?", (file_id,)) as cur:
                if not await cur.fetchone():
                    return

        loop = asyncio.get_running_loop()

        if ext not in SUPPORTED:
            _dispose(path, settings.unsupported_dir)
            await _set_status(file_id, "unsupported")
            logger.info("Unsupported: %s", path.name)
            return

        if path.parent == settings.inbox_dir:
            file_hash = await loop.run_in_executor(None, _sha256, path)
            async with _dedup_lock:
                async with connect() as db:
                    db.row_factory = aiosqlite.Row
                    async with db.execute(
                        "SELECT id FROM files WHERE file_hash=? AND status NOT IN "
                        "('deleted','failed','duplicate','unsupported') AND id<>?",
                        (file_hash, file_id),
                    ) as cur:
                        existing = await cur.fetchone()
                if existing:
                    _dispose(path, settings.duplicates_dir)
                    await _set_status(file_id, "duplicate")
                    logger.info("Duplicate: %s", path.name)
                    return

                async with connect() as db:
                    await db.execute(
                        "UPDATE files SET file_hash=?, status='processing' WHERE id=?",
                        (file_hash, file_id),
                    )
                    await db.commit()

                proc_path = _move(path, settings.processing_dir)
                if proc_path.name != path.name:
                    async with connect() as db:
                        await db.execute(
                            "UPDATE files SET filename=? WHERE id=?",
                            (proc_path.name, file_id),
                        )
                        await db.commit()
                path = proc_path
        else:
            proc_path = path
            file_hash = await loop.run_in_executor(None, _sha256, path)

        async with connect() as db:
            await db.execute(
                "UPDATE files SET status='processing', file_hash=?, file_size_bytes=? WHERE id=?",
                (file_hash, path.stat().st_size, file_id),
            )
            await db.commit()
        logger.info("Processing: %s", path.name)

        text = ""
        try:
            text = await loop.run_in_executor(None, _extract_markdown, path)
        except Exception as e:
            logger.warning("Extraction failed for %s: %s", path.name, str(e)[:160])

        if not text.strip():
            raise ValueError("No text extracted")

        file_id_remote = await _upload_markdown(proc_path.name, text)
        stored = 1

        async with connect() as db:
            await db.execute(
                "UPDATE files SET status='done', qdrant_collection=?, qdrant_chunk_count=?, ingested_at=? WHERE id=?",
                ("openwebui", stored, datetime.now(timezone.utc).isoformat(), file_id),
            )
            await db.commit()

        if settings.delete_after_ingest:
            try:
                proc_path.unlink(missing_ok=True)
            except OSError as e:
                logger.warning("Could not delete %s after ingest: %s", proc_path.name, e)
        elif proc_path.exists():
            _move(proc_path, settings.done_dir)
        logger.info("Done: %s (%d chunks)", proc_path.name, stored)

    except Exception as exc:
        async with connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT id FROM files WHERE id=?", (file_id,)) as cur:
                still_exists = await cur.fetchone()

        if not still_exists:
            logger.debug("Processing cancelled for %s: deleted while in progress", original_path.name)
            if proc_path is not None and proc_path.exists():
                try:
                    proc_path.unlink()
                except OSError:
                    pass
        else:
            logger.exception("Failed: %s — %s", original_path.name, exc)
            try:
                if locals().get("file_id_remote"):
                    await openwebui.remove(file_id_remote)
            except Exception as ce:
                logger.warning("Post-failure cleanup for %s: %s", file_id, ce)
            failed = None
            if proc_path is not None and proc_path.exists():
                failed = _move(proc_path, settings.failed_dir)
                (settings.failed_dir / (failed.name + ".error")).write_text(str(exc), encoding="utf-8")
            async with connect() as db:
                if failed is not None:
                    await db.execute(
                        "UPDATE files SET status='failed', filename=?, error_message=? WHERE id=?",
                        (failed.name, str(exc)[:4000], file_id),
                    )
                else:
                    await db.execute(
                        "UPDATE files SET status='failed', error_message=? WHERE id=?",
                        (str(exc)[:4000], file_id),
                    )
                await db.commit()
    finally:
        active_paths.discard(str(original_path))


async def _delayed_nudge(seconds: int):
    await asyncio.sleep(seconds)
    if _inbox_queue is not None:
        _inbox_queue.put_nowait(None)


async def _periodic_rescan(every_s: int = 15):
    """Belt-and-braces: rescan the inbox at a slow tick so files aren't
    stranded if the watchdog Observer thread silently dies (rare but it
    has happened in production with mounted shares / SFTP). The main.py
    lifespan also supervises and restarts a dead Observer."""
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


def _extract_markdown(path: Path) -> str:
    """Runs the structured extractor and renders markdown.

    Markdown is what goes to OpenWebUI, and its headings sit exactly on the
    block boundaries, so OpenWebUI's markdown splitter reproduces the structure
    the extractor found instead of cutting blindly at a character count."""
    from app.markdown import to_markdown
    from app.parsers import base

    kind, extractor = base.route(path)
    if extractor is None:
        raise ValueError(f"no parser for: {path.suffix.lower()} ({kind})")
    doc = extractor(path)
    _apply_models(doc, path)
    return to_markdown(doc, path)


def _apply_models(doc, path: Path) -> None:
    """Fills blocks that carry no text of their own.

    The gate is what keeps this affordable: over the reference corpus only
    14.5 percent of slides qualify, so this runs on 2590 of 17833 instead of
    every image."""
    from app.parsers import asr, video
    from app.parsers.base import Part
    from app.vision import gate

    for block in doc.blocks:
        if settings.vision_enabled and gate.needs_vision(block):
            try:
                text = _describe_image(doc, block, path)
                if text.strip():
                    block.parts.append(Part("vlm", text.strip()))
            except Exception as exc:
                logger.warning("vision failed on %s: %s", path.name, str(exc)[:150])
        if settings.asr_enabled and block.loc.get("needs_asr"):
            try:
                media = _media_for(block, path)
                result = asr.transcribe(media[0], language=settings.asr_language)
                if media[1]:
                    shutil.rmtree(media[0].parent, ignore_errors=True)
                if result["text"].strip():
                    block.parts.append(Part("asr", result["text"].strip()))
            except Exception as exc:
                logger.warning("speech failed on %s: %s", path.name, str(exc)[:150])


def _media_for(block, path: Path):
    """Returns (file, is_temporary) for a block that carries audio."""
    if block.kind == "embedded_media":
        return _extract_member(path, block.loc["member"]), True
    return path, False


def _extract_member(source: Path, member: str) -> Path:
    import zipfile

    target = Path(tempfile.mkdtemp(prefix="embedded_")) / Path(member).name
    with zipfile.ZipFile(source) as archive, open(target, "wb") as out:
        shutil.copyfileobj(archive.open(member), out)
    return target


def _describe_image(doc, block, path: Path) -> str:
    from app.parsers import video
    from app.vision import ocr, render, vlm

    if block.kind in ("video_image", "embedded_media"):
        media, temporary = _media_for(block, path)
        try:
            frames = video.sample_frames(media)
            described = [
                (second, _read_image(png))
                for second, png in frames
            ]
            return video.timeline([(s, t) for s, t in described if t.strip()])
        finally:
            if temporary:
                shutil.rmtree(media.parent, ignore_errors=True)

    if path.suffix.lower() in IMAGE_SUFFIXES:
        return _read_image(path.read_bytes())

    page = block.loc.get("slide") or block.loc.get("page") or 1
    if path.suffix.lower() == ".pdf":
        data = render.rasterize(path, [page]).get(page, b"")
    else:
        data = render.pages_as_png(path, [page]).get(page, b"")
    return _read_image(data) if data else ""


def _read_image(data: bytes) -> str:
    from app.vision import ocr, vlm

    if not data:
        return ""
    if settings.vision_model == "tesseract":
        return ocr.read(data)["text"]
    return vlm.describe(data, settings.vision_model, host=settings.ollama_host)["text"]


async def _upload_markdown(filename: str, markdown: str) -> str:
    """Sends the extracted text and queues it for indexing."""
    name = filename[:120] + ".md"
    file_id = await openwebui.upload(markdown, name)
    await openwebui.register(file_id)
    return file_id


async def _check_openwebui() -> None:
    if not await openwebui.reachable():
        logger.error(
            "OpenWebUI not reachable at %s or API key rejected. Files will fail.",
            settings.openwebui_url,
        )


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _dispose(path: Path, fallback_dir: Path) -> None:
    """For a resolved-but-not-ingested file (duplicate / unsupported): delete it
    when delete_after_ingest is on, so nothing customer-supplied is retained on
    disk — the DB row still records what happened. Otherwise move it to its
    category dir for inspection."""
    if settings.delete_after_ingest:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
    else:
        _move(path, fallback_dir)


def _move(src: Path, dest_dir: Path) -> Path:
    """Move src into dest_dir, renaming on collision. Returns final path.
    Creates dest_dir on demand so category dirs (done/failed/duplicates/…) are
    made lazily — under delete_after_ingest they otherwise never exist."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    counter = 1
    while dest.exists():
        dest = dest_dir / f"{src.stem}_{counter}{src.suffix}"
        counter += 1
    shutil.move(str(src), dest)
    return dest


