import asyncio
import logging
import secrets
import time
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import TimedRotatingFileHandler
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from starlette.middleware.sessions import SessionMiddleware

from app import events
from app.config import settings
from app.database import init_db
from app.watcher import start_watcher
from app.worker import run_worker, shutdown as worker_shutdown
from app.api.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)


def _session_secret() -> str:
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    secret_file = settings.db_path.parent / ".session_secret"
    if secret_file.exists():
        return secret_file.read_text().strip()
    secret = secrets.token_urlsafe(48)
    secret_file.write_text(secret)
    try:
        secret_file.chmod(0o600)
    except OSError:
        pass
    return secret


def _setup_audit_log() -> logging.Logger:
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    audit = logging.getLogger("audit")
    audit.setLevel(logging.INFO)
    audit.propagate = False
    if audit.handlers:
        return audit
    handler = TimedRotatingFileHandler(
        filename=str(settings.log_dir / "auth.log"),
        when="midnight",
        interval=1,
        backupCount=settings.auth_log_retention_days,
        encoding="utf-8",
        utc=True,
    )
    handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    ))
    audit.addHandler(handler)
    return audit


def _check_log_size() -> None:
    if not settings.log_dir.exists():
        return
    total = sum(f.stat().st_size for f in settings.log_dir.rglob("*") if f.is_file())
    limit = settings.auth_log_max_total_mb * 1024 * 1024
    if total > limit:
        logging.warning(
            "Audit log dir exceeds %.0f MB (%.1f MB used) — consider pruning",
            settings.auth_log_max_total_mb, total / 1024 / 1024,
        )


audit = _setup_audit_log()

log = logging.getLogger("app")


def _check_owui_db() -> None:
    """Loudly flag a misconfigured OpenWebUI DB at startup. If the path is
    wrong/unmounted or the expected tables are missing, every register would
    otherwise fail quietly — now those failures surface (and per-op futures
    propagate the error so affected files are marked 'failed', not 'done')."""
    import sqlite3
    path = settings.openwebui_db_path
    if not path.exists():
        log.error("OpenWebUI DB not found at %s — files will NOT appear in the "
                  "Knowledge Base until OPENWEBUI_DB_PATH / the volume mount is fixed.", path)
        return
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            have = {r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            con.close()
        missing = {"file", "knowledge_file"} - have
        if missing:
            log.error("OpenWebUI DB at %s is missing table(s) %s — registrations "
                      "will fail. Is this the right webui.db?", path, sorted(missing))
    except Exception as e:
        log.error("Could not inspect OpenWebUI DB at %s: %s", path, e)


def _sweep_stale_uploads() -> None:
    """Reap abandoned resumable-upload partials. Keyed on the .part's mtime
    (bumped by every PATCH) so a paused-but-alive upload is never swept — only
    those untouched for longer than UPLOAD_TTL_HOURS. NOTE: unlike the old
    one-shot upload, we must NOT wipe all *.part on boot — that would destroy
    resumability across a restart."""
    d = settings.uploads_tmp_dir
    if not d.exists():
        return
    ttl = settings.upload_ttl_hours * 3600
    now = time.time()
    for sidecar in d.glob("*.json"):
        part = sidecar.with_suffix(".part")
        ref = part if part.exists() else sidecar
        try:
            if now - ref.stat().st_mtime > ttl:
                part.unlink(missing_ok=True)
                sidecar.unlink(missing_ok=True)
                log.info("Swept stale upload partial: %s", sidecar.stem)
        except OSError:
            pass


PUBLIC_PATHS = {"/login", "/logout", "/health"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.create_dirs()
    _check_log_size()
    _check_owui_db()
    _sweep_stale_uploads()
    await init_db()

    loop = asyncio.get_running_loop()
    # Default ThreadPoolExecutor maxes out at ~32 workers — too small when
    # WORKER_CONCURRENCY is high. Bump it so concurrent text extraction,
    # hashing, and legacy conversions don't queue behind each other.
    loop.set_default_executor(ThreadPoolExecutor(
        max_workers=settings.worker_concurrency * 2,
        thread_name_prefix="ingest",
    ))
    events.set_loop(loop)
    events.install_log_handler()

    inbox_queue: asyncio.Queue = asyncio.Queue()
    observer = start_watcher(settings.inbox_dir, inbox_queue, loop)
    worker_task = asyncio.create_task(run_worker(inbox_queue))

    async def _supervise_observer():
        """Restart the watchdog Observer if its thread dies (seen with mounted
        shares / SFTP). Without this, file detection silently degrades to the
        15s periodic rescan with no alert."""
        nonlocal observer
        while True:
            try:
                await asyncio.sleep(20)
                if not observer.is_alive():
                    log.warning("Watcher Observer died — restarting it")
                    observer = start_watcher(settings.inbox_dir, inbox_queue, loop)
                    inbox_queue.put_nowait(None)  # force an immediate rescan
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Observer supervisor tick failed (continuing)")

    supervisor_task = asyncio.create_task(_supervise_observer())

    yield

    # Shutdown order matters:
    # 1. Stop the Observer supervisor and the watcher (no new inbox events).
    # 2. Cancel the worker scan loop so it doesn't enqueue more files.
    # 3. Cancel + await every in-flight per-file processing task.
    # 4. Close the persistent Qdrant client + drain the OWUI writer.
    supervisor_task.cancel()
    try:
        await supervisor_task
    except (asyncio.CancelledError, Exception):
        pass
    observer.stop()
    observer.join()
    worker_task.cancel()
    try:
        await worker_task
    except (asyncio.CancelledError, Exception):
        pass
    await worker_shutdown()


app = FastAPI(title="data-ingestor", lifespan=lifespan)


def _real_ip(request: Request) -> str:
    """X-Forwarded-For aware client IP (Caddy is the only ingress)."""
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip() or "?"
    return request.client.host if request.client else "?"


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if path in PUBLIC_PATHS:
        return await call_next(request)
    if request.session.get("authed"):
        return await call_next(request)
    audit.warning("unauth: %s %s from %s", request.method, path, _real_ip(request))
    if path == "/" or request.headers.get("accept", "").startswith("text/html"):
        return RedirectResponse("/login", status_code=303)
    return JSONResponse({"detail": "auth required"}, status_code=401)


app.add_middleware(
    SessionMiddleware,
    secret_key=_session_secret(),
    session_cookie="ingestor_session",
    same_site="strict",
    https_only=True,   # Caddy handles TLS — never expose this container directly
    max_age=60 * 60 * 8,  # 8 hours
)

app.include_router(router)
