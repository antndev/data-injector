import asyncio
import logging
import secrets

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from contextlib import asynccontextmanager
from starlette.middleware.sessions import SessionMiddleware

from app import events
from app.config import settings
from app.database import init_db
from app.watcher import start_watcher
from app.worker import run_worker
from app.api.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)


def _session_secret() -> str:
    """Load a persisted session secret, or generate one and store it.
    Lives next to the DB so logins survive restarts without needing env config."""
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.create_dirs()
    await init_db()

    loop = asyncio.get_running_loop()
    events.set_loop(loop)
    events.install_log_handler()

    inbox_queue: asyncio.Queue = asyncio.Queue()
    observer = start_watcher(settings.inbox_dir, inbox_queue, loop)
    worker_task = asyncio.create_task(run_worker(inbox_queue))

    yield

    observer.stop()
    observer.join()
    worker_task.cancel()


app = FastAPI(title="data-ingestor", lifespan=lifespan)

PUBLIC_PATHS = {"/login", "/logout", "/health"}


# Registered FIRST → runs INSIDE SessionMiddleware (so request.session is populated).
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    if path in PUBLIC_PATHS:
        return await call_next(request)
    if request.session.get("authed"):
        return await call_next(request)
    if path == "/" or request.headers.get("accept", "").startswith("text/html"):
        return RedirectResponse("/login", status_code=303)
    return JSONResponse({"detail": "auth required"}, status_code=401)


# Registered LAST → wraps everything → runs first on the request, populates session.
app.add_middleware(
    SessionMiddleware,
    secret_key=_session_secret(),
    session_cookie="ingestor_session",
    same_site="lax",
    https_only=False,
    max_age=60 * 60 * 24 * 7,  # 1 week
)

app.include_router(router)
