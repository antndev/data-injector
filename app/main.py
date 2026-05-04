import asyncio
import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.config import settings
from app.database import init_db
from app.watcher import start_watcher
from app.worker import run_worker
from app.api.routes import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.create_dirs()
    await init_db()

    inbox_queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    observer = start_watcher(settings.inbox_dir, inbox_queue, loop)
    worker_task = asyncio.create_task(run_worker(inbox_queue))

    yield

    observer.stop()
    observer.join()
    worker_task.cancel()


app = FastAPI(title="data-ingestor", lifespan=lifespan)
app.include_router(router)
