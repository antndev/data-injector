import asyncio
import logging
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

logger = logging.getLogger(__name__)

SUPPORTED = {
    ".pdf", ".pptx", ".pptm", ".ppsx", ".ppt",
    ".docx", ".docm", ".dotm", ".doc",
    ".xlsx", ".xlsm", ".xlsb", ".xls",
    ".csv", ".txt", ".md",
    ".html", ".xml",
    ".msg",
}


class InboxHandler(FileSystemEventHandler):
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self._queue = queue
        self._loop = loop

    def on_closed(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() in SUPPORTED or path.suffix.lower() not in {
            ".strings", ".nib", ".icns", ".plist", ".gitignore",
        }:
            logger.info("Detected: %s", path.name)
            self._loop.call_soon_threadsafe(self._queue.put_nowait, path)

    # fallback for systems without IN_CLOSE_WRITE (e.g. Windows dev)
    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        self._loop.call_soon_threadsafe(self._queue.put_nowait, path)


def start_watcher(inbox: Path, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop) -> Observer:
    handler = InboxHandler(queue, loop)
    observer = Observer()
    observer.schedule(handler, str(inbox), recursive=False)
    observer.start()
    logger.info("Watching %s", inbox)
    return observer
