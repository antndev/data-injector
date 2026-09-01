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
    ".html", ".htm", ".xml",
    ".msg", ".eml",
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp",
    ".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm",
    ".m4a", ".mp3", ".wav", ".aac", ".flac", ".ogg", ".opus", ".aiff",
}
IGNORED = {".strings", ".nib", ".icns", ".plist", ".gitignore", ""}


class InboxHandler(FileSystemEventHandler):
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self._queue = queue
        self._loop = loop

    def _wake(self, event):
        if event.is_directory:
            return
        path = getattr(event, "dest_path", None) or event.src_path
        ext = Path(path).suffix.lower()
        if ext in IGNORED:
            return
        self._loop.call_soon_threadsafe(self._queue.put_nowait, None)

    on_closed = _wake
    on_created = _wake
    on_moved = _wake


def start_watcher(inbox: Path, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop) -> Observer:
    observer = Observer()
    observer.schedule(InboxHandler(queue, loop), str(inbox), recursive=False)
    observer.start()
    logger.info("Watching %s", inbox)
    return observer
