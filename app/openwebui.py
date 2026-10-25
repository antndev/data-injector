"""Uploads extracted text to OpenWebUI through its public API.

The previous version wrote Qdrant points and rows in webui.db directly. That
coupled the injector to OpenWebUI internals that nobody guarantees, and it broke
silently: the injector wrote 1024 dimensional vectors while OpenWebUI queried
with a 384 dimensional model, so the two never saw each other.

Going through the API costs about three times the wall clock on a full load
(measured: 29 minutes against 10 for 2018 documents) and removes that whole
class of failure. Adding files in batches instead of one by one is worth 2.3x,
so the flush below is not an optimisation but the difference between the two
numbers above.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Iterable

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

_queue: asyncio.Queue | None = None
_task: asyncio.Task | None = None
_on_settled = None
_misses: dict[str, int] = {}


class Rejected(RuntimeError):
    """OpenWebUI refused the request itself, so sending it again unchanged cannot help."""


def _headers() -> dict:
    return {"Authorization": f"Bearer {settings.openwebui_api_key}"}


def _url(path: str) -> str:
    return settings.openwebui_url.rstrip("/") + path


ATTEMPTS = 5
BACKOFF = (5, 15, 30, 60)


async def upload(text: str, filename: str, timeout: float = 300.0) -> str:
    """Sends one markdown document and returns its file id.

    OpenWebUI embeds the document inside this request, so under load it can sit
    well past a minute before answering. A full run lost nine documents to a
    read timeout on this call, after the extraction work was already done, so a
    transient slow answer must not cost the file.

    The waits stretch to almost two minutes in total on purpose. Three tries
    over three seconds looked like enough until OpenWebUI was restarted mid
    run: it needs about thirty seconds to come back, and every file in flight
    was lost with ConnectError while the retries had long given up."""
    files = {"file": (filename, text.encode("utf-8"), "text/markdown")}
    last = None
    for attempt in range(ATTEMPTS):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(
                    _url("/api/v1/files/"), headers=_headers(), files=files
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last = exc
            if attempt + 1 < ATTEMPTS:
                await asyncio.sleep(BACKOFF[attempt])
                continue
            raise RuntimeError(f"upload gave up after {ATTEMPTS} tries: {type(exc).__name__}")
        if response.status_code >= 400:
            raise RuntimeError(f"upload failed {response.status_code}: {response.text[:200]}")
        file_id = response.json().get("id")
        if not file_id:
            raise RuntimeError(f"upload returned no id: {response.text[:200]}")
        return file_id
    raise RuntimeError(f"upload gave up: {last}")


async def add_batch(file_ids: Iterable[str], timeout: float = 900.0) -> set:
    """Attaches uploaded files to the knowledge base and returns the ids that made it.

    This is the step that makes an uploaded document findable. A file that is
    uploaded but never attached sits in OpenWebUI as dead weight: it consumes
    storage and it shows up in no knowledge base.

    A 200 does not mean every file was attached. OpenWebUI leaves out each file
    whose embedding failed and mentions it only under warnings, so the ids that
    count are the ones the knowledge base lists in the answer."""
    ids = [i for i in file_ids if i]
    if not ids:
        return set()
    body = [{"file_id": i} for i in ids]
    path = f"/api/v1/knowledge/{settings.openwebui_knowledge_id}/files/batch/add"
    for attempt in range(ATTEMPTS):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(_url(path), headers=_headers(), json=body)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if attempt + 1 < ATTEMPTS:
                await asyncio.sleep(BACKOFF[attempt])
                continue
            raise RuntimeError(
                f"batch add gave up after {ATTEMPTS} tries: {type(exc).__name__}")
        if response.status_code == 400:
            raise Rejected(f"batch add rejected: {response.text[:200]}")
        if response.status_code >= 400:
            raise RuntimeError(f"batch add failed {response.status_code}: {response.text[:200]}")
        listed = {f.get("id") for f in response.json().get("files") or []}
        return {i for i in ids if i in listed}
    return set()


async def knowledge_file_ids(timeout: float = 120.0) -> set:
    """Every file id the knowledge base holds, read page by page."""
    path = f"/api/v1/knowledge/{settings.openwebui_knowledge_id}/files"
    ids: set = set()
    page = 1
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            response = await client.get(
                _url(path), headers=_headers(), params={"page": page, "limit": 500}
            )
            response.raise_for_status()
            data = response.json()
            items = data.get("items") or []
            ids |= {item["id"] for item in items}
            if not items or len(ids) >= data.get("total", 0):
                return ids
            page += 1


async def remove(file_id: str, timeout: float = 120.0) -> None:
    """Detaches a file from the knowledge base and deletes it.

    Detaching alone leaves the file and its vectors behind, so a deleted
    document kept answering questions."""
    path = f"/api/v1/knowledge/{settings.openwebui_knowledge_id}/file/remove"
    async with httpx.AsyncClient(timeout=timeout) as client:
        await client.post(_url(path), headers=_headers(), json={"file_id": file_id})
        response = await client.delete(_url(f"/api/v1/files/{file_id}"), headers=_headers())
    if response.status_code >= 400 and response.status_code != 404:
        raise RuntimeError(f"delete failed {response.status_code}: {response.text[:200]}")


async def reachable(timeout: float = 15.0) -> bool:
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(_url("/api/v1/auths/"), headers=_headers())
        return response.status_code == 200
    except Exception as exc:
        logger.warning("OpenWebUI not reachable: %s", exc)
        return False


async def _flush(pending: list) -> None:
    """Attaches the collected ids, keeps the ones that did not make it, and
    settles the rest.

    The list used to be cleared whether the call succeeded or not. One failed
    batch therefore lost the ids for good: seventeen documents were uploaded,
    five were attached, and twelve became unreachable while every one of them
    was recorded as done. Keeping the ids means the next flush tries again.

    Keeping them has a trap of its own. OpenWebUI refuses the whole batch with
    a 400 as soon as one id is unknown to it, so that id would take every later
    batch down with it. A refused batch is tried one id at a time, and only the
    refused id is given up. A file left out because its embedding failed gets
    ATTEMPTS flushes before it is given up as well."""
    if not pending:
        return
    try:
        attached = await add_batch(pending)
    except Rejected as exc:
        if len(pending) == 1:
            await _settle([], [(pending.pop(), str(exc))])
            return
        for file_id in list(pending):
            one = [file_id]
            await _flush(one)
            if not one:
                pending.remove(file_id)
        return
    except Exception as exc:
        logger.error("batch add failed for %d files, they stay queued: %s",
                     len(pending), exc)
        return
    given_up = []
    for file_id in pending:
        if file_id in attached:
            continue
        _misses[file_id] = _misses.get(file_id, 0) + 1
        if _misses[file_id] >= ATTEMPTS:
            given_up.append((file_id, f"not attached by OpenWebUI after {ATTEMPTS} tries"))
    settled = attached | {file_id for file_id, _ in given_up}
    pending[:] = [i for i in pending if i not in settled]
    for file_id in settled:
        _misses.pop(file_id, None)
    if attached:
        logger.info("indexed %d files in OpenWebUI", len(attached))
    await _settle(sorted(attached), given_up)


async def _settle(attached: list, failed: list) -> None:
    if not _on_settled or not (attached or failed):
        return
    try:
        await _on_settled(attached, failed)
    except Exception:
        logger.exception("recording attached files failed, a restart attaches them again")


async def _writer_loop() -> None:
    """Collects file ids and flushes them together.

    A batch leaves as soon as it is full or the queue goes quiet, so a single
    dropped file is not stuck waiting for a batch that never fills."""
    pending: list = []
    while True:
        try:
            timeout = settings.openwebui_batch_seconds if pending else None
            file_id = await asyncio.wait_for(_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            await _flush(pending)
            continue
        except asyncio.CancelledError:
            await _flush(pending)
            raise
        if file_id is None:
            await _flush(pending)
            continue
        pending.append(file_id)
        if len(pending) >= settings.openwebui_batch_size:
            await _flush(pending)


def start_writer(on_settled=None) -> None:
    """Starts the writer. on_settled(attached, failed) learns which files made it."""
    global _queue, _task, _on_settled
    _on_settled = on_settled
    if _task and not _task.done():
        return
    _queue = asyncio.Queue()
    _task = asyncio.create_task(_writer_loop())


async def stop_writer() -> None:
    global _task
    if not _task:
        return
    if _queue is not None:
        await _queue.put(None)
        await asyncio.sleep(0)
    _task.cancel()
    try:
        await _task
    except asyncio.CancelledError:
        pass
    _task = None


async def register(file_id: str) -> None:
    """Queues an uploaded file for indexing."""
    if _queue is None:
        raise RuntimeError("writer not started")
    await _queue.put(file_id)
