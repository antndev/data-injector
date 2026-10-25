"""Checks how the writer settles batches, against a fake OpenWebUI.

    python tests/check_writer.py
"""

import asyncio, os, sys

from pathlib import Path

os.environ.setdefault("OPENWEBUI_API_KEY", "x")
os.environ.setdefault("OPENWEBUI_KNOWLEDGE_ID", "kb")
os.environ.setdefault("ADMIN_PASSWORD", "x")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import openwebui

settled = []


async def record(attached, failed):
    settled.append((list(attached), list(failed)))


def fake(refuse=(), leave_out=(), down=False):
    async def add_batch(ids):
        if down:
            raise RuntimeError("connection refused")
        if any(i in refuse for i in ids):
            raise openwebui.Rejected("File not found")
        return {i for i in ids if i not in leave_out}

    return add_batch


async def main():
    openwebui._on_settled = record

    openwebui.add_batch = fake(refuse={"gone"})
    pending = ["a", "gone", "b"]
    await openwebui._flush(pending)
    assert pending == [], pending
    attached = sorted(i for a, _ in settled for i in a)
    failed = [i for _, f in settled for i, _ in f]
    assert attached == ["a", "b"] and failed == ["gone"], settled

    settled.clear()
    openwebui.add_batch = fake(leave_out={"flaky"})
    pending = ["c", "flaky"]
    await openwebui._flush(pending)
    assert pending == ["flaky"] and settled == [(["c"], [])], (pending, settled)
    for _ in range(openwebui.ATTEMPTS - 1):
        await openwebui._flush(pending)
    assert pending == [] and settled[-1][1][0][0] == "flaky", (pending, settled)

    settled.clear()
    openwebui.add_batch = fake(down=True)
    pending = ["d"]
    await openwebui._flush(pending)
    assert pending == ["d"] and settled == [], (pending, settled)

    print("writer: refused id isolated, left-out file given up after", openwebui.ATTEMPTS,
          "tries, outage keeps the ids")


asyncio.run(main())
