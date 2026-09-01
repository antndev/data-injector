"""Samples video frames once per second and keeps only the ones that changed."""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

FPS = 1
THUMB = 24
CHANGE_THRESHOLD = 8.0
MAX_KEPT = 12


def _thumbnail(path: Path):
    from PIL import Image

    with Image.open(path) as im:
        return list(im.convert("L").resize((THUMB, THUMB)).getdata())


def _difference(a, b) -> float:
    if not a or not b or len(a) != len(b):
        return 255.0
    return sum(abs(x - y) for x, y in zip(a, b)) / len(a)


def _spread(items: list, limit: int) -> list:
    """Picks at most limit items spread evenly across the whole list.

    Taking the first N would only cover the beginning of the video."""
    if len(items) <= limit:
        return items
    step = (len(items) - 1) / (limit - 1)
    return [items[round(i * step)] for i in range(limit)]


def sample_frames(
    video: Path,
    fps: int = FPS,
    threshold: float = CHANGE_THRESHOLD,
    max_kept: int = MAX_KEPT,
) -> list:
    """Returns [(second, png_bytes)] for the visually distinct frames."""
    work = Path(tempfile.mkdtemp(prefix="frames_"))
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(video),
                "-vf",
                f"fps={fps}",
                str(work / "f%05d.png"),
            ],
            capture_output=True,
            timeout=1800,
            check=False,
        )
        frames = sorted(work.glob("f*.png"))
        distinct, previous = [], None
        for index, frame in enumerate(frames):
            try:
                thumb = _thumbnail(frame)
            except Exception:
                continue
            if previous is not None and _difference(thumb, previous) < threshold:
                continue
            previous = thumb
            distinct.append((int(index / fps), frame))
        chosen = _spread(distinct, max_kept)
        return [(second, frame.read_bytes()) for second, frame in chosen]
    finally:
        import shutil

        shutil.rmtree(work, ignore_errors=True)


def timeline(descriptions: list) -> str:
    """Builds one readable timeline from [(second, text)]."""
    lines = []
    for second, text in descriptions:
        stamp = f"{second // 60:02d}:{second % 60:02d}"
        body = " ".join(text.split())
        if body:
            lines.append(f"[{stamp}] {body}")
    return "\n\n".join(lines)
