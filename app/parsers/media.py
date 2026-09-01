"""Image, audio and video.

The same split as everywhere else: extraction is cheap and deterministic and
only marks what needs a model. The expensive model runs in its own stage, so
a repeated extraction run costs nothing.
"""

from __future__ import annotations

import json

import shutil

import subprocess

from pathlib import Path

from .base import Block, Document, Part, EXTRACTOR_VERSION, source_info

IMAGE_EXT = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff", ".webp"}

AUDIO_EXT = {".m4a", ".mp3", ".wav", ".aiff", ".aac", ".flac", ".ogg", ".opus"}

VIDEO_EXT = {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"}


def _ffprobe(path: Path) -> dict:

    if not shutil.which("ffprobe"):
        return {}
    p = subprocess.run(
        [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(path),
        ],
        capture_output=True,
        timeout=60,
        check=False,
    )
    try:
        return json.loads(p.stdout or b"{}")
    except Exception:
        return {}


def extract_image(path: Path) -> Document:

    from PIL import Image

    doc = Document(
        source=source_info(path), extractor="image", version=EXTRACTOR_VERSION["media"]
    )
    try:
        with Image.open(path) as im:
            b, h = im.size
            mode = im.mode
    except Exception as e:
        doc.status = "errors"
        doc.error = f"{type(e).__name__}: {e}"
        return doc
    doc.blocks.append(
        Block(
            kind="image",
            loc={"width": b, "height": h, "mode": mode, "images": 1, "needs_ocr": True},
            parts=[],
        )
    )
    return doc


def extract_audio(path: Path) -> Document:

    doc = Document(
        source=source_info(path), extractor="audio", version=EXTRACTOR_VERSION["media"]
    )
    info = _ffprobe(path)
    duration = float((info.get("format") or {}).get("duration") or 0)
    tracks = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
    if not tracks:
        doc.status = "errors"
        doc.error = "keine Audiospur found"
        return doc
    doc.blocks.append(
        Block(
            kind="audio",
            loc={
                "duration_s": round(duration, 1),
                "codec": tracks[0].get("codec_name"),
                "needs_asr": True,
            },
            parts=[],
        )
    )
    return doc


def extract_video(path: Path) -> Document:

    doc = Document(
        source=source_info(path), extractor="video", version=EXTRACTOR_VERSION["media"]
    )
    info = _ffprobe(path)
    if not info:
        doc.status = "errors"
        doc.error = "ffprobe lieferte nichts"
        return doc
    duration = float((info.get("format") or {}).get("duration") or 0)
    v = [s for s in info.get("streams", []) if s.get("codec_type") == "video"]
    a = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
    if not v and not a:
        doc.status = "errors"
        doc.error = "weder Bild- noch Tonspur"
        return doc
    if v:
        doc.blocks.append(
            Block(
                kind="video_image",
                loc={
                    "duration_s": round(duration, 1),
                    "width": v[0].get("width"),
                    "height": v[0].get("height"),
                    "codec": v[0].get("codec_name"),
                    "images": 1,
                    "needs_ocr": True,
                },
                parts=[],
            )
        )
    if a:
        doc.blocks.append(
            Block(
                kind="video_audio",
                loc={
                    "duration_s": round(duration, 1),
                    "codec": a[0].get("codec_name"),
                    "needs_asr": True,
                },
                parts=[],
            )
        )
    return doc
