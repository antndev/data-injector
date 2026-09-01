"""Writable locations inside the container, overridable by environment."""

from __future__ import annotations

import os
from pathlib import Path

WORK = Path(os.environ.get("EXTRACT_WORK_DIR", "/db/extract"))
MODELS = Path(os.environ.get("WHISPER_MODEL_DIR", "/models"))
PDF_CACHE = WORK / "pdf"
CONVERTED = WORK / "converted"
QUARANTINE = WORK / "lo_quarantine.json"
WHISPER_MODEL = MODELS / os.environ.get("WHISPER_MODEL", "ggml-small.bin")

for _d in (WORK, PDF_CACHE, CONVERTED):
    _d.mkdir(parents=True, exist_ok=True)
