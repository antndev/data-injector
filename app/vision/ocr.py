"""Tesseract as the cheap baseline.

About a hundred times faster than a vision model. It reads text but does not
understand diagram structure. Good as a prefilter: when Tesseract already
returns enough, the expensive model never has to run.
"""

from __future__ import annotations

import shutil, subprocess, tempfile, time

from pathlib import Path


def available() -> bool:

    return shutil.which("tesseract") is not None


def read(png: bytes, languages: str = "deu+eng", timeout: int = 60) -> dict:

    t0 = time.time()
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(png)
        tmp = f.name
    try:
        p = subprocess.run(
            ["tesseract", tmp, "stdout", "-l", languages],
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        return {
            "text": p.stdout.decode("utf-8", "replace").strip(),
            "duration_s": time.time() - t0,
            "model": f"tesseract:{languages}",
        }
    finally:
        Path(tmp).unlink(missing_ok=True)
