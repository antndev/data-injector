"""Speech recognition through whisper.cpp. Its own stage, like the vision models."""

from __future__ import annotations

import shutil, subprocess, tempfile, time

from pathlib import Path

from .paths import WHISPER_MODEL as MODEL


def available() -> bool:

    return bool(shutil.which("whisper-cli")) and MODEL.exists()


def _to_wav(source: Path) -> Path:
    """whisper.cpp will 16 kHz Mono PCM."""
    target = Path(tempfile.mktemp(suffix=".wav"))
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(source),
            "-ar",
            "16000",
            "-ac",
            "1",
            "-c:a",
            "pcm_s16le",
            str(target),
        ],
        capture_output=True,
        timeout=600,
        check=False,
    )
    return target


def transcribe(source: Path, language: str = "de", timeout: int = 900) -> dict:

    if not available():
        raise RuntimeError("whisper-cli or model missing")
    t0 = time.time()
    wav = _to_wav(source)
    try:
        if not wav.exists() or wav.stat().st_size < 1000:
            raise RuntimeError("Umwandlung nach WAV fehlgeschlagen")
        p = subprocess.run(
            [
                "whisper-cli",
                "-m",
                str(MODEL),
                "-f",
                str(wav),
                "-l",
                language,
                "-nt",
                "--output-txt",
                "-of",
                str(wav),
            ],
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        txt = Path(str(wav) + ".txt")
        if txt.exists():
            text = txt.read_text(encoding="utf-8", errors="replace").strip()
        else:
            text = p.stdout.decode("utf-8", "replace").strip()
        txt.unlink(missing_ok=True)
        return {"text": text, "duration_s": time.time() - t0, "model": MODEL.name}
    finally:
        wav.unlink(missing_ok=True)
