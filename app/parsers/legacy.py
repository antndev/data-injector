import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


def convert(path: Path, target_ext: str) -> tuple[Path, Path]:
    """Convert legacy .doc/.ppt to modern format via LibreOffice.
    Returns (converted_path, tmpdir) — caller must delete tmpdir when done.

    LibreOffice often emits non-fatal warnings on stderr ("failed to launch
    javaldx", missing fonts, etc.) and exits non-zero even when the output
    file was produced fine. Treat the existence of the output file as the
    real success signal.
    """
    tmpdir = Path(tempfile.mkdtemp())
    result = subprocess.run(
        ["libreoffice", "--headless", "--convert-to", target_ext,
         str(path), "--outdir", str(tmpdir)],
        capture_output=True,
        timeout=120,
    )

    converted = tmpdir / (path.stem + "." + target_ext)
    if converted.exists() and converted.stat().st_size > 0:
        return converted, tmpdir

    # Real failure — surface the most useful diagnostic
    shutil.rmtree(tmpdir, ignore_errors=True)
    err = (result.stderr.decode(errors="replace") or
           result.stdout.decode(errors="replace") or "unknown error").strip()
    raise RuntimeError(
        f"LibreOffice produced no output (exit {result.returncode}): {err[:500]}"
    )


def convert_to_text(path: Path) -> str:
    """
    Extract text from any office document by asking LibreOffice to convert
    it to a plain .txt file.  Catches text that python-docx / python-pptx
    miss — text boxes, shapes, headers, footers, slide masters — and works
    as a last-resort path when the structured conversion has failed.
    """
    tmpdir = Path(tempfile.mkdtemp())
    try:
        result = subprocess.run(
            [
                "libreoffice", "--headless",
                "--convert-to", "txt:Text (encoded):UTF8",
                str(path), "--outdir", str(tmpdir),
            ],
            capture_output=True,
            timeout=180,
        )
        for txt in tmpdir.glob("*.txt"):
            if txt.stat().st_size > 0:
                return txt.read_text(encoding="utf-8", errors="replace")
        err = (result.stderr.decode(errors="replace")
               or result.stdout.decode(errors="replace")
               or "no output").strip()
        raise RuntimeError(
            f"LibreOffice text extraction failed (exit {result.returncode}): "
            f"{err[:300]}"
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
