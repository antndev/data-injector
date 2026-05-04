import shutil
import subprocess
import tempfile
from pathlib import Path


def convert(path: Path, target_ext: str) -> tuple[Path, Path]:
    """Convert legacy .doc/.ppt to modern format via LibreOffice.
    Returns (converted_path, tmpdir) — caller must delete tmpdir when done."""
    tmpdir = Path(tempfile.mkdtemp())
    result = subprocess.run(
        ["libreoffice", "--headless", "--convert-to", target_ext,
         str(path), "--outdir", str(tmpdir)],
        capture_output=True,
        timeout=120,
    )
    if result.returncode != 0:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise RuntimeError(f"LibreOffice failed: {result.stderr.decode()}")

    converted = tmpdir / (path.stem + "." + target_ext)
    if not converted.exists():
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise RuntimeError("LibreOffice produced no output file")

    return converted, tmpdir
