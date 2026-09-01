"""Slides and pages as images, through LibreOffice to PDF and PyMuPDF to raster.

The PDF stage is produced once per file and cached because it is the expensive
part. Rastering single pages after that costs almost nothing.
"""

from __future__ import annotations

import hashlib, shutil, subprocess, tempfile

from pathlib import Path

SOFFICE = [
    "/Applications/LibreOffice.app/Contents/MacOS/soffice",
    "/usr/bin/soffice",
    "/usr/bin/libreoffice",
]

from ..parsers.paths import PDF_CACHE

TIMEOUT_S = 300


def _soffice() -> str:
    from ..parsers import legacy

    path = legacy.soffice_path()
    if not path:
        raise RuntimeError("LibreOffice nicht found")
    return path


def to_pdf(source: Path, cache: bool = True) -> Path:
    """Konvertiert nach PDF und gibt den Pfad im Cache zurueck."""
    PDF_CACHE.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(
        f"{source.resolve()}|{source.stat().st_mtime_ns}|{source.stat().st_size}".encode()
    ).hexdigest()[:32]
    target = PDF_CACHE / f"{key}.pdf"
    if cache and target.exists():
        return target
    out = Path(tempfile.mkdtemp(prefix="rend_out_"))
    prof = Path(tempfile.mkdtemp(prefix="rend_prof_"))
    try:
        subprocess.run(
            [
                _soffice(),
                f"-env:UserInstallation=file://{prof}",
                "--headless",
                "--norestore",
                "--invisible",
                "--convert-to",
                "pdf",
                "--outdir",
                str(out),
                str(source),
            ],
            capture_output=True,
            timeout=TIMEOUT_S,
            check=False,
        )
        hits = list(out.glob("*.pdf"))
        if not hits:
            raise RuntimeError(f"PDF-Konvertierung lieferte nichts fuer {source.name}")
        shutil.move(str(hits[0]), target)
        return target
    finally:
        shutil.rmtree(prof, ignore_errors=True)
        shutil.rmtree(out, ignore_errors=True)


def rasterize(pdf: Path, pages: list, dpi: int = 150) -> dict:
    """Pages are 1 based like the slide numbers. Returns {number: png_bytes}."""
    import fitz

    out = {}
    with fitz.open(str(pdf)) as d:
        zoom = dpi / 72.0
        m = fitz.Matrix(zoom, zoom)
        for nr in pages:
            if not (1 <= nr <= len(d)):
                continue
            out[nr] = d[nr - 1].get_pixmap(matrix=m).tobytes("png")
    return out


def pages_as_png(source: Path, pages: list, dpi: int = 150) -> dict:

    return rasterize(to_pdf(source), pages, dpi)
