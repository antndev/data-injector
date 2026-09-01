"""Legacy binary formats via headless LibreOffice."""

from __future__ import annotations

import json
import os

import shutil

import subprocess
import sys
import time

import tempfile

from pathlib import Path

from .base import Document, source_info

SOFFICE_CANDIDATES = [
    "/Applications/LibreOffice.app/Contents/MacOS/soffice",
    "/usr/bin/soffice",
    "/usr/bin/libreoffice",
    "/usr/lib/libreoffice/program/soffice",
]

TIMEOUT_S = 180


WRAPPER = (
    Path.home() / "Library" / "Application Support" / "xpro" / "LibreOfficeHeadless.app"
)
WRAPPER_PLIST = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key><string>soffice</string>
  <key>CFBundleIdentifier</key><string>ch.xpro.libreoffice.headless</string>
  <key>CFBundleName</key><string>LibreOfficeHeadless</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
  <key>CFBundleShortVersionString</key><string>1.0</string>
  <key>LSUIElement</key><true/>
  <key>LSBackgroundOnly</key><true/>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
"""


def _system_soffice():
    for candidate in SOFFICE_CANDIDATES:
        if os.path.exists(candidate):
            return candidate
    return shutil.which("soffice") or shutil.which("libreoffice")


def _build_wrapper(real: str) -> str:
    contents = Path(real).resolve().parents[1]
    if not (contents / "Info.plist").exists():
        return real
    target = WRAPPER / "Contents"
    try:
        shutil.rmtree(WRAPPER, ignore_errors=True)
        (target / "MacOS").mkdir(parents=True, exist_ok=True)
        for name in ("Frameworks", "Library", "PlugIns", "Resources"):
            source = contents / name
            if source.exists():
                (target / name).symlink_to(source)
        if (contents / "PkgInfo").exists():
            shutil.copy2(contents / "PkgInfo", target / "PkgInfo")
        shutil.copy2(contents / "MacOS" / "soffice", target / "MacOS" / "soffice")
        for entry in (contents / "MacOS").iterdir():
            if entry.name != "soffice":
                (target / "MacOS" / entry.name).symlink_to(entry)
        (target / "Info.plist").write_text(WRAPPER_PLIST, encoding="utf-8")
        subprocess.run(
            ["codesign", "--force", "--sign", "-", "--timestamp=none", str(WRAPPER)],
            capture_output=True,
            timeout=120,
            check=False,
        )
    except Exception:
        shutil.rmtree(WRAPPER, ignore_errors=True)
        return real
    return str(target / "MacOS" / "soffice")


def soffice_path():
    real = _system_soffice()
    if not real or sys.platform != "darwin":
        return real
    wrapped = WRAPPER / "Contents" / "MacOS" / "soffice"
    if wrapped.exists():
        return str(wrapped)
    return _build_wrapper(real)


def convert(path: Path, target_ext: str, timeout_s: int = TIMEOUT_S) -> Path:
    """Converts to target_ext and returns the path inside the temp folder.

    The caller owns the cleanup of the parent folder."""
    exe = soffice_path()
    if not exe:
        raise RuntimeError("LibreOffice not found")
    out = Path(tempfile.mkdtemp(prefix="lo_out_"))
    profile = Path(tempfile.mkdtemp(prefix="lo_prof_"))
    cmd = [
        exe,
        f"-env:UserInstallation=file://{profile}",
        "--headless",
        "--norestore",
        "--invisible",
        "--convert-to",
        target_ext,
        "--outdir",
        str(out),
        str(path),
    ]
    try:
        subprocess.run(cmd, capture_output=True, timeout=timeout_s, check=False)
    except subprocess.TimeoutExpired:
        shutil.rmtree(profile, ignore_errors=True)
        shutil.rmtree(out, ignore_errors=True)
        raise RuntimeError(f"LibreOffice timed out after {timeout_s}s")
    finally:
        shutil.rmtree(profile, ignore_errors=True)
    hits = list(out.glob(f"*.{target_ext}"))
    if not hits:
        shutil.rmtree(out, ignore_errors=True)
        raise RuntimeError(f"conversion to {target_ext} produced no file")
    return hits[0]


from .paths import QUARANTINE

OLE_STREAMS = {
    ".ppt": ("PowerPoint Document",),
    ".pps": ("PowerPoint Document",),
    ".doc": ("WordDocument",),
    ".dot": ("WordDocument",),
    ".xls": ("Workbook", "Book"),
}


def structurally_sound(path: Path) -> bool:
    wanted = OLE_STREAMS.get(path.suffix.lower())
    if not wanted:
        return True
    try:
        import olefile
    except ImportError:
        return True
    try:
        if not olefile.isOleFile(str(path)):
            return False
        with olefile.OleFileIO(str(path)) as ole:
            names = {"/".join(entry) for entry in ole.listdir()}
    except Exception:
        return False
    return any(w in names for w in wanted)


CHUNK = 25
VERBOSE = True


def _quarantined() -> set:
    try:
        return set(json.loads(QUARANTINE.read_text(encoding="utf-8")))
    except Exception:
        return set()


def _key(path) -> str:
    try:
        return str(Path(path).resolve())
    except Exception:
        return str(path)


def _quarantine(paths) -> None:
    if not paths:
        return
    QUARANTINE.parent.mkdir(parents=True, exist_ok=True)
    current = _quarantined() | {_key(p) for p in paths}
    QUARANTINE.write_text(json.dumps(sorted(current), indent=1), encoding="utf-8")


def _convert_chunk(exe, files, target_ext, target_dir, timeout_s) -> dict:
    profile = Path(tempfile.mkdtemp(prefix="lo_batch_"))
    try:
        subprocess.run(
            [
                exe,
                f"-env:UserInstallation=file://{profile}",
                "--headless",
                "--norestore",
                "--invisible",
                "--nologo",
                "--nolockcheck",
                "--convert-to",
                target_ext,
                "--outdir",
                str(target_dir),
            ]
            + [str(f) for f in files],
            capture_output=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired:
        pass
    finally:
        shutil.rmtree(profile, ignore_errors=True)
    out = {}
    for f in files:
        produced = target_dir / (Path(f).stem + "." + target_ext)
        if produced.exists() and produced.stat().st_size > 0:
            out[str(f)] = produced
    return out


def batch_convert(files, target_ext, target_dir, timeout_s=900) -> dict:
    exe = soffice_path()
    if not exe or not files:
        return {}
    target_dir.mkdir(parents=True, exist_ok=True)
    skip = _quarantined()
    todo, broken = [], []
    for f in files:
        if _key(f) in skip:
            continue
        if not structurally_sound(Path(f)):
            broken.append(f)
            continue
        todo.append(f)
    t0 = time.time()
    out = _convert_chunk(exe, todo, target_ext, target_dir, timeout_s)
    missing = [f for f in todo if str(f) not in out]
    if VERBOSE:
        print(
            f"    full batch {target_ext}: {len(out)}/{len(todo)} "
            f"in {time.time() - t0:.0f}s",
            flush=True,
        )
    if missing:
        if VERBOSE:
            print(f"    falling back to chunks for {len(missing)} files", flush=True)
        for start in range(0, len(missing), CHUNK):
            chunk = missing[start : start + CHUNK]
            done = _convert_chunk(exe, chunk, target_ext, target_dir, timeout_s)
            out.update(done)
            for f in chunk:
                if str(f) in done:
                    continue
                single = _convert_chunk(exe, [f], target_ext, target_dir, timeout_s)
                if single:
                    out.update(single)
                else:
                    broken.append(f)
    _quarantine(broken)
    return out


def from_converted(original: Path, converted: Path, then) -> Document:
    """Extracts from the converted file but records the original as the source."""
    doc = then(converted)
    doc.source = source_info(original)
    doc.extractor = f"legacy-stapel->{converted.suffix.lstrip('.')}"
    return doc


def _via_lo(path: Path, target_ext: str, then) -> Document:

    converted = None
    try:
        converted = convert(path, target_ext)
        doc = then(converted)
    finally:
        if converted is not None:
            shutil.rmtree(converted.parent, ignore_errors=True)
    doc.source = source_info(path)
    doc.extractor = f"legacy-lo->{target_ext}"
    return doc


def extract_doc(path: Path) -> Document:

    from . import office

    return _via_lo(path, "docx", office.extract_docx)


def extract_ppt(path: Path) -> Document:

    from . import office

    return _via_lo(path, "pptx", office.extract_pptx)


def extract_docm(path: Path) -> Document:
    """docm und dotm: python-docx weist den Content-Type zurueck, LibreOffice
    normalisiert sie nach sauberem docx."""
    from . import office

    return _via_lo(path, "docx", office.extract_docx)


def extract_ppsx(path: Path) -> Document:

    from . import office

    return _via_lo(path, "pptx", office.extract_pptx)


def repair_xlsx(path: Path) -> Document:
    """Some xlsx report sheetnames=[] to openpyxl because their workbook
    relationships do not follow the standard. LibreOffice rewrites them."""
    from . import office

    return _via_lo(path, "xlsx", office.extract_xlsx)
