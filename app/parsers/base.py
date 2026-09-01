"""Datenmodell und Routing. Spaeter app/parsers/base.py im Injector."""

from __future__ import annotations

import hashlib

import unicodedata

from dataclasses import dataclass, field, asdict

from pathlib import Path

from typing import Callable, Optional

EXTRACTOR_VERSION = {
    "pptx": 1,
    "docx": 1,
    "xlsx": 2,
    "xls": 2,
    "xlsb": 1,
    "pdf": 1,
    "msg": 1,
    "text": 1,
    "legacy": 1,
    "media": 1,
}

PART_KINDS = ("title", "body", "table", "chart", "notes", "cell", "vlm")

FAMILY = {
    ".docx": "word",
    ".docm": "word",
    ".dotm": "word",
    ".doc": "word",
    ".pptx": "ppt",
    ".pptm": "ppt",
    ".ppsx": "ppt",
    ".ppt": "ppt",
    ".xlsx": "xl",
    ".xlsm": "xl",
    ".xlsb": "xl",
    ".xls": "xl",
}

OOXML_CHECK = {".docx", ".pptx", ".xlsx", ".docm", ".pptm", ".xlsm"}

UNSUPPORTED_EXT = {
    ".nib",
    ".strings",
    ".icns",
    ".js",
    ".plist",
    ".css",
    ".zip",
    ".exe",
    ".dll",
    ".dylib",
    ".so",
    ".ds_store",
    ".lproj",
}


@dataclass
class Part:

    kind: str
    text: str


@dataclass
class Block:

    kind: str
    loc: dict
    parts: list = field(default_factory=list)

    def text(self, include=None) -> str:
        inc = include or set(PART_KINDS)
        return "\n".join(p.text for p in self.parts if p.kind in inc and p.text.strip())

    @property
    def chars(self) -> int:
        return len(self.text())


@dataclass
class SourceInfo:

    hash: str
    filename: str
    ext: str
    size_bytes: int


@dataclass
class Document:

    source: SourceInfo
    blocks: list = field(default_factory=list)
    extractor: str = ""
    version: int = 0
    status: str = "ok"
    error: Optional[str] = None

    @property
    def chars(self) -> int:
        return sum(b.chars for b in self.blocks)

    def chars_by_kind(self) -> dict:
        out = {}
        for b in self.blocks:
            for p in b.parts:
                out[p.kind] = out.get(p.kind, 0) + len(p.text)
        return out

    def to_dict(self) -> dict:
        return asdict(self)


def file_hash(path: Path, chunk=1 << 20) -> str:

    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def source_info(path: Path) -> SourceInfo:

    return SourceInfo(
        hash=file_hash(path),
        filename=unicodedata.normalize("NFC", path.name),
        ext=path.suffix.lower(),
        size_bytes=path.stat().st_size,
    )


def sniff(path: Path) -> Optional[str]:
    """Determines the real type from the content instead of the extension.

    Files carrying the wrong extension are common in practice, for instance
    when someone saves a pptx as docx. Without this check such a file fails as
    an error although it would read perfectly well.
    """
    import zipfile

    try:
        with open(path, "rb") as f:
            header = f.read(8)
    except Exception:
        return None
    if header.startswith(b"%PDF"):
        return ".pdf"
    if header.startswith(b"\xd0\xcf\x11\xe0"):
        return "ole2"
    if header.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(path) as z:
                namen = set(z.namelist())
        except Exception:
            return None
        if any(n.startswith("word/") for n in namen):
            return ".docx"
        if any(n.startswith("ppt/") for n in namen):
            return ".pptx"
        if any(n.startswith("xl/") for n in namen):
            return ".xlsx"
    return None


def route(path: Path) -> tuple:
    """Returns (extractor_name, callable). Imported here rather than at module
    level so one missing optional package cannot stop the whole run."""
    ext = path.suffix.lower()
    from . import legacy, media, office, simple

    table = {
        ".pptx": ("pptx", office.extract_pptx),
        ".pptm": ("pptx", office.extract_pptx),
        ".ppsx": ("legacy", legacy.extract_ppsx),
        ".docx": ("docx", office.extract_docx),
        ".docm": ("legacy", legacy.extract_docm),
        ".dotm": ("legacy", legacy.extract_docm),
        ".xlsx": ("xlsx", office.extract_xlsx),
        ".xlsm": ("xlsx", office.extract_xlsx),
        ".xlsb": ("xlsb", office.extract_xlsb),
        ".xls": ("xls", office.extract_xls),
        ".pdf": ("pdf", simple.extract_pdf),
        ".msg": ("msg", simple.extract_msg),
        ".txt": ("text", simple.extract_txt),
        ".md": ("text", simple.extract_txt),
        ".csv": ("text", simple.extract_csv),
        ".html": ("text", simple.extract_html),
        ".htm": ("text", simple.extract_html),
        ".xml": ("text", simple.extract_xml),
        ".doc": ("legacy", legacy.extract_doc),
        ".ppt": ("legacy", legacy.extract_ppt),
        ".eml": ("msg", simple.extract_eml),
    }
    for e in media.IMAGE_EXT:
        table[e] = ("media", media.extract_image)
    for e in media.AUDIO_EXT:
        table[e] = ("media", media.extract_audio)
    for e in media.VIDEO_EXT:
        table[e] = ("media", media.extract_video)
    if ext in UNSUPPORTED_EXT:
        return ("unsupported", None)
    hits = table.get(ext)
    if hits:
        if ext in OOXML_CHECK:
            echt = sniff(path)
            if echt and FAMILY.get(echt) and FAMILY.get(echt) != FAMILY.get(ext):
                name, fn = table[echt]
                return (f"{name}-korrigiert", fn)
        return hits
    echt = sniff(path)
    if echt and echt in table:
        name, fn = table[echt]
        return (f"{name}-erkannt", fn)
    return (None, None)
