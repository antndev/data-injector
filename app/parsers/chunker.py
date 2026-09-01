"""Cuts a Document into chunks at block boundaries instead of character counts.

The normal case is one block becoming one chunk. On the real corpus that
averages 1163 characters, which is exactly the right size. max_chars and
min_chars are only the emergency brakes for the outliers.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ChunkConfig:

    include: set = field(
        default_factory=lambda: {"title", "body", "table", "chart", "notes", "cell", "vlm"}
    )
    sheet_types: set = field(default_factory=lambda: {"text", "mixed"})
    max_chars: int = 2000
    min_chars: int = 80
    overlap: int = 120


@dataclass
class Chunk:

    text: str
    payload: dict


def _split(text: str, max_chars: int, overlap: int) -> list:
    """Teilt an Absatzgrenzen, faellt auf harte Schnitte zurueck."""
    if len(text) <= max_chars:
        return [text]
    pieces, cur = [], ""
    for abs_ in text.split("\n"):
        if cur and len(cur) + len(abs_) + 1 > max_chars:
            pieces.append(cur)
            cur = (cur[-overlap:] + "\n" + abs_) if overlap else abs_
        else:
            cur = f"{cur}\n{abs_}" if cur else abs_
    if cur:
        pieces.append(cur)
    out = []
    for t in pieces:
        while len(t) > max_chars * 1.5:
            out.append(t[:max_chars])
            t = t[max_chars - overlap :]
        out.append(t)
    return out


def already_mentions(text: str, title: str) -> bool:
    """True when the chunk already carries the document context itself."""
    flat = " ".join(text.split()).lower()
    needle = " ".join(title.split()).lower()
    return needle[:60] in flat


def document_title(doc) -> str:
    """First real title in the document, used as context for blocks without one.

    A table block on its own carries no customer name, so it matches every
    question about the same topic and answers with the wrong document."""
    for block in doc.blocks:
        for part in block.parts:
            if part.kind == "title" and part.text.strip():
                return part.text.strip()
    return ""


def _title_of(block) -> str:

    for p in block.parts:
        if p.kind == "title" and p.text.strip():
            return p.text.strip()
    return str(block.loc.get("title") or block.loc.get("sheet") or "").strip()


def chunk(doc, cfg: ChunkConfig = None) -> list:

    cfg = cfg or ChunkConfig()
    raw = []
    for b in doc.blocks:
        if b.kind == "sheet" and b.loc.get("type") not in cfg.sheet_types:
            continue
        text = b.text(cfg.include).strip()
        if not text:
            continue
        raw.append((b, text))
    merged = []
    buffer = None
    for b, text in raw:
        if buffer is not None:
            pb, pt = buffer
            if len(pt) < cfg.min_chars and pb.kind == b.kind:
                buffer = (pb, pt + "\n" + text)
                continue
            merged.append(buffer)
        buffer = (b, text)
    if buffer is not None:
        merged.append(buffer)
    context = document_title(doc)
    chunks = []
    for b, text in merged:
        title = _title_of(b)
        if context and not already_mentions(text, context):
            text = f"{context}\n{text}"
        pieces = _split(text, cfg.max_chars, cfg.overlap)
        for i, t in enumerate(pieces):
            if i > 0 and title and not t.lstrip().startswith(title):
                t = f"{title}\n{t}"
            chunks.append(
                Chunk(
                    text=t,
                    payload={
                        "file_hash": doc.source.hash,
                        "filename": doc.source.filename,
                        "ext": doc.source.ext,
                        "loc": b.loc,
                        "kind": b.kind,
                        "kinds": sorted({p.kind for p in b.parts if p.kind in cfg.include}),
                        "piece": i + 1,
                        "pieces": len(pieces),
                        "extractor": doc.extractor,
                        "extractor_version": doc.version,
                    },
                )
            )
    return chunks
