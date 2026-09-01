"""Extractors for PDF, email and the simple text formats."""

from __future__ import annotations

from pathlib import Path

from .base import Block, Document, Part, EXTRACTOR_VERSION, source_info

LEERE_SEITE = 30


def extract_pdf(path: Path) -> Document:

    import fitz

    doc = Document(
        source=source_info(path), extractor="pdf-pymupdf", version=EXTRACTOR_VERSION["pdf"]
    )
    with fitz.open(str(path)) as f:
        for i, page in enumerate(f, 1):
            t = page.get_text().strip()
            images = len(page.get_images(full=True))
            doc.blocks.append(
                Block(
                    kind="page",
                    loc={"page": i, "images": images, "needs_ocr": len(t) < LEERE_SEITE},
                    parts=[Part("body", t)] if t else [],
                )
            )
    return doc


def extract_msg(path: Path) -> Document:

    import extract_msg as em

    doc = Document(
        source=source_info(path), extractor="msg", version=EXTRACTOR_VERSION["msg"]
    )
    with em.openMsg(str(path)) as m:
        header = "\n".join(
            filter(
                None,
                [
                    f"Betreff: {m.subject or ''}",
                    f"Von: {m.sender or ''}",
                    f"An: {m.to or ''}",
                    f"Datum: {m.date or ''}",
                ],
            )
        )
        parts = [Part("title", (m.subject or "").strip())] if m.subject else []
        parts.append(Part("body", header + "\n\n" + (m.body or "")))
        doc.blocks.append(Block("message", {"subject": (m.subject or "")[:120]}, parts))
    return doc


def _ein_block(path: Path, extractor: str, key: str, text: str) -> Document:

    doc = Document(
        source=source_info(path), extractor=extractor, version=EXTRACTOR_VERSION[key]
    )
    if text.strip():
        doc.blocks.append(Block("doc", {}, [Part("body", text.strip())]))
    return doc


def extract_txt(path: Path) -> Document:

    return _ein_block(
        path, "txt", "text", path.read_text(encoding="utf-8", errors="replace")
    )


def extract_csv(path: Path) -> Document:

    import csv

    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        rows = list(csv.reader(f))
    md = []
    if rows:
        md.append("| " + " | ".join(rows[0]) + " |")
        md.append("|" + "|".join(["---"] * len(rows[0])) + "|")
        for r in rows[1:]:
            md.append("| " + " | ".join(r) + " |")
    doc = Document(
        source=source_info(path), extractor="csv", version=EXTRACTOR_VERSION["text"]
    )
    if md:
        doc.blocks.append(
            Block("sheet", {"rows": len(rows)}, [Part("cell", "\n".join(md))])
        )
    return doc


def extract_html(path: Path) -> Document:

    from bs4 import BeautifulSoup

    soup = BeautifulSoup(path.read_bytes(), "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return _ein_block(path, "html", "text", soup.get_text("\n", strip=True))


def extract_xml(path: Path) -> Document:

    import xml.etree.ElementTree as ET

    tree = ET.parse(str(path))
    txt = "\n".join(el.text.strip() for el in tree.iter() if el.text and el.text.strip())
    return _ein_block(path, "xml", "text", txt)


def extract_eml(path: Path) -> Document:
    """RFC-822-Mail. Anders als .msg ohne Fremdbibliothek lesbar."""
    import email
    from email import policy

    m = email.message_from_bytes(path.read_bytes(), policy=policy.default)
    doc = Document(
        source=source_info(path), extractor="eml", version=EXTRACTOR_VERSION["msg"]
    )
    header = "\n".join(f"{k}: {m[k]}" for k in ("Subject", "From", "To", "Date") if m[k])
    try:
        koerper = m.get_body(preferencelist=("plain", "html"))
        text = koerper.get_content() if koerper else ""
    except Exception:
        text = ""
    parts = []
    if m["Subject"]:
        parts.append(Part("title", str(m["Subject"])))
    parts.append(Part("body", (header + "\n\n" + (text or "")).strip()))
    doc.blocks.append(Block("message", {"subject": str(m["Subject"] or "")[:120]}, parts))
    return doc
