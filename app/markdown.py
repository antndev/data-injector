"""Renders an extracted Document as markdown.

The headings sit on the block boundaries the extractor found, so
OpenWebUI's markdown splitter reproduces that structure instead of
cutting at a character count."""

from __future__ import annotations

from pathlib import Path


def block_label(block) -> str:
    for key, template in (
        ("slide", "Slide {}"),
        ("page", "Page {}"),
        ("section", "Section {}"),
        ("sheet", "Sheet “{}”"),
    ):
        if block.loc.get(key):
            return template.format(block.loc[key])
    return block.kind


def block_tags(block) -> list:
    tags = []
    if block.loc.get("type"):
        tags.append(f"type {block.loc['type']}")
    if block.loc.get("duration_s"):
        tags.append(f"{block.loc['duration_s']} s")
    if block.loc.get("size_mb"):
        tags.append(f"{block.loc['size_mb']} MB")
    if block.loc.get("needs_ocr"):
        tags.append("image")
    if block.loc.get("needs_asr"):
        tags.append("audio")
    return tags


def render_part(part) -> list:
    text = part.text.strip()
    if not text:
        return []
    if part.kind == "title":
        return ["", f"**{text}**"]
    if part.kind in ("table", "cell"):
        return ["", text]
    if part.kind == "notes":
        return ["", "> **Note:** " + text.replace("\n", "\n> ")]
    if part.kind == "chart":
        return ["", "*Chart:* " + text.replace("\n", " / ")]
    if part.kind == "vlm":
        if text.lstrip().startswith("["):
            return ["", "*video timeline:*", "", text]
        return ["", "*read from image:*", "", text]
    if part.kind == "asr":
        return ["", "*heard in audio:*", "", text]
    return ["", text]


def to_markdown(doc, source: Path) -> str:
    chars = f"{doc.chars:,}".replace(",", "'")
    lines = [
        f"# {source.name}",
        "",
        f"*{doc.extractor}, {len(doc.blocks)} blocks, {chars} chars, "
        f"status {doc.status}*",
    ]
    if doc.error:
        lines += ["", f"> **Error:** {doc.error}"]
    lines.append("")
    for block in doc.blocks:
        tags = block_tags(block)
        head = f"## {block_label(block)}"
        if tags:
            head += f"  ({', '.join(tags)})"
        lines.append(head)
        if not block.parts:
            lines += ["", "*no text*", ""]
            continue
        for part in block.parts:
            lines += render_part(part)
        lines.append("")
    return "\n".join(lines)
