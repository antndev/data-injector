"""Decides which blocks are worth sending to a vision model."""

from __future__ import annotations

CHAR_THRESHOLD = 50


def needs_vision(block, threshold: int = CHAR_THRESHOLD) -> bool:
    if block.chars >= threshold:
        return False
    return block.loc.get("images", 0) > 0 or block.loc.get("needs_ocr", False)


def candidates(doc, threshold: int = CHAR_THRESHOLD) -> list:
    return [(i, b) for i, b in enumerate(doc.blocks) if needs_vision(b, threshold)]
