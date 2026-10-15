"""Text normalisation shared by all importers."""

from __future__ import annotations

import re

_INLINE_WS = re.compile(r"[ \t ]+")
_MANY_NEWLINES = re.compile(r"\n{3,}")


def clean_text(text: str | None) -> str | None:
    """Normalise whitespace, keeping paragraph breaks. Returns None for blank input.

    Unlike the original implementation, leading digits are kept: a quote such as
    "1984 was not supposed to be an instruction manual" must survive intact.
    """
    if text is None:
        return None
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [_INLINE_WS.sub(" ", line).strip() for line in text.split("\n")]
    text = _MANY_NEWLINES.sub("\n\n", "\n".join(lines)).strip()
    return text or None


def paragraphs(text: str) -> list[str]:
    """Split text into paragraphs on blank lines, joining soft line breaks."""
    return [" ".join(p.split("\n")) for p in text.split("\n\n") if p.strip()]
