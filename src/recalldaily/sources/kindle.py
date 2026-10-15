"""Read highlights from a Kindle's 'My Clippings.txt'.

Each clipping looks like:

    The Art of War (Sun Tzu)
    - Your Highlight on page 12 | Location 180-182 | Added on Monday, 3 February 2025 ...

    All warfare is based on deception.
    ==========

Notes are attached to the highlight whose location range contains them.
"""

from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path

from recalldaily.models import Highlight
from recalldaily.text import clean_text

SEPARATOR = "=========="
_TITLE_AUTHOR = re.compile(r"^(?P<title>.*?)\s*\((?P<author>[^()]*)\)\s*$")
_KIND = re.compile(r"^-\s*Your (?P<kind>Highlight|Note|Bookmark)", re.IGNORECASE)
_LOCATION = re.compile(r"Location (?P<start>\d+)(?:-(?P<end>\d+))?", re.IGNORECASE)


def _parse_header(line: str) -> tuple[str, str | None]:
    line = line.strip().lstrip("﻿")
    match = _TITLE_AUTHOR.match(line)
    if match:
        return match["title"].strip(), match["author"].strip() or None
    return line, None


def read_kindle_clippings(path: str | Path) -> list[Highlight]:
    content = Path(path).read_text(encoding="utf-8-sig")
    highlights: list[Highlight] = []
    # (title, start, end) for each highlight, so notes can find their highlight.
    locations: list[tuple[str, int, int]] = []
    seen: set[tuple[str, str]] = set()

    for block in content.split(SEPARATOR):
        lines = block.strip().splitlines()
        if len(lines) < 3:
            continue
        title, author = _parse_header(lines[0])
        kind_match = _KIND.match(lines[1].strip())
        if not kind_match:
            continue
        kind = kind_match["kind"].lower()
        loc = _LOCATION.search(lines[1])
        start = int(loc["start"]) if loc else -1
        end = int(loc["end"] or loc["start"]) if loc else -1
        text = clean_text("\n".join(lines[2:]))
        if not text or kind == "bookmark":
            continue

        if kind == "highlight":
            # Highlighting the same passage twice appends an identical clipping.
            key = (title, text)
            if key in seen:
                continue
            seen.add(key)
            highlights.append(Highlight(text=text, title=title, author=author, origin="kindle"))
            locations.append((title, start, end))
        else:  # note
            for i in range(len(highlights) - 1, -1, -1):
                h_title, h_start, h_end = locations[i]
                if h_title == title and h_start <= start <= h_end:
                    highlights[i] = replace(highlights[i], annotation=text)
                    break
    return highlights
