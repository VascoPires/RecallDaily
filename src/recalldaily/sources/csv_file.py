"""Read manually curated quotes from a CSV file.

Columns: Text (required), Source (required), Author and Annotation (optional).
"""

from __future__ import annotations

import csv
from pathlib import Path

from recalldaily.models import Highlight
from recalldaily.text import clean_text


def read_csv(path: str | Path) -> list[Highlight]:
    highlights = []
    with open(path, newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file, skipinitialspace=True)
        missing = {"Text", "Source"} - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path}: missing required column(s): {', '.join(sorted(missing))}")
        for row in reader:
            text = clean_text(row.get("Text"))
            title = clean_text(row.get("Source"))
            if not text or not title:
                continue
            highlights.append(
                Highlight(
                    text=text,
                    title=title,
                    author=clean_text(row.get("Author")),
                    annotation=clean_text(row.get("Annotation")),
                    origin="csv",
                )
            )
    return highlights
