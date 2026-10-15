"""Read highlights from a Kobo e-reader's KoboReader.sqlite.

Only the Bookmark and content tables are read. The database also holds account
tokens in its user table, which is why the raw file must never be committed.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from recalldaily.models import Highlight
from recalldaily.text import clean_text

# Bookmark.Type values that carry highlighted text. "dogear" is a page bookmark
# and "markup" is a handwritten drawing; neither has text worth sending.
TEXT_TYPES = ("highlight", "note")


def source_from_volume_id(volume_id: str | None) -> tuple[str, str | None]:
    """Best-effort (title, author) from a sideloaded book path.

    'file:///mnt/onboard/Sun Tzu/Art of War, The - Sun Tzu.kepub.epub'
    -> ('Art of War, The - Sun Tzu', 'Sun Tzu')
    """
    if not volume_id:
        return "", None
    path = re.sub(r"^file:///mnt/onboard/", "", volume_id)
    path = re.sub(r"(\.kepub)?\.epub$|\.pdf$", "", path)
    parts = [p.replace("_", " ").strip() for p in path.split("/") if p]
    if len(parts) > 1:
        return parts[-1], parts[-2]
    return (parts[0] if parts else ""), None


def read_kobo(path: str | Path) -> list[Highlight]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        columns = {row[1] for row in con.execute("PRAGMA table_info(Bookmark)")}
        where = ["b.Text IS NOT NULL"]
        if "Type" in columns:
            where.append(f"(b.Type IS NULL OR b.Type IN {TEXT_TYPES!r})")
        if "Hidden" in columns:
            where.append("(b.Hidden IS NULL OR b.Hidden NOT IN ('true', 1))")
        has_content = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'content'"
        ).fetchone()
        if has_content:
            query = (
                "SELECT b.Text, b.Annotation, b.VolumeID, c.Title, c.Attribution "
                "FROM Bookmark b LEFT JOIN content c ON c.ContentID = b.VolumeID "
            )
        else:
            query = "SELECT b.Text, b.Annotation, b.VolumeID, NULL, NULL FROM Bookmark b "
        query += "WHERE " + " AND ".join(where) + " ORDER BY b.DateCreated"
        rows = con.execute(query).fetchall()
    finally:
        con.close()

    highlights = []
    for text, annotation, volume_id, title, author in rows:
        text = clean_text(text)
        if not text:
            continue
        fallback_title, fallback_author = source_from_volume_id(volume_id)
        highlights.append(
            Highlight(
                text=text,
                annotation=clean_text(annotation),
                title=clean_text(title) or fallback_title,
                author=clean_text(author) or fallback_author,
                origin="kobo",
            )
        )
    return highlights
