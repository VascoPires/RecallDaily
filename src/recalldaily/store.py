"""The highlight library: a single SQLite database that also tracks review history."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from recalldaily.models import Highlight, StoredHighlight

SCHEMA = """
CREATE TABLE IF NOT EXISTS highlights (
    id          INTEGER PRIMARY KEY,
    text        TEXT    NOT NULL,
    title       TEXT    NOT NULL DEFAULT '',
    author      TEXT,
    annotation  TEXT,
    origin      TEXT    NOT NULL,
    added_on    TEXT    NOT NULL,
    times_sent  INTEGER NOT NULL DEFAULT 0,
    last_sent   TEXT,
    UNIQUE (text, title)
);
"""


@dataclass
class ImportResult:
    added: int = 0
    updated: int = 0
    unchanged: int = 0


class Library:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(self.path)
        self.con.executescript(SCHEMA)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> Library:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def add(self, highlights: Iterable[Highlight], today: date | None = None) -> ImportResult:
        """Insert new highlights and refresh notes on existing ones. Safe to re-run."""
        today = today or date.today()
        result = ImportResult()
        with self.con:
            for h in highlights:
                row = self.con.execute(
                    "SELECT id, annotation, author FROM highlights WHERE text = ? AND title = ?",
                    (h.text, h.title),
                ).fetchone()
                if row is None:
                    self.con.execute(
                        "INSERT INTO highlights (text, title, author, annotation, origin, added_on)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        (h.text, h.title, h.author, h.annotation, h.origin, today.isoformat()),
                    )
                    result.added += 1
                    continue
                row_id, annotation, author = row
                new_annotation = h.annotation or annotation
                new_author = h.author or author
                if (new_annotation, new_author) != (annotation, author):
                    self.con.execute(
                        "UPDATE highlights SET annotation = ?, author = ? WHERE id = ?",
                        (new_annotation, new_author, row_id),
                    )
                    result.updated += 1
                else:
                    result.unchanged += 1
        return result

    def all(self) -> list[StoredHighlight]:
        rows = self.con.execute(
            "SELECT id, text, title, author, annotation, origin, times_sent, last_sent"
            " FROM highlights ORDER BY id"
        ).fetchall()
        return [
            StoredHighlight(
                id=row[0],
                text=row[1],
                title=row[2],
                author=row[3],
                annotation=row[4],
                origin=row[5],
                times_sent=row[6],
                last_sent=date.fromisoformat(row[7]) if row[7] else None,
            )
            for row in rows
        ]

    def mark_sent(self, ids: Iterable[int], when: date | None = None) -> None:
        when = when or date.today()
        with self.con:
            self.con.executemany(
                "UPDATE highlights SET times_sent = times_sent + 1, last_sent = ? WHERE id = ?",
                [(when.isoformat(), i) for i in ids],
            )

    def stats(self) -> dict:
        total, sent, never = self.con.execute(
            "SELECT COUNT(*), COALESCE(SUM(times_sent), 0),"
            " SUM(CASE WHEN times_sent = 0 THEN 1 ELSE 0 END) FROM highlights"
        ).fetchone()
        by_book = self.con.execute(
            "SELECT title, COUNT(*) FROM highlights GROUP BY title ORDER BY COUNT(*) DESC, title"
        ).fetchall()
        return {"total": total, "emails_sent": sent, "never_sent": never or 0, "by_book": by_book}
