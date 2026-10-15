import pytest

from recalldaily.sources import read_csv, read_kindle_clippings, read_kobo
from recalldaily.sources.kobo import source_from_volume_id


def test_kobo_reads_highlights_and_notes(kobo_db):
    highlights = read_kobo(kobo_db)
    texts = [h.text for h in highlights]
    assert texts == ["All warfare is based on deception.", "Know the enemy.", "1984 was a warning."]
    assert highlights[0].title == "The Art of War"
    assert highlights[0].author == "Sun Tzu"
    assert highlights[1].annotation == "Applies to exams too"


def test_kobo_falls_back_to_file_path(kobo_db):
    assert read_kobo(kobo_db)[2].title == "Some Book"
    assert read_kobo(kobo_db)[2].author == "Some Author"


def test_kobo_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_kobo(tmp_path / "nope.sqlite")


def test_source_from_volume_id():
    assert source_from_volume_id(None) == ("", None)
    assert source_from_volume_id("file:///mnt/onboard/Book.kepub.epub") == ("Book", None)


def test_csv(tmp_path):
    path = tmp_path / "quotes.csv"
    path.write_text(
        "Text,Annotation,Source\n"
        "Quote one,,Book A\n"
        '"Quote, with comma", A note, Book B\n'
        "No source,,\n",
        encoding="utf-8",
    )
    highlights = read_csv(path)
    assert [(h.text, h.annotation, h.title) for h in highlights] == [
        ("Quote one", None, "Book A"),
        ("Quote, with comma", "A note", "Book B"),
    ]


def test_csv_requires_columns(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("Quote\nhello\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Source, Text"):
        read_csv(path)


CLIPPINGS = """﻿The Art of War (Sun Tzu)
- Your Highlight on page 3 | Location 40-42 | Added on Monday, 3 February 2025 10:00:00

All warfare is based on deception.
==========
The Art of War (Sun Tzu)
- Your Note on page 3 | Location 42 | Added on Monday, 3 February 2025 10:01:00

Classic.
==========
The Art of War (Sun Tzu)
- Your Bookmark on page 9 | Location 120 | Added on Monday, 3 February 2025 10:02:00


==========
The Art of War (Sun Tzu)
- Your Highlight on page 3 | Location 40-42 | Added on Monday, 3 February 2025 10:03:00

All warfare is based on deception.
==========
Meditations
- Your Highlight on Location 10-11 | Added on Tuesday, 4 February 2025 08:00:00

You have power over your mind.
==========
"""


def test_kindle_clippings(tmp_path):
    path = tmp_path / "My Clippings.txt"
    path.write_text(CLIPPINGS, encoding="utf-8")
    highlights = read_kindle_clippings(path)
    assert len(highlights) == 2
    first, second = highlights
    assert (first.title, first.author, first.annotation) == (
        "The Art of War",
        "Sun Tzu",
        "Classic.",
    )
    assert (second.title, second.author) == ("Meditations", None)
