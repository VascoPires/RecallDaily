import sqlite3

import pytest


@pytest.fixture
def kobo_db(tmp_path):
    """A minimal KoboReader.sqlite with the tables and columns the importer reads."""
    path = tmp_path / "KoboReader.sqlite"
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE Bookmark (
            BookmarkID TEXT, VolumeID TEXT, Text TEXT, Annotation TEXT,
            DateCreated TEXT, Hidden TEXT, Type TEXT
        );
        CREATE TABLE content (ContentID TEXT, Title TEXT, Attribution TEXT);
        CREATE TABLE user (UserEmail TEXT, RefreshToken TEXT);
        """
    )
    book = "file:///mnt/onboard/Sun Tzu/Art of War, The - Sun Tzu.kepub.epub"
    sideloaded = "file:///mnt/onboard/Some_Author/Some_Book.epub"
    con.executemany(
        "INSERT INTO Bookmark VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1",
                book,
                "  All warfare is based on   deception. ",
                None,
                "2025-01-01",
                "false",
                "highlight",
            ),
            ("2", book, "Know the enemy.", "Applies to exams too", "2025-01-02", "false", "note"),
            ("3", book, None, None, "2025-01-03", "false", "dogear"),
            ("4", book, "Deleted one", None, "2025-01-04", "true", "highlight"),
            ("5", sideloaded, "1984 was a warning.", None, "2025-01-05", "false", "highlight"),
        ],
    )
    con.execute("INSERT INTO content VALUES (?, ?, ?)", (book, "The Art of War", "Sun Tzu"))
    con.execute("INSERT INTO user VALUES ('someone@example.com', 'secret')")
    con.commit()
    con.close()
    return path
