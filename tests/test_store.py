from datetime import date

from recalldaily.models import Highlight
from recalldaily.store import Library


def test_add_is_idempotent_and_updates_notes(tmp_path):
    with Library(tmp_path / "lib.db") as lib:
        first = lib.add([Highlight("a", "Book"), Highlight("b", "Book")])
        again = lib.add([Highlight("a", "Book", annotation="new note"), Highlight("b", "Book")])
        assert (first.added, first.updated) == (2, 0)
        assert (again.added, again.updated, again.unchanged) == (0, 1, 1)
        assert [h.annotation for h in lib.all()] == ["new note", None]


def test_same_text_different_books_are_distinct(tmp_path):
    with Library(tmp_path / "lib.db") as lib:
        assert lib.add([Highlight("same", "A"), Highlight("same", "B")]).added == 2


def test_mark_sent_and_stats(tmp_path):
    with Library(tmp_path / "lib.db") as lib:
        lib.add([Highlight("a", "Book"), Highlight("b", "Other")])
        a = lib.all()[0]
        lib.mark_sent([a.id], date(2025, 3, 1))
        a = lib.all()[0]
        assert (a.times_sent, a.last_sent) == (1, date(2025, 3, 1))
        stats = lib.stats()
        assert stats["total"] == 2
        assert stats["never_sent"] == 1
        assert stats["emails_sent"] == 1
