import random
from datetime import date, timedelta

from recalldaily.models import StoredHighlight
from recalldaily.selection import choose, weight

TODAY = date(2025, 6, 1)


def make(i, times_sent=0, days_ago=None):
    last = TODAY - timedelta(days=days_ago) if days_ago is not None else None
    return StoredHighlight(text=f"q{i}", title="B", id=i, times_sent=times_sent, last_sent=last)


def test_never_returns_more_than_available():
    assert len(choose([make(1), make(2)], 5, TODAY, rng=random.Random(0))) == 2


def test_no_duplicates():
    picked = choose([make(i) for i in range(20)], 10, TODAY, rng=random.Random(1))
    assert len({h.id for h in picked}) == 10


def test_recently_sent_are_held_back():
    library = [make(1, 1, days_ago=1), make(2, 1, days_ago=2), make(3, 1, days_ago=30)]
    for seed in range(50):
        assert [h.id for h in choose(library, 1, TODAY, 14, random.Random(seed))] == [3]


def test_recent_used_to_top_up_when_needed():
    library = [make(1, 1, days_ago=1), make(2, 1, days_ago=30)]
    assert {h.id for h in choose(library, 2, TODAY, 14, random.Random(0))} == {1, 2}


def test_weight_prefers_long_unseen_and_rarely_sent():
    assert weight(make(1, 1, days_ago=100), TODAY) > weight(make(2, 1, days_ago=20), TODAY)
    assert weight(make(1, 1, days_ago=50), TODAY) > weight(make(2, 5, days_ago=50), TODAY)


def test_distribution_favours_overdue_highlights():
    library = [make(1, 10, days_ago=15), make(2, 0, days_ago=None)]
    rng = random.Random(42)
    hits = sum(choose(library, 1, TODAY, 14, rng)[0].id == 2 for _ in range(2000))
    assert hits > 1800
