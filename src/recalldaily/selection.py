"""Choose which highlights to resurface today.

Plain random sampling keeps repeating some quotes while others never appear.
Instead each highlight gets a weight that grows the longer it has gone unseen and
shrinks the more often it has been sent:

    weight = (1 + days since last sent) / (1 + times sent)

Highlights sent within the cooldown window are held back unless there are not
enough others. Never-sent highlights count as unseen for NEW_HIGHLIGHT_DAYS, so
fresh imports surface quickly without completely crowding out old favourites.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from datetime import date

from recalldaily.models import StoredHighlight

NEW_HIGHLIGHT_DAYS = 90


def days_unseen(h: StoredHighlight, today: date) -> int:
    if h.last_sent is None:
        return NEW_HIGHLIGHT_DAYS
    return max((today - h.last_sent).days, 0)


def weight(h: StoredHighlight, today: date) -> float:
    return (1 + days_unseen(h, today)) / (1 + h.times_sent)


def _weighted_sample(
    items: Sequence[StoredHighlight], k: int, today: date, rng: random.Random
) -> list[StoredHighlight]:
    # Efraimidis–Spirakis: sampling without replacement, proportional to weight.
    keyed = sorted(items, key=lambda h: rng.random() ** (1 / weight(h, today)), reverse=True)
    return keyed[:k]


def choose(
    highlights: Sequence[StoredHighlight],
    n: int,
    today: date | None = None,
    cooldown_days: int = 14,
    rng: random.Random | None = None,
) -> list[StoredHighlight]:
    today = today or date.today()
    rng = rng or random.Random()
    n = min(n, len(highlights))
    rested = [h for h in highlights if days_unseen(h, today) >= cooldown_days]
    recent = [h for h in highlights if days_unseen(h, today) < cooldown_days]
    picked = _weighted_sample(rested, n, today, rng)
    if len(picked) < n:
        picked += _weighted_sample(recent, n - len(picked), today, rng)
    return picked
