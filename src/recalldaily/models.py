from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Highlight:
    """A highlight as produced by an importer, before it is stored."""

    text: str
    title: str = ""
    author: str | None = None
    annotation: str | None = None
    origin: str = "manual"

    @property
    def source(self) -> str:
        if self.title and self.author:
            return f"{self.title}, {self.author}"
        return self.title or self.author or "Unknown source"


@dataclass(frozen=True)
class StoredHighlight(Highlight):
    """A highlight read back from the library, with its review history."""

    id: int = 0
    times_sent: int = 0
    last_sent: date | None = None
