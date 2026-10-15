"""Importers that turn e-reader exports into Highlight objects."""

from recalldaily.sources.csv_file import read_csv
from recalldaily.sources.kindle import read_kindle_clippings
from recalldaily.sources.kobo import read_kobo

IMPORTERS = {
    "kobo": read_kobo,
    "csv": read_csv,
    "kindle": read_kindle_clippings,
}

__all__ = ["IMPORTERS", "read_csv", "read_kindle_clippings", "read_kobo"]
