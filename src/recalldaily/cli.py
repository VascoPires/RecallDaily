"""Command-line entry point.

recalldaily import kobo /path/to/KoboReader.sqlite
recalldaily import kindle "My Clippings.txt"
recalldaily import csv manual_quotes.csv
recalldaily send                    # email today's digest
recalldaily send --dry-run out.html # write it to a file instead
recalldaily stats
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from recalldaily import __version__, mailer, selection
from recalldaily.config import load_credentials, load_settings
from recalldaily.render import render
from recalldaily.sources import IMPORTERS
from recalldaily.store import Library

log = logging.getLogger("recalldaily")


def cmd_import(args, settings) -> int:
    highlights = IMPORTERS[args.kind](args.path)
    with Library(args.library or settings.library) as library:
        result = library.add(highlights)
    log.info(
        "Imported %d highlight(s) from %s: %d new, %d updated, %d unchanged",
        len(highlights),
        args.path,
        result.added,
        result.updated,
        result.unchanged,
    )
    return 0


def cmd_send(args, settings) -> int:
    today = date.today()
    n = args.num or settings.num_quotes
    with Library(args.library or settings.library) as library:
        picked = selection.choose(library.all(), n, today, settings.cooldown_days)
        if not picked:
            log.error("The library is empty. Import some highlights first.")
            return 1
        label = today.strftime(settings.date_format)
        digest = render(picked, label, settings.footer)

        if args.dry_run:
            Path(args.dry_run).write_text(digest.html, encoding="utf-8")
            log.info("Wrote %d highlight(s) to %s (nothing sent)", len(picked), args.dry_run)
            return 0

        creds = load_credentials()
        msg = mailer.build_message(digest, settings.subject.format(date=label), creds)
        mailer.send(msg, creds, settings.smtp_host, settings.smtp_port)
        library.mark_sent([h.id for h in picked], today)
    log.info("Sent %d highlight(s) to %d recipient(s)", len(picked), len(creds.recipients))
    return 0


def cmd_stats(args, settings) -> int:
    with Library(args.library or settings.library) as library:
        stats = library.stats()
    print(f"Highlights:   {stats['total']}")
    print(f"Never sent:   {stats['never_sent']}")
    print(f"Times sent:   {stats['emails_sent']}")
    print("\nBy book:")
    for title, count in stats["by_book"]:
        print(f"  {count:>4}  {title or '(untitled)'}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="recalldaily", description=__doc__.split("\n")[0])
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--config", help="settings file (default: recalldaily.toml if present)")
    parser.add_argument("--library", help="library database (overrides the config)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("import", help="add highlights from an e-reader export or CSV")
    p.add_argument("kind", choices=sorted(IMPORTERS))
    p.add_argument("path")
    p.set_defaults(func=cmd_import)

    p = sub.add_parser("send", help="pick today's highlights and email them")
    p.add_argument("-n", "--num", type=int, help="number of highlights to send")
    p.add_argument("--dry-run", metavar="FILE", help="write the HTML email to FILE instead")
    p.set_defaults(func=cmd_send)

    p = sub.add_parser("stats", help="show library statistics")
    p.set_defaults(func=cmd_stats)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    try:
        return args.func(args, load_settings(args.config))
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        log.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
