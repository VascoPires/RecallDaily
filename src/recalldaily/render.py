"""Render the daily digest as HTML and plain text."""

from __future__ import annotations

import base64
from collections.abc import Sequence
from dataclasses import dataclass, field
from html import escape
from importlib import resources
from string import Template

from recalldaily.models import Highlight
from recalldaily.text import paragraphs

QUOTE_HTML = Template("""
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                       style="margin-top:28px;">
                  <tr>
                    <td class="quote" style="padding:2px 0 2px 20px;border-left:3px solid #c7001b;">
                      $paragraphs
                      $note
                      <p class="muted" style="margin:12px 0 0;font-family:Manrope,Inter,Helvetica,Arial,sans-serif;font-size:13px;font-weight:600;letter-spacing:0.02em;color:#828282;">— $source</p>
                    </td>
                  </tr>
                </table>""")

PARAGRAPH_HTML = Template(
    '<p class="ink" style="margin:0 0 10px;font-family:Inter,Manrope,Helvetica,Arial,sans-serif;'
    'font-size:17px;line-height:1.65;color:#000000;">$text</p>'
)

NOTE_HTML = Template(
    '<p class="note" style="margin:10px 0 0;font-family:Inter,Manrope,Helvetica,Arial,sans-serif;'
    'font-size:15px;line-height:1.5;font-style:italic;font-weight:500;color:#2272a6;">'
    '<span style="font-style:normal;font-weight:600;">Your note:</span> $text</p>'
)


TITLE_CID = "recalldaily-title"


@dataclass
class Digest:
    html: str
    text: str
    # Inline images referenced from the HTML as "cid:<key>", sent as attachments.
    images: dict[str, bytes] = field(default_factory=dict)

    def browser_html(self) -> str:
        """The HTML with inline images embedded as data URIs, for previewing in a browser."""
        html = self.html
        for cid, data in self.images.items():
            uri = "data:image/png;base64," + base64.b64encode(data).decode("ascii")
            html = html.replace(f"cid:{cid}", uri)
        return html


def _quote_html(h: Highlight) -> str:
    # Everything that came from a book or the CSV is escaped: a stray "<" or "&"
    # in a highlight used to break the email markup.
    body = "\n".join(PARAGRAPH_HTML.substitute(text=escape(p)) for p in paragraphs(h.text))
    note = NOTE_HTML.substitute(text=escape(h.annotation)) if h.annotation else ""
    return QUOTE_HTML.substitute(paragraphs=body, note=note, source=escape(h.source))


def _quote_text(h: Highlight) -> str:
    lines = [f"“{h.text}”"]
    if h.annotation:
        lines.append(f"  Your note: {h.annotation}")
    lines.append(f"  — {h.source}")
    return "\n".join(lines)


def render(
    highlights: Sequence[Highlight], date_label: str, footer: str = "Recall Daily"
) -> Digest:
    templates = resources.files("recalldaily").joinpath("templates")
    template = Template(templates.joinpath("email.html").read_text("utf-8"))
    # The handwritten title is a PNG because Gmail doesn't load web fonts.
    title_png = templates.joinpath("title.png").read_bytes()
    count = f"{len(highlights)} highlight{'s' if len(highlights) != 1 else ''}"
    preheader = highlights[0].text[:120] if highlights else "Nothing to revisit today."
    html = template.substitute(
        title_src=f"cid:{TITLE_CID}",
        preheader=escape(preheader),
        date=escape(date_label),
        count=count,
        quotes="".join(_quote_html(h) for h in highlights),
        footer=escape(footer),
    )
    text = f"Recall Daily — {date_label}\n{count} to revisit today\n\n"
    text += "\n\n".join(_quote_text(h) for h in highlights) + f"\n\n{footer}\n"
    return Digest(html=html, text=text, images={TITLE_CID: title_png})
