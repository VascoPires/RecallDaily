from email import message_from_bytes, policy

from recalldaily.cli import main
from recalldaily.config import Credentials, load_credentials
from recalldaily.mailer import build_message
from recalldaily.models import Highlight
from recalldaily.render import render


def test_render_escapes_html():
    digest = render([Highlight("x < y & <b>bold</b>", "A & B", annotation="<i>")], "01.01.2025")
    assert "x &lt; y &amp; &lt;b&gt;bold&lt;/b&gt;" in digest.html
    assert "A &amp; B" in digest.html
    assert "<b>bold" not in digest.html
    assert "1 highlight to revisit" in digest.html
    assert "x < y" in digest.text


def test_message_has_text_and_html_parts():
    creds = Credentials("u", "p", "from@example.com", ["a@example.com", "b@example.com"])
    msg = build_message(render([Highlight("hi", "Book")], "today"), "Subject ✓", creds)
    parsed = message_from_bytes(msg.as_bytes())
    types = [part.get_content_type() for part in parsed.walk()]
    assert types == [
        "multipart/alternative",
        "text/plain",
        "multipart/related",
        "text/html",
        "image/png",
    ]
    assert parsed["To"] == "a@example.com, b@example.com"


def test_title_image_is_inline_and_referenced():
    creds = Credentials("u", "p", "from@example.com", ["a@example.com"])
    msg = build_message(render([Highlight("hi", "Book")], "today"), "Subject", creds)
    parsed = message_from_bytes(msg.as_bytes(), policy=policy.default)
    html = next(p for p in parsed.walk() if p.get_content_type() == "text/html")
    image = next(p for p in parsed.walk() if p.get_content_type() == "image/png")
    cid = image["Content-ID"].strip("<>")
    assert f'src="cid:{cid}"' in html.get_content()
    assert image.get_content_disposition() == "inline"
    assert image.get_payload(decode=True).startswith(b"\x89PNG")


def test_credentials_from_env():
    creds = load_credentials({"MAIL_USER": "me", "MAIL_PASSWORD": "pw", "DAILY_REPORT_TO": "x, y"})
    assert creds.sender == "me"
    assert creds.recipients == ["x", "y"]


def test_cli_import_and_dry_run(tmp_path, kobo_db, monkeypatch):
    monkeypatch.chdir(tmp_path)
    lib = str(tmp_path / "lib.db")
    assert main(["--library", lib, "import", "kobo", str(kobo_db)]) == 0
    out = tmp_path / "preview.html"
    assert main(["--library", lib, "send", "-n", "2", "--dry-run", str(out)]) == 0
    preview = out.read_text()
    assert preview.count('class="quote"') == 2
    # The browser preview embeds the title image instead of an email-only cid: link.
    assert "data:image/png;base64," in preview
    assert "cid:" not in preview
    assert main(["--library", lib, "stats"]) == 0


def test_cli_send_on_empty_library_fails(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert main(["--library", str(tmp_path / "lib.db"), "send", "--dry-run", "x.html"]) == 1


def test_cli_missing_credentials(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for var in ("MAIL_USER", "MAIL_PASSWORD", "DAILY_REPORT_TO"):
        monkeypatch.delenv(var, raising=False)
    lib = str(tmp_path / "lib.db")
    csv = tmp_path / "q.csv"
    csv.write_text("Text,Source\nhello,Book\n", encoding="utf-8")
    assert main(["--library", lib, "import", "csv", str(csv)]) == 0
    assert main(["--library", lib, "send"]) == 1
