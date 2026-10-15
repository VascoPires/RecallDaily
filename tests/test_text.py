from recalldaily.text import clean_text, paragraphs


def test_clean_text_normalises_whitespace():
    assert clean_text("  a   b\t c  ") == "a b c"


def test_clean_text_blank_is_none():
    assert clean_text("   \n  ") is None
    assert clean_text(None) is None


def test_clean_text_keeps_leading_numbers():
    assert clean_text("1984 was not supposed to be a manual.") == (
        "1984 was not supposed to be a manual."
    )


def test_clean_text_keeps_paragraph_breaks():
    assert clean_text("one\r\n\r\n\r\n\r\ntwo") == "one\n\ntwo"


def test_paragraphs():
    assert paragraphs("a\nb\n\nc") == ["a b", "c"]
