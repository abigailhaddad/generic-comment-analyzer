"""Tests for attachment selection and text extraction.

These cover the failure modes that made attachments extract to nothing without
anyone noticing: an empty extraction is indistinguishable from an attachment
that genuinely has no text, so none of it surfaced until the corpus was audited
by hand. Each test below pins one of those behaviours.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from attachment_utils import (  # noqa: E402
    _attachment_stem,
    group_attachments,
    is_gibberish,
)


def names(groups):
    return [[fn for _, fn in g] for g in groups]


# --- grouping -------------------------------------------------------------

def test_original_and_pdf_rendition_are_one_group():
    """regulations.gov stores the upload plus a PDF of it; that is one document."""
    g = group_attachments([('u1', 'attachment_1_Letter.docx'),
                           ('u2', 'attachment_2_Letter.pdf')])
    assert len(g) == 1


def test_docx_is_preferred_over_pdf():
    """A .docx cannot be a scan, carry a broken cmap, or need OCR."""
    g = group_attachments([('u1', 'attachment_1_Letter.pdf'),
                           ('u2', 'attachment_2_Letter.docx')])
    assert names(g)[0][0].endswith('.docx')


def test_images_are_tried_last():
    """An image costs a vision call, so only reach for it when nothing else exists."""
    g = group_attachments([('u1', 'attachment_1_x.png'),
                           ('u2', 'attachment_2_x.pdf'),
                           ('u3', 'attachment_3_x.docx')])
    assert [os.path.splitext(f)[1] for f in names(g)[0]] == ['.docx', '.pdf', '.png']


def test_distinct_uploads_stay_separate():
    """Two different letters must not collapse into one group and lose content."""
    g = group_attachments([('u1', 'attachment_1_LetterA.pdf'),
                           ('u2', 'attachment_2_LetterB.pdf')])
    assert len(g) == 2


def test_lone_image_is_still_selected():
    """When the picture is the whole comment, it has to be read."""
    g = group_attachments([('u1', 'attachment_1_photo.png')])
    assert names(g) == [['attachment_1_photo.png']]


def test_stem_ignores_our_prefix_and_extension():
    assert _attachment_stem('attachment_12_My Letter.PDF') == 'my letter'


def test_unknown_extension_sorts_last_but_is_kept():
    """An unsupported format is still a candidate if nothing better is present."""
    g = group_attachments([('u1', 'attachment_1_a.wpd'), ('u2', 'attachment_2_a.pdf')])
    assert names(g)[0] == ['attachment_2_a.pdf', 'attachment_1_a.wpd']


# --- gibberish detection --------------------------------------------------

def test_control_bytes_are_gibberish():
    """A PDF whose fonts carry no usable cmap yields raw control bytes."""
    assert is_gibberish('\x00\x01\x02\x03\x04' * 200) is True


def test_prose_of_names_and_places_is_not_gibberish():
    """The old word-list check binned a 21-page petition scoring 4.3% common words.

    Signatory lists are mostly names and state abbreviations; that is content,
    not a failed extraction.
    """
    petition = ('Jane Smith CA\nJohn Doe TX\nMaria Garcia PA\nWei Chen GA\n' * 60)
    assert is_gibberish(petition) is False


def test_ordinary_letter_is_not_gibberish():
    text = ('On behalf of our members I write to express strong opposition to '
            'the proposed revisions to 2 CFR Part 200. ') * 10
    assert is_gibberish(text) is False


def test_empty_and_tiny_input_is_gibberish():
    assert is_gibberish('') is True
    assert is_gibberish('   ') is True
    assert is_gibberish('hi') is True


def test_digits_only_is_gibberish():
    """Almost no letters means the extraction did not produce readable content."""
    assert is_gibberish('1234567890 ' * 100) is True


@pytest.mark.parametrize('text', [
    'The quick brown fox jumps over the lazy dog. ' * 5,
    'Comentario público sobre la regulación propuesta para asistencia financiera federal. ' * 5,
])
def test_language_agnostic(text):
    """The check judges character classes, so it must not be English-only."""
    assert is_gibberish(text) is False
