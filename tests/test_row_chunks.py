"""Tests for the chunked table-row sidecar.

The row data used to be a JavaScript literal inline in index.html: 33.6 MB of a
36.3 MB page, parsed before anything could render, and over Cloudflare Pages'
25 MiB per-file limit, which made the report unhostable there. It is now written
to comment_rows/<n>.js and pulled in by ordinary blocking script tags.

Two things can break silently here, and neither raises:

  - Row ORDER. The detail modal picks its shard with
    Math.floor(commentIndexById[id] / DETAIL_SHARD_SIZE), so the row order in
    these chunks and the shard order in comment_detail/ must be the same
    sequence. Reorder one and modals quietly show the wrong comment's text.
  - Field ORDER. Rows are positional arrays, so _row_to_list and the
    COMMENT_FIELDS list in report_template.html must agree exactly. Disagree and
    every column shifts - the table still renders, just with dates in the
    submitter column.
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from generate_report import (  # noqa: E402
    ROW_CHUNK_MAX_BYTES,
    _row_to_list,
    write_row_chunks,
)

TEMPLATE = os.path.join(os.path.dirname(__file__), '..', 'report_template.html')


def make_rows(n):
    return [
        {
            # Two dates: `received_date` is when it was submitted (the table's
            # column), `date` is when it was posted. Posted is the later of the two.
            'id': f'DOC-{i}', 'date': '2026-01-05', 'received_date': '2026-01-01',
            'submitter': f'S{i}',
            'organization': f'Org{i}', 'entity_type': 'Individual/Other',
            'cosigner_count': 1, 'stances_list': ['Position: Oppose'],
            'flags': {'x': False}, 'state_identified': 'CA',
            'political_affiliation': '', 'attachment_text': '' if i % 2 else 'has one',
            'campaign_id': None if i % 3 else 7, 'campaign_rank': None,
            'campaign_size': None if i % 3 else 12, 'campaign_stance': '',
            'multi_values': {}, 'comment_text': 'x', 'key_quote': 'q',
            'rationale': 'r', 'entity_name': 'e', 'cosigner_names': [],
            'state_quote': '', 'political_affiliation_quote': '',
        }
        for i in range(n)
    ]


def read_chunks(out_dir):
    """Concatenate the chunk files back in file-number order."""
    d = os.path.join(out_dir, 'comment_rows')
    paths = sorted(os.listdir(d), key=lambda f: int(f.split('.')[0]))
    rows = []
    for p in paths:
        body = open(os.path.join(d, p), encoding='utf-8').read()
        rows.extend(json.loads(re.search(r'\.concat\((\[.*\])\);\s*$', body, re.S).group(1)))
    return rows, paths


# --- order, the thing that breaks silently ---------------------------------

def test_rows_survive_chunking_in_exact_order(tmp_path):
    rows = make_rows(500)
    write_row_chunks(rows, str(tmp_path), max_bytes=4096)
    got, _ = read_chunks(str(tmp_path))
    assert got == [_row_to_list(r) for r in rows]


def template_fields():
    """The row field names, in order, as the template names them."""
    html = open(TEMPLATE, encoding='utf-8').read()
    block = re.search(r'const COMMENT_FIELDS = \[(.*?)\];', html, re.S).group(1)
    return re.findall(r"'([^']+)'", block)


def test_field_order_matches_the_template(tmp_path):
    """_row_to_list and COMMENT_FIELDS must agree, or every column shifts."""
    fields = template_fields()
    write_row_chunks(make_rows(1), str(tmp_path))
    row, _ = read_chunks(str(tmp_path))
    assert len(row[0]) == len(fields), (
        f'row has {len(row[0])} positions, template names {len(fields)}')
    # spot-check the positions whose meaning is unambiguous from the value
    by_name = dict(zip(fields, row[0]))
    assert by_name['id'] == 'DOC-0'
    # Distinct values on purpose: the two dates sit next to each other, so equal
    # values would let a swapped pair pass this order check unnoticed.
    assert by_name['date'] == '2026-01-05'
    assert by_name['received_date'] == '2026-01-01'
    assert by_name['submitter'] == 'S0'
    assert by_name['entity_type'] == 'Individual/Other'
    assert by_name['state'] == 'CA'


# --- the size limit that forced this ---------------------------------------

def test_no_chunk_exceeds_the_byte_cap(tmp_path):
    """A chunk over 25 MiB is rejected outright by Cloudflare Pages."""
    write_row_chunks(make_rows(4000), str(tmp_path), max_bytes=64 * 1024)
    d = tmp_path / 'comment_rows'
    for f in os.listdir(d):
        # the cap bounds accumulated row JSON; the wrapper adds a little
        assert os.path.getsize(d / f) < 64 * 1024 + 4096, f'{f} is too big'


def test_chunking_is_by_bytes_not_row_count(tmp_path):
    """Fat rows must produce more chunks, not oversized ones."""
    thin = make_rows(200)
    fat = make_rows(200)
    for r in fat:
        r['organization'] = 'x' * 5000
    n_thin = write_row_chunks(thin, str(tmp_path), max_bytes=32 * 1024)
    n_fat = write_row_chunks(fat, str(tmp_path), max_bytes=32 * 1024)
    assert n_fat > n_thin


def test_default_cap_is_under_the_cloudflare_limit():
    assert ROW_CHUNK_MAX_BYTES < 25 * 1024 * 1024


# --- edge cases ------------------------------------------------------------

def test_single_row_produces_one_chunk(tmp_path):
    assert write_row_chunks(make_rows(1), str(tmp_path)) == 1


def test_no_rows_produces_no_chunks(tmp_path):
    assert write_row_chunks([], str(tmp_path)) == 0
    assert os.listdir(tmp_path / 'comment_rows') == []


def test_rerun_removes_chunks_from_a_larger_previous_run(tmp_path):
    """A shrinking corpus must not leave orphan chunks the page still requests."""
    write_row_chunks(make_rows(4000), str(tmp_path), max_bytes=16 * 1024)
    n = write_row_chunks(make_rows(10), str(tmp_path), max_bytes=16 * 1024)
    _, paths = read_chunks(str(tmp_path))
    assert len(paths) == n == 1


def test_chunks_concat_rather_than_spread(tmp_path):
    """push(...rows) blows the argument limit on chunks this size."""
    write_row_chunks(make_rows(10), str(tmp_path))
    body = open(tmp_path / 'comment_rows' / '0.js', encoding='utf-8').read()
    assert '.concat(' in body
    assert '...' not in body


def test_missing_optional_campaign_fields_do_not_raise(tmp_path):
    rows = make_rows(3)
    for r in rows:
        r.pop('campaign_id'); r.pop('campaign_rank'); r.pop('campaign_size')
    write_row_chunks(rows, str(tmp_path))
    got, _ = read_chunks(str(tmp_path))
    # By field name, not by position: these were hardcoded indices, and adding a
    # column earlier in the row shifted them silently.
    by_name = dict(zip(template_fields(), got[0]))
    assert by_name['campaign_id'] is None and by_name['campaign_size'] == 0
