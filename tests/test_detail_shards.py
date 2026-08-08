"""Tests for the sharded per-comment detail sidecar.

The single-file version of this sidecar reached 124 MB (~26 MB gzipped) at 65k
comments and was fetched on *every* pageview, so roughly 3,700 visits exhausted
a 100 GB monthly bandwidth allowance — for data most visitors never looked at.
Sharding fixed the cost, but introduced a quieter failure mode in its place:
the page computes which shard a comment lives in from its row index, so a shard
whose contents don't match its index sends the browser to the wrong file and
the detail modal silently renders blank. Nothing raises; it just looks empty.

Each test below pins one property that failure would break.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from generate_report import (  # noqa: E402
    DETAIL_FIELDS,
    write_detail_shards,
)


def make_rows(n):
    """Rows shaped like prepare_rows() output, with identifiable field values."""
    return [
        {
            'id': f'DOC-{i}',
            'comment_text': f'body {i}',
            'key_quote': f'quote {i}',
            'rationale': f'why {i}',
            'entity_name': f'entity {i}',
            'cosigner_names': [f'cosigner {i}'],
            'state_quote': f'state {i}',
            'political_affiliation_quote': f'politics {i}',
        }
        for i in range(n)
    ]


def read_shards(out_dir):
    d = os.path.join(out_dir, 'comment_detail')
    return {
        int(fn.split('.')[0]): json.load(open(os.path.join(d, fn)))
        for fn in os.listdir(d)
    }


# --- shard placement ------------------------------------------------------

def test_comment_lands_in_the_shard_its_row_index_implies(tmp_path):
    """The page derives the shard from the row index; the file must agree."""
    size, count = write_detail_shards(make_rows(1250), str(tmp_path), shard_size=500)
    assert (size, count) == (500, 3)
    shards = read_shards(str(tmp_path))
    for i in range(1250):
        assert f'DOC-{i}' in shards[i // 500], f'row {i} is in the wrong shard'


def test_shards_are_disjoint(tmp_path):
    """A comment in two shards means one copy can go stale against the other."""
    write_detail_shards(make_rows(1250), str(tmp_path), shard_size=500)
    shards = read_shards(str(tmp_path))
    seen = set()
    for ids in shards.values():
        assert not (seen & ids.keys()), 'a comment appears in more than one shard'
        seen |= ids.keys()


def test_every_comment_is_written_exactly_once(tmp_path):
    """A dropped comment renders an empty modal rather than raising."""
    rows = make_rows(1250)
    write_detail_shards(rows, str(tmp_path), shard_size=500)
    shards = read_shards(str(tmp_path))
    written = set()
    for ids in shards.values():
        written |= ids.keys()
    assert written == {r['id'] for r in rows}


# --- shard contents -------------------------------------------------------

def test_all_detail_fields_survive_sharding(tmp_path):
    """Dropping a field here empties one row of the modal, silently."""
    write_detail_shards(make_rows(3), str(tmp_path), shard_size=500)
    entry = read_shards(str(tmp_path))[0]['DOC-1']
    assert set(entry) == set(DETAIL_FIELDS)
    assert entry['comment'] == 'body 1'
    assert entry['key_quote'] == 'quote 1'
    assert entry['cosigner_names'] == ['cosigner 1']
    assert entry['political_quote'] == 'politics 1'


# --- counts and edge cases ------------------------------------------------

def test_exact_multiple_does_not_emit_a_trailing_empty_shard(tmp_path):
    """An off-by-one here makes the page fetch a shard that does not exist."""
    _, count = write_detail_shards(make_rows(1000), str(tmp_path), shard_size=500)
    assert count == 2
    assert sorted(read_shards(str(tmp_path))) == [0, 1]


def test_no_rows_writes_no_shards(tmp_path):
    _, count = write_detail_shards([], str(tmp_path), shard_size=500)
    assert count == 0
    assert os.listdir(os.path.join(tmp_path, 'comment_detail')) == []


# --- stale artefacts ------------------------------------------------------

def test_rerun_removes_shards_from_a_larger_previous_run(tmp_path):
    """Shrinking the corpus must not leave orphan shards past the new count."""
    write_detail_shards(make_rows(1250), str(tmp_path), shard_size=500)
    _, count = write_detail_shards(make_rows(200), str(tmp_path), shard_size=500)
    assert count == 1
    assert sorted(read_shards(str(tmp_path))) == [0]


def test_legacy_single_file_is_deleted(tmp_path):
    """Left behind, the old 124 MB file still ships on every deploy."""
    legacy = tmp_path / 'comment_detail.json'
    legacy.write_text('{"stale": 1}')
    write_detail_shards(make_rows(10), str(tmp_path), shard_size=500)
    assert not legacy.exists()
