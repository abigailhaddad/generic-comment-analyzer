"""Tests for the anti-shrink guard in save_results.

The guard exists so a stray `--sample` or `--reprocess` cannot quietly replace a
157k-row canonical parquet with 5 rows. It was dead code from the day it was
written: it counted rows with `pd.read_parquet(path, columns=[])`, which returns
a frame with no columns AND no rows, so `existing` was always 0, `existing > 100`
was never true, and nothing was ever guarded. Nobody noticed because the guard
staying silent is indistinguishable from the guard passing.

These tests pin the behaviour rather than the implementation, so a future
refactor that reintroduces a zero-row count fails loudly.
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline import save_results  # noqa: E402


def _rows(n, tag='x'):
    return [{'id': f'C-{i}', 'text': f'{tag} comment {i}'} for i in range(n)]


def test_row_count_helper_sees_real_rows(tmp_path):
    """The bug in one line: columns=[] reads zero rows off a non-empty file."""
    p = tmp_path / 'f.parquet'
    pd.DataFrame(_rows(500)).to_parquet(p, index=False)

    import pyarrow.parquet as pq
    assert pq.ParquetFile(p).metadata.num_rows == 500
    # The old approach, kept as a regression marker for why this is not used:
    assert len(pd.read_parquet(p, columns=[])) == 0


def test_refuses_to_shrink_an_existing_parquet(tmp_path):
    """Writing far fewer rows over a large parquet must raise, not overwrite."""
    p = str(tmp_path / 'full_run.parquet')
    pd.DataFrame(_rows(1000, 'original')).to_parquet(p, index=False)

    with pytest.raises(SystemExit) as e:
        save_results(_rows(5, 'tiny'), p)
    assert '1000' in str(e.value) and '5' in str(e.value)

    # the original survives untouched, and a backup was taken
    assert len(pd.read_parquet(p)) == 1000
    assert os.path.exists(p + '.bak')
    assert len(pd.read_parquet(p + '.bak')) == 1000


def test_force_overrides_the_guard(tmp_path):
    """--force is the documented escape hatch and must still work."""
    p = str(tmp_path / 'full_run.parquet')
    pd.DataFrame(_rows(1000)).to_parquet(p, index=False)
    save_results(_rows(5), p, force=True)
    assert len(pd.read_parquet(p)) == 5


def test_growth_is_never_blocked(tmp_path):
    """The normal path — a corpus getting bigger — must not trip the guard.
    This is what every real run does, including the 157k -> 167k bulk load."""
    p = str(tmp_path / 'full_run.parquet')
    pd.DataFrame(_rows(1000)).to_parquet(p, index=False)
    save_results(_rows(1200), p)
    assert len(pd.read_parquet(p)) == 1200


def test_small_shrink_is_allowed(tmp_path):
    """Only a drop past 50% is treated as accidental; ordinary churn is not."""
    p = str(tmp_path / 'full_run.parquet')
    pd.DataFrame(_rows(1000)).to_parquet(p, index=False)
    save_results(_rows(800), p)          # -20%, above the 50% floor
    assert len(pd.read_parquet(p)) == 800


def test_tiny_existing_file_is_not_guarded(tmp_path):
    """Below 100 rows the file is assumed to be a fixture/smoke-test artifact."""
    p = str(tmp_path / 'full_run.parquet')
    pd.DataFrame(_rows(50)).to_parquet(p, index=False)
    save_results(_rows(2), p)
    assert len(pd.read_parquet(p)) == 2
