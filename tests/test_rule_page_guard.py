"""Tests for the Read-the-Rule page's presence guard.

read-the-rule.html is built only when rule_sections.json is present, and the
report links to it only in the same case. Both halves failing together is what
made this dangerous: rule_sections.json is gitignored and was never synced from
R2, so CI checkouts never had it, every automated deploy dropped the page *and*
the link, and the report looked perfectly fine with a hole where a page used to
be. The live URL 404'd for ten days before anyone opened it.

The file is committed now, so CI has it. These tests pin the guard that makes a
recurrence loud: a regulation whose config declares `rule_text:` is asserting it
has a rule page, so a missing rule_sections.json there is a broken build rather
than a configuration choice.
"""
import os
import sys

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from generate_report import config_declares_rule_text, load_rule_sections  # noqa: E402

REG_DIR = os.path.join(os.path.dirname(__file__), '..', 'regulations')


def _regulations_declaring_rule_text():
    """(slug, dir) for every regulation whose config declares `rule_text:`."""
    out = []
    if not os.path.isdir(REG_DIR):
        return out
    for slug in sorted(os.listdir(REG_DIR)):
        cfg = os.path.join(REG_DIR, slug, 'analyzer_config.yaml')
        if not os.path.isfile(cfg):
            continue
        with open(cfg) as f:
            if (yaml.safe_load(f) or {}).get('rule_text'):
                out.append((slug, os.path.join(REG_DIR, slug)))
    return out


def test_config_declares_rule_text_reads_the_config(tmp_path, monkeypatch):
    """The guard's trigger is the config, read from the working directory."""
    monkeypatch.chdir(tmp_path)
    assert config_declares_rule_text() is False  # no config at all

    (tmp_path / 'analyzer_config.yaml').write_text('report:\n  show_state: true\n')
    assert config_declares_rule_text() is False  # config, but no rule_text

    (tmp_path / 'analyzer_config.yaml').write_text(
        'rule_text:\n  federal_register_document: "2026-10817"\n  part: "200"\n')
    assert config_declares_rule_text() is True


def test_missing_rule_sections_reads_as_absent(tmp_path, monkeypatch):
    """No file, empty file, and a non-list payload all count as absent, so the
    guard cannot be satisfied by a file that exists but carries nothing."""
    monkeypatch.chdir(tmp_path)
    assert load_rule_sections() is None

    (tmp_path / 'rule_sections.json').write_text('[]')
    assert load_rule_sections() is None

    (tmp_path / 'rule_sections.json').write_text('{"sections": []}')
    assert load_rule_sections() is None


@pytest.mark.parametrize('slug,path', _regulations_declaring_rule_text(),
                         ids=lambda v: v if isinstance(v, str) else '')
def test_declared_rule_page_has_its_sections_committed(slug, path):
    """A regulation that declares `rule_text:` ships rule_sections.json in the
    repo. This is the check that would have caught the ten-day outage: it fails
    on a fresh clone, which is exactly what CI builds from."""
    sections = os.path.join(path, 'rule_sections.json')
    assert os.path.isfile(sections), (
        f"{slug} declares `rule_text:` but rule_sections.json is not in the repo. "
        f"A CI checkout would build no read-the-rule.html and no link to it.")
    assert os.path.getsize(sections) > 0, f"{slug}: rule_sections.json is empty"
