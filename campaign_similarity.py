"""Shared 5-gram MinHash similarity — the same "is this the same form letter"
measure pipeline.py's detect_campaigns() uses to build the organic campaigns,
factored out so a lightweight script (audit_named_campaigns.py) can check a
comment against a campaign without importing all of pipeline.py's dependencies
(psycopg2, docx, litellm) for two pure functions.

detect_campaigns() and cluster_families() in pipeline.py keep their own local
copies of this logic rather than importing it — they already differ slightly
from each other (whitespace collapsing, the short-text cutoff), and unifying
that is a separate, more careful change than this one. This module is the
definition to use for anything NEW that needs "does this text match a known
campaign's text", so it doesn't invent a third, slightly-different variant.
"""
import re

NUM_PERM = 128


def normalize_campaign_text(text: str) -> str:
    """Normalize text the same way detect_campaigns() does, so anything that
    measures similarity to a campaign is comparing apples to apples with what
    actually got clustered."""
    text = re.sub(r'[^a-z0-9 ]', '', (text or '').lower())
    return re.sub(r'\s+', ' ', text).strip()


def make_campaign_minhash(text: str, num_perm: int = NUM_PERM):
    """MinHash of a text's 5-gram word shingles, matching detect_campaigns()'s
    signature exactly. Returns None for text too short to shingle (fewer than
    5 words) — the same cutoff detect_campaigns() uses to skip a comment."""
    from datasketch import MinHash

    words = normalize_campaign_text(text).split()
    if len(words) < 5:
        return None
    shingles = set(tuple(words[j:j + 5]) for j in range(len(words) - 4))
    m = MinHash(num_perm=num_perm)
    for s in shingles:
        m.update(' '.join(s).encode('utf-8'))
    return m
