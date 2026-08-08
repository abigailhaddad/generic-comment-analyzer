"""Tests for the guards that stop a bad run from being published.

Each of these pins a failure that actually shipped. They share a shape: nothing
crashed, the totals stayed plausible, and the damage was only found by a human
reading a percentage days later.

  - identity quotes leaking between commenters who wrote the same words
    (Greg Power's comment displayed Erin Brandewie's name)
  - a failed analysis cached as though it were a result, so the comment was
    never retried and its missing stance quietly dragged the headline down
  - a run continuing after the API stopped answering, publishing the hole
  - a batch landing that looks nothing like the corpus it joins
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline import (  # noqa: E402
    _is_credentials_error,
    check_batch_quality,
    localize_identity_quotes,
    stance_shares,
)

OPPOSE = {'stances': ['Position: Oppose the proposed rule']}
SUPPORT = {'stances': ['Position: Support the proposed rule']}
NO_STANCE = {'stances': []}


def comment(cid, analysis):
    return {'id': cid, 'analysis': analysis}


# --- identity quotes must not travel between commenters -------------------

def test_quote_is_kept_for_the_commenter_it_came_from():
    analysis = {'entity_name': 'Erin Brandewie'}
    own = {'submitter': 'Erin Brandewie', 'organization': '',
           'text': 'I oppose this proposed rule.'}
    assert localize_identity_quotes(analysis, own)['entity_name'] == 'Erin Brandewie'


def test_another_commenters_name_is_cleared():
    """Five people submitted this text verbatim; only one of them is Erin."""
    analysis = {'entity_name': 'Erin Brandewie'}
    someone_else = {'submitter': 'Greg Power', 'organization': '',
                    'text': 'I oppose this proposed rule.'}
    assert localize_identity_quotes(analysis, someone_else)['entity_name'] == ''


def test_clearing_a_quote_also_clears_what_it_justified():
    """A state read off someone else's address is not this commenter's state."""
    analysis = {'state_quote': 'Columbus, Ohio', 'state_identified': 'OH'}
    someone_else = {'submitter': 'Greg Power', 'organization': '',
                    'text': 'I oppose this proposed rule.'}
    out = localize_identity_quotes(analysis, someone_else)
    assert out['state_quote'] == ''
    assert out['state_identified'] == ''


def test_quote_from_the_shared_text_survives():
    """Text-derived quotes are genuinely shared — only metadata-derived ones aren't."""
    text = 'As a Californian I oppose this rule.'
    analysis = {'state_quote': 'As a Californian', 'state_identified': 'CA'}
    out = localize_identity_quotes(analysis, {'submitter': 'Anyone', 'organization': '',
                                              'text': text})
    assert out['state_identified'] == 'CA'


def test_organization_name_counts_as_the_commenters_own():
    analysis = {'entity_name': 'Yale University'}
    own = {'submitter': 'J. Doe', 'organization': 'Yale University', 'text': 'We object.'}
    assert localize_identity_quotes(analysis, own)['entity_name'] == 'Yale University'


def test_untouched_analysis_is_returned_unchanged():
    """Same object back when nothing needed clearing — the caller counts on this."""
    analysis = {'entity_name': 'Erin Brandewie'}
    own = {'submitter': 'Erin Brandewie', 'organization': '', 'text': 'x'}
    assert localize_identity_quotes(analysis, own) is analysis


def test_non_dict_analysis_is_passed_through():
    assert localize_identity_quotes(None, {'submitter': 'a', 'text': 'b'}) is None


# --- credential failures are not per-comment problems ---------------------

@pytest.mark.parametrize('error', [
    'litellm.RateLimitError: OpenAIException - You have no credits remaining.',
    'AuthenticationError: invalid api key',
    'Error code: 429 - insufficient_quota',
])
def test_credentials_errors_are_recognised(error):
    assert _is_credentials_error(error)


@pytest.mark.parametrize('error', [None, '', 'Timeout waiting for response',
                                   'litellm.APIError: 500 internal server error'])
def test_ordinary_errors_are_not_credentials_errors(error):
    """A per-comment failure must not stop a run that is otherwise fine."""
    assert not _is_credentials_error(error)


# --- stance shares --------------------------------------------------------

def test_stance_shares_are_percentages():
    shares = stance_shares([OPPOSE, OPPOSE, OPPOSE, SUPPORT])
    assert shares['Oppose'] == 75.0
    assert shares['Support'] == 25.0


def test_stance_shares_of_nothing_is_empty():
    assert stance_shares([]) == {}


# --- the quality gate -----------------------------------------------------

def test_a_normal_batch_passes():
    previous = [OPPOSE] * 940 + [SUPPORT] * 60
    current = [comment(i, OPPOSE) for i in range(940)] \
        + [comment(1000 + i, SUPPORT) for i in range(60)] \
        + [comment(2000 + i, OPPOSE) for i in range(95)] \
        + [comment(3000 + i, SUPPORT) for i in range(5)]
    assert check_batch_quality(current, set(range(2000)), previous, {}) == []


def test_a_batch_unlike_the_corpus_is_flagged():
    """94% oppose corpus, then 100 arrivals that are almost all support."""
    previous = [OPPOSE] * 940 + [SUPPORT] * 60
    current = [comment(i, OPPOSE) for i in range(940)] \
        + [comment(1000 + i, SUPPORT) for i in range(60)] \
        + [comment(5000 + i, SUPPORT) for i in range(100)]
    problems = check_batch_quality(current, set(range(2000)), previous, {})
    assert problems and 'new comment' in problems[0]


def test_a_small_batch_is_not_judged():
    """Ten comments swing on their own noise; the gate must not cry wolf."""
    previous = [OPPOSE] * 940 + [SUPPORT] * 60
    current = [comment(i, OPPOSE) for i in range(940)] \
        + [comment(1000 + i, SUPPORT) for i in range(60)] \
        + [comment(5000 + i, SUPPORT) for i in range(10)]
    assert check_batch_quality(current, set(range(2000)), previous, {}) == []


def test_analysis_quietly_failing_is_caught():
    """August 2026: an exhausted key left comments with no stance at all."""
    previous = [OPPOSE] * 1000
    current = [comment(i, OPPOSE) for i in range(950)] \
        + [comment(i, NO_STANCE) for i in range(950, 1000)]
    problems = check_batch_quality(current, set(range(1000)), previous, {})
    assert any('no stance' in p for p in problems)


def test_the_july_campaign_flip_would_have_been_caught():
    """A support campaign relabelled oppose moved the corpus 94% -> 98%."""
    previous = [OPPOSE] * 940 + [SUPPORT] * 60
    current = [comment(i, OPPOSE) for i in range(980)] \
        + [comment(1000 + i, SUPPORT) for i in range(20)]
    problems = check_batch_quality(current, set(range(2000)), previous, {})
    assert any('overall' in p for p in problems)


def test_the_gate_can_be_turned_off_per_regulation():
    previous = [OPPOSE] * 1000
    current = [comment(i, SUPPORT) for i in range(1000)]
    config = {'quality_gate': {'enabled': False}}
    assert check_batch_quality(current, set(range(1000)), previous, config) == []


def test_thresholds_come_from_the_config():
    """A docket that genuinely swings can widen the tolerance instead of --force."""
    previous = [OPPOSE] * 940 + [SUPPORT] * 60
    current = [comment(i, OPPOSE) for i in range(940)] \
        + [comment(1000 + i, SUPPORT) for i in range(60)] \
        + [comment(5000 + i, SUPPORT) for i in range(100)]
    wide = {'quality_gate': {'max_batch_shift_pp': 100, 'max_corpus_shift_pp': 100}}
    assert check_batch_quality(current, set(range(2000)), previous, wide) == []


def test_a_first_run_with_no_previous_corpus_passes():
    """Nothing to compare against — the gate must not block a bootstrap."""
    current = [comment(i, OPPOSE) for i in range(500)]
    assert check_batch_quality(current, set(), [], {}) == []
