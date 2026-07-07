#!/usr/bin/env python3
"""
Second-pass verification of stance and entity type classifications.

Sends flagged comments to a stronger model for re-evaluation.
Configuration is driven by the second_pass section in analyzer_config.yaml.

Can be run standalone or called from pipeline.py.

Usage:
    python3 verify_stances.py full_run.parquet                    # verify and update in place
    python3 verify_stances.py full_run.parquet --output verified.parquet  # write to new file
"""

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter
from typing import List, Dict, Any

import yaml
import time

import pandas as pd
from dotenv import load_dotenv
import litellm
litellm.drop_params = True  # drop params a model does not support (e.g. temperature on GPT-5 reasoning models)
from enum import Enum
from pydantic import BaseModel, Field, create_model
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from attachment_utils import reextract_attachment_text

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MAX_RETRIES = 4
RETRY_BACKOFF = [1, 2, 4, 8]


def _retry_on_rate_limit(fn, *args, **kwargs):
    """Retry a function call with backoff on rate limit errors."""
    for attempt in range(MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if '429' in str(e) or 'RESOURCE_EXHAUSTED' in str(e):
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF[attempt]
                    time.sleep(wait)
                    continue
            raise
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries")


def load_second_pass_config():
    """Load second-pass verification config from analyzer_config.yaml.

    Reads from the current working directory because the pipeline chdirs into
    the per-regulation directory before running.
    """
    if os.path.exists('analyzer_config.yaml'):
        with open('analyzer_config.yaml') as f:
            config = yaml.safe_load(f)
        return config.get('second_pass', {})
    return {}


# Regulation-specific prompts are loaded from analyzer_config.yaml (second_pass.prompts).
# These are populated by _load_prompts() at runtime.
STANCE_VERIFICATION_PROMPT = None
ENTITY_VERIFICATION_PROMPT = None


# Generic module-level defaults (regulation-agnostic). Used as fallbacks if the
# config does not provide state/political prompts.
_DEFAULT_STATE_VERIFICATION_PROMPT = """You are verifying a state classification for a public comment submitter. The original classification was based on weak evidence (the supporting quote was not found in the comment text itself).

Original state: {state}
Original quote: "{quote}"
Submitter name: "{submitter}"

Your job: determine if the commenter's US state can be identified FROM THE COMMENT TEXT ALONE.

Rules:
- A person's first name (e.g. "Virginia", "Carolina") is NOT evidence of their state
- Mentioning a state-level organization does NOT mean the commenter is from that state unless they say so
- The state must be clearly indicated in the text (e.g. "I live in Texas", "as a California attorney", city/state like "Portland, OR")
- If no state can be determined from the text, return empty string
- If the state IS in the text, return the correct two-letter abbreviation"""


_DEFAULT_POLITICAL_VERIFICATION_PROMPT = """You are verifying the political affiliation classification of a public comment submitter.

Original classification: {affiliation}
Original quote: "{quote}"

Your job: determine if the commenter EXPLICITLY SELF-IDENTIFIES their political party in the comment text.

Rules:
- ONLY classify if the commenter says something like "I am a Republican", "as a registered Democrat", "lifelong Independent", "I'm a Libertarian"
- Mentioning a party does NOT count: "Republicans won't be in charge forever" is NOT self-identification
- Criticizing or praising a party does NOT count: "Pam is a shill for Trump" does NOT make them Republican
- "Conservative" does NOT automatically mean Republican
- "Liberal" does NOT automatically mean Democrat
- "Registered voter" does NOT mean Independent
- If no explicit self-identification, return empty string for both fields"""


_DEFAULT_COSIGNER_SPAN_PROMPT = """You are checking whether a public comment is a joint/coalition letter signed by multiple distinct individuals or organizations (as opposed to a single submitter).

Rules:
- has_cosigners is true ONLY if the letter's OWN closing/signature section names 2+ DISTINCT PEOPLE or 2+ DISTINCT ORGANIZATIONS as signers or endorsers of THIS COMMENT — not people or organizations merely discussed, cited, or assessed somewhere in the body text.
- A single person's own contact block (their name, followed by their title, followed by their organization/affiliation) is ONE signer, even though it spans several lines — do NOT count title or organization lines as separate signers.
- Do NOT count: a sentence in the body citing what other organizations said/assessed/authored (e.g. "X, Y, and Z jointly assessed that..." or "a brief authored by X, Y, and Z") — that is a citation, not this letter's signature block. Do NOT count the author byline, references, or contributor notes of an attached research paper/article — a comment that attaches a multi-author paper for support is still a single submitter's comment.
- Formal boilerplate like "the undersigned submits this comment" used by a SINGLE person does NOT count — that person writing in formal/plural style alone is not a coalition letter.
- When in doubt, prefer has_cosigners=false. This should only be true when you can point to actual distinct names, each clearly signing on their own behalf, in the letter's own signature section.
- If has_cosigners is true, extract two VERBATIM quotes that bound the ENTIRE section listing the letter's own distinct signers: block_start (the first signer's name) and block_end (the LAST signer's own name or their final title/org line — re-check the text after your first guess for block_end to make sure no further distinct signer appears after it; if one does, extend block_end to cover them too). Both must be exact substrings of the provided text. Do not let the span extend into body prose, an attached paper, or citations.
- If has_cosigners is false, leave block_start and block_end empty."""


# State and political prompts default to the generic module constants; they may
# be overridden by _load_prompts() from config.
STATE_VERIFICATION_PROMPT = _DEFAULT_STATE_VERIFICATION_PROMPT
POLITICAL_VERIFICATION_PROMPT = _DEFAULT_POLITICAL_VERIFICATION_PROMPT
COSIGNER_SPAN_PROMPT = _DEFAULT_COSIGNER_SPAN_PROMPT


def _load_prompts():
    """Load verification prompts from config into module globals.

    Stance and entity prompts are regulation-specific and REQUIRED. State and
    political prompts fall back to the generic module defaults if not provided.
    """
    global STANCE_VERIFICATION_PROMPT, ENTITY_VERIFICATION_PROMPT
    global STATE_VERIFICATION_PROMPT, POLITICAL_VERIFICATION_PROMPT, COSIGNER_SPAN_PROMPT

    config = load_second_pass_config()
    prompts = config.get('prompts', {}) or {}

    stance = prompts.get('stance')
    entity = prompts.get('entity')
    if not stance or not entity:
        raise ValueError(
            "second_pass.prompts.stance and .entity are required in analyzer_config.yaml"
        )

    STANCE_VERIFICATION_PROMPT = stance
    ENTITY_VERIFICATION_PROMPT = entity
    STATE_VERIFICATION_PROMPT = prompts.get('state') or _DEFAULT_STATE_VERIFICATION_PROMPT
    POLITICAL_VERIFICATION_PROMPT = prompts.get('political') or _DEFAULT_POLITICAL_VERIFICATION_PROMPT
    COSIGNER_SPAN_PROMPT = prompts.get('cosigner') or _DEFAULT_COSIGNER_SPAN_PROMPT

    # Build constrained response models so the verifier can only emit valid values.
    global STANCE_VERIFICATION_MODEL, ENTITY_VERIFICATION_MODEL
    stance_enum = Enum("VStanceEnum", {"Oppose": "Oppose", "Support": "Support", "Unclear": "Unclear"}, type=str)
    STANCE_VERIFICATION_MODEL = create_model(
        "ConstrainedStanceVerification", __base__=StanceVerification,
        verified_stance=(stance_enum, Field(description="Exactly one of: Oppose, Support, Unclear.")),
    )
    entity_types = _load_full_config().get('entity_types', []) or []
    if "Individual/Other" not in entity_types:
        entity_types = entity_types + ["Individual/Other"]
    if entity_types:
        entity_enum = Enum("VEntityEnum", {f"E{i}": v for i, v in enumerate(entity_types)}, type=str)
        ENTITY_VERIFICATION_MODEL = create_model(
            "ConstrainedEntityVerification", __base__=EntityVerification,
            verified_entity_type=(entity_enum, Field(description="Exactly one of the allowed entity types.")),
        )
    else:
        ENTITY_VERIFICATION_MODEL = EntityVerification


class PoliticalVerification(BaseModel):
    verified_affiliation: str = Field(
        description="Political party (Republican, Democrat, Independent, Libertarian) or empty string if no explicit self-identification"
    )
    reasoning: str = Field(
        description="One sentence explaining the verification"
    )


class StanceVerification(BaseModel):
    verified_stance: str = Field(
        description="One of: Oppose, Support, Unclear"
    )
    reasoning: str = Field(
        description="One sentence explaining the classification"
    )


class EntityVerification(BaseModel):
    verified_entity_type: str = Field(
        description="The correct entity type classification"
    )
    reasoning: str = Field(
        description="One sentence explaining the verification"
    )


class CosignerSpanVerification(BaseModel):
    has_cosigners: bool = Field(
        description="True only if this comment is jointly signed by 2+ distinct individuals or organizations."
    )
    block_start: str = Field(
        default="",
        description="Verbatim quote marking the start of the signer-list section. Empty if has_cosigners is false."
    )
    block_end: str = Field(
        default="",
        description="Verbatim quote marking the end of the signer-list section. Empty if has_cosigners is false."
    )
    reasoning: str = Field(
        description="One sentence explaining the determination."
    )


# Constrained response models — populated by _load_prompts() from the config so the
# verifier can only return valid values (mirrors the enum constraint on the main pass).
STANCE_VERIFICATION_MODEL = StanceVerification
ENTITY_VERIFICATION_MODEL = EntityVerification
COSIGNER_SPAN_MODEL = CosignerSpanVerification


def _load_full_config():
    """Load the full analyzer_config.yaml from the current working directory."""
    if os.path.exists('analyzer_config.yaml'):
        with open('analyzer_config.yaml') as f:
            return yaml.safe_load(f) or {}
    return {}


class StateVerification(BaseModel):
    verified_state: str = Field(
        description="Two-letter state abbreviation, or empty string if no state can be determined from the comment text"
    )
    reasoning: str = Field(
        description="One sentence explaining the verification"
    )


def _safe_stances_list(analysis):
    """Get stances as a plain Python list from analysis dict."""
    stances = analysis.get('stances', [])
    if hasattr(stances, 'tolist'):
        stances = stances.tolist()
    if not isinstance(stances, list):
        stances = []
    return stances


def find_ambiguous_comments(comments: List[Dict[str, Any]], config: dict) -> List[tuple]:
    """Find comments that need stance verification based on config."""
    stance_config = config.get('stance', {})
    trigger_stances = stance_config.get('trigger_stances', ['Support', 'Unclear'])
    verify_short = stance_config.get('also_verify_short_oppose', True)
    short_threshold = stance_config.get('short_threshold_chars', 200)

    ambiguous = []
    for i, comment in enumerate(comments):
        analysis = comment.get('analysis')
        if not analysis or not isinstance(analysis, dict):
            continue

        # Skip if already verified
        if analysis.get('verified_stance'):
            continue

        stances = _safe_stances_list(analysis)
        has_support = any('Support' in s for s in stances if isinstance(s, str))
        has_oppose = any('Oppose' in s for s in stances if isinstance(s, str))

        # Trigger on configured stances
        triggered = False
        if has_support and 'Support' in trigger_stances:
            triggered = True
        if not has_oppose and not has_support and 'Unclear' in trigger_stances:
            triggered = True

        if triggered:
            ambiguous.append((i, comment))
            continue

        # Short oppose check
        if verify_short and has_oppose:
            text = (comment.get('comment_text') or comment.get('text') or '').strip()
            if len(text) < short_threshold:
                ambiguous.append((i, comment))

    return ambiguous


def find_entity_verify_comments(comments: List[Dict[str, Any]], config: dict) -> List[tuple]:
    """Find comments that need entity type verification based on config."""
    entity_config = config.get('entity_type', {})
    trigger_types = entity_config.get('trigger_types', [])

    verify_attorney_quote_mismatch = entity_config.get('verify_attorney_on_quote_mismatch', False)

    if not trigger_types and not verify_attorney_quote_mismatch:
        return []

    candidates = []
    for i, comment in enumerate(comments):
        analysis = comment.get('analysis')
        if not analysis or not isinstance(analysis, dict):
            continue

        # Skip if already verified
        if analysis.get('verified_entity_type'):
            continue

        entity_type = analysis.get('entity_type', '')
        if entity_type in trigger_types:
            candidates.append((i, comment))
            continue

        # Check Attorney/Lawyer: verify ALL attorneys that lack strong self-ID
        if verify_attorney_quote_mismatch and entity_type == 'Attorney/Lawyer':
            text = (comment.get('comment_text') or comment.get('text', '')).lower()
            # Only skip verification if there's a clear self-identification
            strong_self_id = re.search(
                r'i am (?:a |an )?(?:concerned )?(?:citizen[, ]+ (?:and )?(?:a )?)?(?:attorney|lawyer)'
                r'|as (?:a |an )?(?:former |retired )?(?:attorney|lawyer)'
                r'|licensed (?:attorney|to practice)'
                r'|member of (?:the |a )?(?:\w+ )?bar'
                r'|admitted to (?:the |a )?bar'
                r'|practicing (?:attorney|lawyer)'
                r'|i practice law|law degree|juris doctor|j\.d\.|esq\b|passed the bar|barred in'
                r'|my license to practice'
                r'|(?:former |retired )?(?:prosecutor|AUSA|assistant u\.?s\.? attorney)'
                r'|(?:concerned |retired |former )?(?:attorney|lawyer) who',
                text
            )
            if not strong_self_id:
                candidates.append((i, comment))

    return candidates


# Used inside _parse_cosigner_block(): letters that state their own signer count
# (e.g. "323 multi-sector organizations") repeat it reliably even when the named
# list itself is long or noisily extracted, so it's preferred over len(names).
_STATED_COUNT_RE = re.compile(
    r'(\d[\d,]*)\+?\s+(?:multi-sector\s+|other\s+|additional\s+)?'
    r'(?:organizations?|signator(?:y|ies)|individuals?|co-?signers?|cosigners?|senators?|members?|entities)',
    re.IGNORECASE,
)

_JUNK_LINE_RE = re.compile(r'^(page\s*)?\d+(\s*of\s*\d+)?$', re.IGNORECASE)
# Page-bottom footnote/citation lines (e.g. "4 Id. at p. 10.") that PDF extraction
# can interleave with list items when a footnote falls on the same page.
_FOOTNOTE_LINE_RE = re.compile(r'^\d{1,3}\s+\S')
# Letter-closing valedictions immediately preceding a signature block.
_VALEDICTION_RE = re.compile(
    r'^(sincerely|very truly yours|yours truly|respectfully( submitted)?|regards|best regards|cordially)[,.]?$',
    re.IGNORECASE,
)
# Case-law citation lines (e.g. "Council v. Department of Agriculture (2025)")
# that can be interleaved in a signature block via a footnote.
_CASE_CITATION_RE = re.compile(r'\sv\.\s.+\(\d{4}\)$')


def _has_repeated_short_line(text: str, min_len: int = 4, max_len: int = 60, min_repeats: int = 2) -> bool:
    """Structural signal for a signature block: some short line (a shared title
    like "United States Senator", or a repeated running header) appears 2+ times
    verbatim. Unlike phrase matching, this doesn't depend on the letter using any
    particular wording ("undersigned", "joint letter", etc.) — it caught the
    motivating Hickenlooper letter, which uses neither phrase.
    """
    lines = [re.sub(r'[ \t]+', ' ', l).strip() for l in text.splitlines()]
    lines = [l for l in lines if min_len <= len(l) <= max_len and not l.isdigit()]
    counts = Counter(l.lower() for l in lines)
    return any(c >= min_repeats for c in counts.values())


def find_cosigner_span_comments(comments: List[Dict[str, Any]], config: dict) -> List[tuple]:
    """Find comments that look like joint/coalition letters and need cosigner-span detection.

    Combines a config-driven phrase list (catches explicit wording like "the
    undersigned" or a stated total) with a generic structural check (catches
    signature blocks that use neither, like a plain list of names sharing a
    title). Both are cheap; false positives just cost one extra model call that
    resolves to has_cosigners=false.
    """
    cosigner_config = config.get('cosigner_span')
    if cosigner_config is None:
        return []
    patterns = cosigner_config.get('trigger_patterns', [])
    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]

    candidates = []
    for i, comment in enumerate(comments):
        analysis = comment.get('analysis')
        if not analysis or not isinstance(analysis, dict):
            continue
        if analysis.get('cosigner_checked'):
            continue
        text = comment.get('text') or ''
        if any(p.search(text) for p in compiled) or _has_repeated_short_line(text):
            candidates.append((i, comment))
    return candidates


def _find_quote(full_text: str, quote: str, start_from: int = 0, prefer_last: bool = False) -> tuple:
    """Locate `quote` in full_text, returning (start_index, match_length), or
    (-1, 0) if not found. Falls back to a whitespace-flexible match — PDF
    extraction sometimes inserts a line break where a quote has a plain space
    (or vice versa), which breaks an exact substring search even though the
    quote is otherwise verbatim.

    prefer_last=True returns the LAST occurrence instead of the first — used
    for the block_start quote, since a signature block sits near the end of a
    letter and a short phrase from it can coincidentally also appear earlier
    in the body (e.g. a sentence quoting a signer's own affiliation).
    """
    search_space = full_text[start_from:]
    if prefer_last:
        idx = search_space.rfind(quote)
    else:
        idx = search_space.find(quote)
    if idx != -1:
        return start_from + idx, len(quote)

    words = quote.split()
    if not words:
        return -1, 0
    pattern = r'\s+'.join(re.escape(w) for w in words)
    matches = list(re.finditer(pattern, search_space))
    if not matches:
        return -1, 0
    m = matches[-1] if prefer_last else matches[0]
    return start_from + m.start(), len(m.group(0))


def _extend_block_end(full_text: str, start_idx: int, end_idx: int,
                       lookahead: int = 500, max_rounds: int = 5) -> int:
    """Grow end_idx forward when a line just past it exactly repeats a line
    already inside [start_idx:end_idx).

    The model's block_end quote sometimes stops at the first signer's entry in
    a small, densely-formatted (no blank-line-separated) list, even though a
    later signer follows immediately — often sharing a repeated line, like a
    common organization name, that only becomes a second occurrence once the
    later entry is included. That repeat is the same signal _dense_parse()
    already uses to drop boilerplate; used prospectively here, it's evidence
    the signature block continues rather than evidence of a duplicate to drop.
    Bounded by `lookahead` per round so it can't run away into unrelated text.
    """
    def _norm(line: str) -> str:
        return re.sub(r'[ \t]+', ' ', line).strip().lower()

    seen = {_norm(l) for l in full_text[start_idx:end_idx].splitlines() if _norm(l)}

    for _ in range(max_rounds):
        window = full_text[end_idx:end_idx + lookahead]
        offset = 0
        extended_by = None
        for line in window.splitlines(keepends=True):
            offset += len(line)
            if _norm(line) in seen:
                extended_by = offset
                break
        if extended_by is None:
            break
        end_idx += extended_by
        seen.update(_norm(l) for l in full_text[start_idx:end_idx].splitlines() if _norm(l))

    return end_idx


def _slice_cosigner_block(full_text: str, start_quote: str, end_quote: str) -> str:
    """Return the raw substring of full_text between two verbatim quotes (inclusive)."""
    if not start_quote or not end_quote:
        return ''
    start_idx, _ = _find_quote(full_text, start_quote, prefer_last=True)
    if start_idx == -1:
        return ''
    end_idx, end_len = _find_quote(full_text, end_quote, start_idx)
    if end_idx == -1:
        return ''
    end_idx = _extend_block_end(full_text, start_idx, end_idx + end_len)
    return full_text[start_idx:end_idx]


def _clean_lines(text: str) -> List[str]:
    """Normalize and drop junk/footnote/valediction/citation lines from a chunk of text."""
    lines = [re.sub(r'[ \t]+', ' ', l).strip() for l in text.splitlines()]
    return [
        l for l in lines
        if l
        and not _JUNK_LINE_RE.match(l)
        and not _FOOTNOTE_LINE_RE.match(l)
        and not _VALEDICTION_RE.match(l)
        and not _CASE_CITATION_RE.search(l)
    ]


def _dense_parse(block: str) -> List[str]:
    """Treat every non-junk line as a candidate signer, dropping lines that
    repeat verbatim within the block (a shared title like "United States
    Senator", or a running header/footer on a multi-page attachment) — a real
    signer's own name appears exactly once. Works well for large lists where
    many signers share identical boilerplate.
    """
    raw_lines = _clean_lines(block)
    line_counts = Counter(l.lower() for l in raw_lines)

    names = []
    seen = set()
    for l in raw_lines:
        key = l.lower()
        if line_counts[key] > 1:
            continue  # repeated boilerplate, not a unique signer
        if key not in seen:
            seen.add(key)
            names.append(l)
    return names


def _chunked_parse(block: str) -> List[str]:
    """Treat each blank-line-separated chunk as one signer's multi-line entry
    (name, then title/org details) and take only its first line. More reliable
    than _dense_parse for a handful of signers whose titles/orgs don't repeat,
    but only applies when the letter actually uses blank lines between entries.
    """
    names = []
    for chunk in re.split(r'\n\s*\n', block):
        lines = _clean_lines(chunk)
        if lines:
            names.append(lines[0])
    return names


def _parse_cosigner_block(block: str) -> tuple:
    """Parse a signer-list span into (names, count) using plain text heuristics.

    Tries blank-line-separated chunking first (each chunk = one signer's
    multi-line entry) since it's unambiguous when present; falls back to dense
    line-based parsing otherwise. The count prefers an explicitly stated total
    (e.g. "323 multi-sector organizations") over the parsed name count, since
    large coalition letters state their own count and that is more reliable
    than re-deriving it from noisy extracted text.
    """
    if not block:
        return [], 0

    stated = _STATED_COUNT_RE.search(block)

    dense_names = _dense_parse(block)

    # Chunking is only attempted for small blocks. Large multi-page lists (tens
    # to hundreds of signers) sometimes contain incidental blank lines from page
    # breaks; treating those as entry boundaries collapses many real signers
    # into a handful of chunks, which is far worse than the dense fallback's
    # minor over-counting. Small letters (a handful of signers, each spanning
    # several lines) are exactly the case chunking helps, and have little room
    # for a page-break false boundary to matter.
    names = dense_names
    if len(dense_names) <= 15:
        chunk_names = _chunked_parse(block)
        if len(chunk_names) >= 2 and len(chunk_names) <= len(dense_names):
            names = chunk_names

    count = int(stated.group(1).replace(',', '')) if stated else len(names)
    return names, count


def find_state_verify_comments(comments: List[Dict[str, Any]], config: dict) -> List[tuple]:
    """Find comments where state classification needs verification."""
    state_config = config.get('state', {})
    verify_all = state_config.get('verify_all', False)
    quote_mismatch = state_config.get('trigger_on_quote_mismatch', False)

    if not verify_all and not quote_mismatch:
        return []

    candidates = []
    for i, comment in enumerate(comments):
        analysis = comment.get('analysis')
        if not analysis or not isinstance(analysis, dict):
            continue

        state = analysis.get('state_identified', '')
        if not state:
            continue

        # Skip if already verified
        if analysis.get('verified_state') is not None:
            continue

        if verify_all:
            candidates.append((i, comment))
        elif quote_mismatch:
            quote = (analysis.get('state_quote', '') or '').lower()
            text = (comment.get('text', '')).lower()
            if quote and quote not in text:
                candidates.append((i, comment))

    return candidates


def find_political_verify_comments(comments: List[Dict[str, Any]], config: dict) -> List[tuple]:
    """Find comments that need political affiliation verification."""
    pol_config = config.get('political_affiliation', {})
    if not pol_config.get('verify_all', False):
        return []

    candidates = []
    for i, comment in enumerate(comments):
        analysis = comment.get('analysis')
        if not analysis or not isinstance(analysis, dict):
            continue
        if analysis.get('verified_political') is not None:
            continue
        if analysis.get('political_affiliation'):
            candidates.append((i, comment))

    return candidates


def verify_single_political(model, comment_text, affiliation, quote, submitter=''):
    """Verify a single comment's political affiliation."""
    prompt = POLITICAL_VERIFICATION_PROMPT.format(
        affiliation=affiliation,
        quote=quote,
    )

    parts = []
    if submitter:
        parts.append(f"Submitter: {submitter}")
    parts.append(comment_text)
    combined = "\n".join(parts)

    resp = litellm.completion(
        model=model,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"Verify the political affiliation for this comment:\n\n{combined}"},
        ],
        response_format=PoliticalVerification,
        temperature=0.0,
    )
    return json.loads(resp.choices[0].message.content)


def verify_single_state(model, comment_text, state, quote, submitter=''):
    """Verify a single comment's state classification."""
    prompt = STATE_VERIFICATION_PROMPT.format(
        state=state,
        quote=quote,
        submitter=submitter,
    )

    resp = litellm.completion(
        model=model,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"Verify the state classification for this comment:\n\n{comment_text}"},
        ],
        response_format=StateVerification,
        temperature=0.0,
    )
    return json.loads(resp.choices[0].message.content)


def verify_single_stance(model, comment_text, submitter='', organization=''):
    """Verify a single comment's stance."""
    parts = []
    if submitter:
        parts.append(f"Submitter: {submitter}")
    if organization:
        parts.append(f"Organization: {organization}")
    parts.append(comment_text)
    combined = "\n".join(parts)

    resp = litellm.completion(
        model=model,
        messages=[
            {"role": "system", "content": STANCE_VERIFICATION_PROMPT},
            {"role": "user", "content": f"Classify this comment:\n\n{combined}"},
        ],
        response_format=STANCE_VERIFICATION_MODEL,
        temperature=0.0,
    )
    return json.loads(resp.choices[0].message.content)


def verify_single_entity(model, comment_text, entity_type, entity_name,
                          submitter='', organization=''):
    """Verify a single comment's entity type classification."""
    parts = []
    if submitter:
        parts.append(f"Submitter: {submitter}")
    if organization:
        parts.append(f"Organization: {organization}")
    parts.append(comment_text)
    combined = "\n".join(parts)

    prompt = ENTITY_VERIFICATION_PROMPT.format(
        entity_type=entity_type,
        entity_name=entity_name,
    )

    resp = litellm.completion(
        model=model,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"Verify the entity type classification for this comment:\n\n{combined}"},
        ],
        response_format=ENTITY_VERIFICATION_MODEL,
        temperature=0.0,
    )
    return json.loads(resp.choices[0].message.content)


# Head+tail cap for the cosigner-span prompt. Unlike the other verify_single_*
# calls (truncated to 2000 chars from the front), this one needs the END of long
# attachments too, since signature blocks usually appear there — but a handful of
# outlier attachments in this corpus run to 750K+ chars (e.g. someone attaching an
# entire Federal Register document), so it still needs an upper bound.
_COSIGNER_HEAD_CHARS = 15000
_COSIGNER_TAIL_CHARS = 45000


def verify_single_cosigner_span(model, comment_text, submitter='', organization=''):
    """Detect whether a comment is a joint/coalition letter and locate its signer-list span."""
    cap = _COSIGNER_HEAD_CHARS + _COSIGNER_TAIL_CHARS
    if len(comment_text) > cap:
        comment_text = (
            comment_text[:_COSIGNER_HEAD_CHARS]
            + "\n\n[...TRUNCATED FOR LENGTH...]\n\n"
            + comment_text[-_COSIGNER_TAIL_CHARS:]
        )

    parts = []
    if submitter:
        parts.append(f"Submitter: {submitter}")
    if organization:
        parts.append(f"Organization: {organization}")
    parts.append(comment_text)
    combined = "\n".join(parts)

    resp = litellm.completion(
        model=model,
        messages=[
            {"role": "system", "content": COSIGNER_SPAN_PROMPT},
            {"role": "user", "content": f"Analyze this comment for joint/coalition signers:\n\n{combined}"},
        ],
        response_format=COSIGNER_SPAN_MODEL,
        temperature=0.0,
    )
    return json.loads(resp.choices[0].message.content)


def verify_stances(comments: List[Dict[str, Any]], model: str = None,
                   max_workers: int = None) -> List[Dict[str, Any]]:
    """Run second-pass verification on stances and entity types. Modifies comments in place."""
    _load_prompts()

    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.warning("No OPENAI_API_KEY — skipping verification")
        return comments

    config = load_second_pass_config()
    model = model or config.get('model', 'gpt-5.4-mini')
    max_workers = max_workers or config.get('max_workers', 8)

    # --- Stance verification ---
    ambiguous = find_ambiguous_comments(comments, config)
    if ambiguous:
        logger.info(f"Verifying {len(ambiguous)} ambiguous stances with {model}")
        verified_count = {'Oppose': 0, 'Support': 0, 'Unclear': 0, 'error': 0}

        def process_stance(idx_comment):
            idx, comment = idx_comment
            text = (comment.get('text') or '')[:2000]
            submitter = comment.get('submitter', '')
            org = comment.get('organization', '')
            try:
                result = _retry_on_rate_limit(verify_single_stance, model, text, submitter, org)
                return idx, result
            except Exception as e:
                logger.warning(f"Stance verification failed for {comment.get('id', '?')}: {e}")
                return idx, None

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_stance, item): item for item in ambiguous}
            for future in tqdm(as_completed(futures), total=len(ambiguous),
                               desc="Verifying stances", unit="comment"):
                idx, result = future.result()
                if result is None:
                    verified_count['error'] += 1
                    continue

                stance = result.get('verified_stance', 'Unclear')
                reasoning = result.get('reasoning', '')
                verified_count[stance] = verified_count.get(stance, 0) + 1

                analysis = comments[idx].get('analysis')
                if analysis and isinstance(analysis, dict):
                    analysis['verified_stance'] = stance
                    analysis['verification_reasoning'] = reasoning

                    stances = _safe_stances_list(analysis)
                    stances = [s for s in stances if not s.startswith('Position:')]
                    if stance == 'Oppose':
                        stances.insert(0, 'Position: Oppose the proposed rule')
                    elif stance == 'Support':
                        stances.insert(0, 'Position: Support the proposed rule')
                    analysis['stances'] = stances

        logger.info(f"Stance verification results: {dict(verified_count)}")
    else:
        logger.info("No ambiguous stances to verify")

    # --- Entity type verification ---
    entity_candidates = find_entity_verify_comments(comments, config)
    if entity_candidates:
        logger.info(f"Verifying {len(entity_candidates)} entity type classifications with {model}")
        entity_count = {'changed': 0, 'confirmed': 0, 'error': 0}

        def process_entity(idx_comment):
            idx, comment = idx_comment
            text = (comment.get('text') or '')[:2000]
            analysis = comment.get('analysis', {})
            entity_type = analysis.get('entity_type', '')
            entity_name = analysis.get('entity_name', '')
            submitter = comment.get('submitter', '')
            org = comment.get('organization', '')
            try:
                result = _retry_on_rate_limit(verify_single_entity, model, text, entity_type, entity_name,
                                               submitter, org)
                return idx, result, entity_type
            except Exception as e:
                logger.warning(f"Entity verification failed for {comment.get('id', '?')}: {e}")
                return idx, None, entity_type

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_entity, item): item for item in entity_candidates}
            for future in tqdm(as_completed(futures), total=len(entity_candidates),
                               desc="Verifying entity types", unit="comment"):
                idx, result, original_type = future.result()
                if result is None:
                    entity_count['error'] += 1
                    continue

                new_type = result.get('verified_entity_type', original_type)
                reasoning = result.get('reasoning', '')

                analysis = comments[idx].get('analysis')
                if analysis and isinstance(analysis, dict):
                    analysis['verified_entity_type'] = new_type
                    analysis['entity_verification_reasoning'] = reasoning

                    if new_type != original_type:
                        entity_count['changed'] += 1
                        logger.info(f"  {comments[idx].get('id')}: {original_type} -> {new_type} ({reasoning})")
                        analysis['entity_type'] = new_type
                    else:
                        entity_count['confirmed'] += 1

        logger.info(f"Entity verification results: {dict(entity_count)}")
    else:
        logger.info("No entity types to verify")

    # --- State verification ---
    state_candidates = find_state_verify_comments(comments, config)
    if state_candidates:
        logger.info(f"Verifying {len(state_candidates)} state classifications with {model}")
        state_count = {'changed': 0, 'confirmed': 0, 'cleared': 0, 'error': 0}

        def process_state(idx_comment):
            idx, comment = idx_comment
            text = (comment.get('text') or '')[:2000]
            analysis = comment.get('analysis', {})
            state = analysis.get('state_identified', '')
            quote = analysis.get('state_quote', '')
            submitter = comment.get('submitter', '')
            try:
                result = _retry_on_rate_limit(verify_single_state, model, text, state, quote, submitter)
                return idx, result, state
            except Exception as e:
                logger.warning(f"State verification failed for {comment.get('id', '?')}: {e}")
                return idx, None, state

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_state, item): item for item in state_candidates}
            for future in tqdm(as_completed(futures), total=len(state_candidates),
                               desc="Verifying states", unit="comment"):
                idx, result, original_state = future.result()
                if result is None:
                    state_count['error'] += 1
                    continue

                new_state = result.get('verified_state', '').strip()
                reasoning = result.get('reasoning', '')

                analysis = comments[idx].get('analysis')
                if analysis and isinstance(analysis, dict):
                    analysis['verified_state'] = new_state
                    analysis['state_verification_reasoning'] = reasoning

                    if new_state != original_state:
                        if not new_state:
                            state_count['cleared'] += 1
                        else:
                            state_count['changed'] += 1
                        logger.info(f"  {comments[idx].get('id')}: {original_state} -> {new_state or '(none)'} ({reasoning})")
                        analysis['state_identified'] = new_state
                        if not new_state:
                            analysis['state_quote'] = ''
                    else:
                        state_count['confirmed'] += 1

        logger.info(f"State verification results: {dict(state_count)}")
    else:
        logger.info("No state classifications to verify")

    # --- Political affiliation verification ---
    pol_candidates = find_political_verify_comments(comments, config)
    if pol_candidates:
        logger.info(f"Verifying {len(pol_candidates)} political affiliation classifications with {model}")
        pol_count = {'changed': 0, 'confirmed': 0, 'cleared': 0, 'error': 0}

        def process_political(idx_comment):
            idx, comment = idx_comment
            text = (comment.get('text') or '')[:2000]
            analysis = comment.get('analysis', {})
            affiliation = analysis.get('political_affiliation', '')
            quote = analysis.get('political_affiliation_quote', '')
            submitter = comment.get('submitter', '')
            try:
                result = _retry_on_rate_limit(verify_single_political, model, text, affiliation, quote, submitter)
                return idx, result, affiliation
            except Exception as e:
                logger.warning(f"Political verification failed for {comment.get('id', '?')}: {e}")
                return idx, None, affiliation

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_political, item): item for item in pol_candidates}
            for future in tqdm(as_completed(futures), total=len(pol_candidates),
                               desc="Verifying political", unit="comment"):
                idx, result, original_affiliation = future.result()
                if result is None:
                    pol_count['error'] += 1
                    continue

                new_affiliation = result.get('verified_affiliation', '').strip()
                reasoning = result.get('reasoning', '')

                analysis = comments[idx].get('analysis')
                if analysis and isinstance(analysis, dict):
                    analysis['verified_political'] = new_affiliation
                    analysis['political_verification_reasoning'] = reasoning

                    if new_affiliation != original_affiliation:
                        if not new_affiliation:
                            pol_count['cleared'] += 1
                        else:
                            pol_count['changed'] += 1
                        logger.info(f"  {comments[idx].get('id')}: {original_affiliation} -> {new_affiliation or '(none)'} ({reasoning})")
                        analysis['political_affiliation'] = new_affiliation
                        if not new_affiliation:
                            analysis['political_affiliation_quote'] = ''
                    else:
                        pol_count['confirmed'] += 1

        logger.info(f"Political verification results: {dict(pol_count)}")
    else:
        logger.info("No political affiliations to verify")

    # --- Cosigner span detection (joint/coalition letters) ---
    cosigner_candidates = find_cosigner_span_comments(comments, config)
    if cosigner_candidates:
        logger.info(f"Checking {len(cosigner_candidates)} comments for cosigners with {model}")
        cosigner_stats = {'found': 0, 'none': 0, 'error': 0}

        def process_cosigner(idx_comment):
            idx, comment = idx_comment
            comment_id = comment.get('id', '')

            # Pick up extractor fixes (e.g. the PyPDF2 -> PyMuPDF swap) for this
            # comment's attachment before detecting the signer block, so the
            # verbatim quotes the model returns match the text we'll slice below.
            refreshed = reextract_attachment_text(comment_id)
            if refreshed is not None:
                comment_text = comment.get('comment_text', '') or ''
                if refreshed and comment_text:
                    full_text = f"{comment_text}\n\n--- ATTACHMENT CONTENT ---\n{refreshed}"
                else:
                    full_text = refreshed or comment_text
                comment['text'] = full_text
                comment['attachment_text'] = refreshed

            text = comment.get('text') or ''
            submitter = comment.get('submitter', '')
            org = comment.get('organization', '')
            try:
                result = _retry_on_rate_limit(verify_single_cosigner_span, model, text, submitter, org)
                return idx, result, text
            except Exception as e:
                logger.warning(f"Cosigner detection failed for {comment_id}: {e}")
                return idx, None, text

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_cosigner, item): item for item in cosigner_candidates}
            for future in tqdm(as_completed(futures), total=len(cosigner_candidates),
                               desc="Checking cosigners", unit="comment"):
                idx, result, full_text = future.result()
                analysis = comments[idx].get('analysis')
                if not analysis or not isinstance(analysis, dict):
                    continue

                analysis['cosigner_checked'] = True
                analysis['cosigner_has_flag'] = False
                analysis['cosigner_names'] = []
                analysis['cosigner_count'] = 1

                if result is None:
                    cosigner_stats['error'] += 1
                    continue

                analysis['cosigner_reasoning'] = result.get('reasoning', '')

                if not result.get('has_cosigners'):
                    cosigner_stats['none'] += 1
                    continue

                analysis['cosigner_has_flag'] = True

                block = _slice_cosigner_block(full_text, result.get('block_start', ''), result.get('block_end', ''))
                if not block:
                    logger.warning(f"  {comments[idx].get('id')}: has_cosigners=True but block quotes did not match text")
                    cosigner_stats['error'] += 1
                    continue

                names, count = _parse_cosigner_block(block)
                analysis['cosigner_names'] = names
                analysis['cosigner_count'] = max(count, 1)
                cosigner_stats['found'] += 1
                logger.info(f"  {comments[idx].get('id')}: {count} cosigners found")

        logger.info(f"Cosigner detection results: {dict(cosigner_stats)}")
    else:
        logger.info("No comments matched cosigner trigger patterns")

    # --- Save verification log ---
    log_entries = []
    for comment in comments:
        analysis = comment.get('analysis')
        if not analysis or not isinstance(analysis, dict):
            continue
        vs = analysis.get('verified_stance')
        vet = analysis.get('verified_entity_type')
        vst = analysis.get('verified_state')
        if vs or vet or vst is not None:
            stances = _safe_stances_list(analysis)
            log_entries.append({
                'id': comment.get('id', ''),
                'original_had_support': any('Support' in s for s in stances if isinstance(s, str)),
                'verified_stance': vs or '',
                'verification_reasoning': analysis.get('verification_reasoning', ''),
                'verified_entity_type': vet or '',
                'entity_verification_reasoning': analysis.get('entity_verification_reasoning', ''),
                'comment_preview': (comment.get('comment_text') or '')[:200],
            })

    if log_entries:
        log_df = pd.DataFrame(log_entries)
        log_path = 'stance_verification_log.csv'
        log_df.to_csv(log_path, index=False)
        logger.info(f"Saved verification log to {log_path} ({len(log_entries)} entries)")

    return comments


def main():
    parser = argparse.ArgumentParser(description='Second-pass verification of classifications')
    parser.add_argument('parquet', help='Input parquet file')
    parser.add_argument('--output', help='Output parquet file (default: update in place)')
    parser.add_argument('--model', help='Override model from config')
    parser.add_argument('--workers', type=int, help='Override parallel workers from config')
    args = parser.parse_args()

    _load_prompts()

    logger.info(f"Loading {args.parquet}")
    df = pd.read_parquet(args.parquet)
    comments = df.to_dict('records')

    comments = verify_stances(comments, model=args.model, max_workers=args.workers)

    output = args.output or args.parquet
    logger.info(f"Saving to {output}")
    pd.DataFrame(comments).to_parquet(output, index=False)

    # Print summary
    oppose = sum(1 for c in comments if any('Oppose' in s for s in _safe_stances_list(c.get('analysis') or {}) if isinstance(s, str)))
    support = sum(1 for c in comments if any('Support' in s for s in _safe_stances_list(c.get('analysis') or {}) if isinstance(s, str)))
    verified = sum(1 for c in comments if (c.get('analysis') or {}).get('verified_stance'))
    print(f"\nFinal: {oppose} oppose, {support} support, {verified} verified")


if __name__ == '__main__':
    main()
