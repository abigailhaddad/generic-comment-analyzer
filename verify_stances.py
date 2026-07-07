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


# State and political prompts default to the generic module constants; they may
# be overridden by _load_prompts() from config.
STATE_VERIFICATION_PROMPT = _DEFAULT_STATE_VERIFICATION_PROMPT
POLITICAL_VERIFICATION_PROMPT = _DEFAULT_POLITICAL_VERIFICATION_PROMPT


def _load_prompts():
    """Load verification prompts from config into module globals.

    Stance and entity prompts are regulation-specific and REQUIRED. State and
    political prompts fall back to the generic module defaults if not provided.
    """
    global STANCE_VERIFICATION_PROMPT, ENTITY_VERIFICATION_PROMPT
    global STATE_VERIFICATION_PROMPT, POLITICAL_VERIFICATION_PROMPT

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


# Constrained response models — populated by _load_prompts() from the config so the
# verifier can only return valid values (mirrors the enum constraint on the main pass).
STANCE_VERIFICATION_MODEL = StanceVerification
ENTITY_VERIFICATION_MODEL = EntityVerification


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
