#!/usr/bin/env python3
"""
Generate HTML Report from Comment Analysis Results

Creates an interactive HTML report with briefing summary and searchable table.
Uses Jinja2 template (report_template.html) for HTML generation.
"""

import argparse
import csv
import json
import math
import os
import re
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

import yaml
import pandas as pd
from jinja2 import Environment, FileSystemLoader

_SMALL_WORDS = {'the', 'of', 'a', 'an', 'and', 'or', 'to', 'in', 'on', 'for'}


def display_pct(count, total, decimals: int = 1) -> float:
    """Percentage for display, keeping at least one significant digit.

    Rounding to a fixed decimal turns a small-but-real share into 0.0, so a card
    showing a non-zero count alongside "0.0%" reads as a bug rather than as a
    rare category. Escalate precision until the first significant digit appears:
    42 of 167,864 renders as 0.03%, not 0.0%. Large shares are unaffected.
    """
    if not total or not count:
        return 0.0
    pct = count / total * 100
    d = decimals
    while round(pct, d) == 0 and d < 12:
        d += 1
    return round(pct, d)


def humanize_flag_label(key: str, flag_cfg: Dict[str, Any]) -> str:
    """Derive a display label for a regex flag.

    Precedence: explicit `label:` in the flag's config > humanized key (strip a
    leading ``mentions_``/``cites_`` and Title-Case the remainder). No
    regulation-specific names live in this generic generator — nice labels come
    from each flag's optional ``label:`` field in analyzer_config.yaml.
    """
    if isinstance(flag_cfg, dict) and flag_cfg.get('label'):
        return str(flag_cfg['label'])
    base = re.sub(r'^(mentions|cites)_', '', key)
    words = base.split('_')
    out = []
    for i, w in enumerate(words):
        if w.lower() in _SMALL_WORDS and i != 0:
            out.append(w.lower())
        else:
            out.append(w.capitalize())
    return ' '.join(out) if out else key


def extract_matching_sentence(text: str, patterns: List[str]) -> str:
    """Return the first sentence in ``text`` matching any of ``patterns``."""
    if not text or not patterns:
        return ''
    combined = '|'.join(f'(?:{p})' for p in patterns)
    sentences = re.split(r'(?<=[.!?])\s+', text)
    for sentence in sentences:
        try:
            if re.search(combined, sentence, re.IGNORECASE):
                return sentence.strip()
        except re.error:
            return ''
    return ''


def load_results(json_file: str) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Load analyzed comments from JSON file and return comments plus metadata."""
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        if isinstance(data, dict) and 'comments' in data:
            return data['comments'], data
        elif isinstance(data, list):
            return data, {}
        else:
            raise ValueError(f"Unexpected JSON format in {json_file}")


def load_results_parquet(parquet_file: str) -> List[Dict[str, Any]]:
    """Load analyzed comments from Parquet file."""
    import numpy as np
    df = pd.read_parquet(parquet_file)
    records = df.to_dict('records')
    for record in records:
        if 'analysis' in record and record['analysis']:
            analysis = record['analysis']
            if 'stances' in analysis and isinstance(analysis['stances'], np.ndarray):
                analysis['stances'] = analysis['stances'].tolist()
    return records


def load_regulation_metadata() -> Dict[str, str]:
    """Load regulation metadata if available."""
    try:
        if os.path.exists('regulation_metadata.json'):
            with open('regulation_metadata.json', 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return {
        "regulation_name": "Regulation Comments Analysis",
        "docket_id": "",
        "agency": "",
        "brief_description": "Analysis of public comments on federal regulation"
    }



def get_date_range(comments: List[Dict[str, Any]]) -> str:
    """Get date range of comments."""
    dates = []
    for comment in comments:
        date_str = comment.get('date', '')
        if date_str:
            try:
                date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                dates.append(date)
            except Exception:
                pass
    if dates:
        min_d, max_d = min(dates), max(dates)
        if min_d.strftime('%B %Y') == max_d.strftime('%B %Y'):
            return f"{min_d.strftime('%B %d')}-{max_d.strftime('%d, %Y')}"
        return f"{min_d.strftime('%B %d, %Y')} to {max_d.strftime('%B %d, %Y')}"
    return "Unknown"


def compute_briefing(comments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute briefing summary stats from analyzed comments."""
    total = len(comments)
    oppose_count = 0
    support_count = 0
    unclear_count = 0
    concern_counts = {}
    concern_stance = {}  # concern label -> {Oppose, Support, Unclear} split
    entity_counts = {}
    entity_submitters = {}  # entity_type -> list of {name, org, id}
    state_counts = {}
    state_comments = {}  # state -> list of submitter details
    political_counts = {}
    political_comments = {}  # party -> list of submitter details
    support_comments = []
    unclear_comments = []

    for c in comments:
        analysis = c.get('analysis') or {}
        stances = analysis.get('stances', [])
        if hasattr(stances, 'tolist'):
            stances = stances.tolist()
        if not isinstance(stances, list):
            stances = []

        # Use verified_stance if available, otherwise fall back to stances list
        verified = analysis.get('verified_stance')
        comment_text = c.get('comment_text', '') or ''
        stance_entry = {
            'name': 'Anonymous' if (c.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else c.get('submitter', '').strip(),
            'id': c.get('id', ''),
            'sentence': comment_text[:200],
        }
        if verified == 'Unclear':
            unclear_count += 1
            unclear_comments.append(stance_entry)
        else:
            bucketed = False
            for s in stances:
                if 'Position: Oppose' in s:
                    oppose_count += 1
                    bucketed = True
                    break
                elif 'Position: Support' in s:
                    support_count += 1
                    support_comments.append(stance_entry)
                    bucketed = True
                    break
            # A comment with neither an Oppose nor a Support position tag is
            # ambiguous — bucket it as Unclear so oppose+support+unclear ≈ 100%.
            if not bucketed:
                unclear_count += 1
                unclear_comments.append(stance_entry)

        # Position bucket for this comment (reused for the per-concern split).
        pos = comment_position(c)
        for s in stances:
            if s.startswith('Concern:'):
                label = s.replace('Concern: ', '')
                concern_counts[label] = concern_counts.get(label, 0) + 1
                cs = concern_stance.setdefault(label, {'Oppose': 0, 'Support': 0, 'Unclear': 0})
                cs[pos] = cs.get(pos, 0) + 1

        entity = analysis.get('entity_type', 'Individual/Other')
        entity_counts[entity] = entity_counts.get(entity, 0) + 1
        if entity not in entity_submitters:
            entity_submitters[entity] = []

        cosigner_names = analysis.get('cosigner_names', [])
        if hasattr(cosigner_names, 'tolist'):
            cosigner_names = cosigner_names.tolist()
        if not isinstance(cosigner_names, list):
            cosigner_names = []

        entity_submitters[entity].append({
            'name': 'Anonymous' if (c.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else c.get('submitter', '').strip(),
            'org': c.get('organization', '').strip(),
            'id': c.get('id', ''),
            'entity_name': analysis.get('entity_name', ''),
            'entity_name_score': analysis.get('entity_name_match_score', ''),
            'cosigner_names': cosigner_names,
            'cosigner_count': _safe_int(analysis.get('cosigner_count')) or 1,
        })

        state = (analysis.get('state_identified') or '').strip()
        if state:
            state_counts[state] = state_counts.get(state, 0) + 1
            if state not in state_comments:
                state_comments[state] = []
            state_comments[state].append({
                'name': 'Anonymous' if (c.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else c.get('submitter', '').strip(),
                'id': c.get('id', ''),
                'entity_type': entity,
                'quote': analysis.get('state_quote', ''),
            })

        pol = (analysis.get('political_affiliation') or '').strip()
        if pol:
            political_counts[pol] = political_counts.get(pol, 0) + 1
            if pol not in political_comments:
                political_comments[pol] = []
            political_comments[pol].append({
                'name': 'Anonymous' if (c.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else c.get('submitter', '').strip(),
                'org': c.get('organization', '').strip(),
                'id': c.get('id', ''),
                'entity_type': entity,
                'quote': analysis.get('political_affiliation_quote', ''),
            })

    # Sort concerns by count descending
    sorted_concerns = sorted(concern_counts.items(), key=lambda x: x[1], reverse=True)
    concern_list = []
    for name, count in sorted_concerns:
        pct = display_pct(count, total)
        cs = concern_stance.get(name, {})
        oppose = cs.get('Oppose', 0)
        support = cs.get('Support', 0)
        unclear = cs.get('Unclear', 0)
        denom = oppose + support
        # Split the bar oppose-vs-support (unclear excluded from the ratio); an
        # all-unclear concern renders as a neutral full-oppose bar.
        oppose_pct = round(oppose / denom * 100) if denom else 100
        support_pct = 100 - oppose_pct if denom else 0
        concern_list.append({
            'name': name, 'count': count, 'pct': pct,
            'oppose': oppose, 'support': support, 'unclear': unclear,
            'oppose_pct': oppose_pct, 'support_pct': support_pct,
        })

    # Sort entities by count descending
    sorted_entities = sorted(entity_counts.items(), key=lambda x: x[1], reverse=True)
    entity_list = [{'name': name, 'count': count, 'submitters': entity_submitters.get(name, [])[:200]} for name, count in sorted_entities]

    with_attachments = sum(1 for c in comments if (c.get('attachment_text') or '').strip())

    # Campaign stats (from MinHash LSH detection in pipeline)
    campaign_groups = {}
    for c in comments:
        cid = c.get('campaign_id')
        if cid is None or (isinstance(cid, float) and cid != cid):  # NaN check
            continue
        cid = int(cid)
        if cid not in campaign_groups:
            campaign_groups[cid] = {
                'canonical': str(c.get('campaign_canonical', '') or '') if isinstance(c.get('campaign_canonical'), str) else '',
                'ids': [],
                'positions': [],
            }
        campaign_groups[cid]['ids'].append(c.get('id', ''))
        campaign_groups[cid]['positions'].append(comment_position(c))

    campaign_comments_count = sum(len(g['ids']) for g in campaign_groups.values())

    # Build old_id -> rank mapping (sorted by size descending)
    sorted_campaigns = sorted(campaign_groups.items(), key=lambda x: -len(x[1]['ids']))
    campaign_id_to_rank = {cid: rank + 1 for rank, (cid, _) in enumerate(sorted_campaigns)}

    # Count exact duplicates of canonical text
    canonical_counts = Counter()
    for c in comments:
        ct = (c.get('comment_text') or '').strip()
        if ct:
            canonical_counts[ct] += 1

    campaigns_list = []
    campaign_id_to_stance = {}
    for rank, (cid, g) in enumerate(sorted_campaigns):
        size = len(g['ids'])
        canonical = g['canonical']
        exact_dupes = canonical_counts.get(canonical, 0)
        preview = canonical[:200] + '...' if len(canonical) > 200 else canonical
        # Derive the campaign's overall stance from its members' already-computed
        # positions (no LLM call): plurality Support/Oppose, else Mixed.
        pc = Counter(g['positions'])
        support_n, oppose_n, unclear_n = pc.get('Support', 0), pc.get('Oppose', 0), pc.get('Unclear', 0)
        mx = max(support_n, oppose_n, unclear_n)
        winners = [k for k, v in (('Support', support_n), ('Oppose', oppose_n), ('Unclear', unclear_n)) if v == mx]
        stance = winners[0] if len(winners) == 1 and winners[0] in ('Support', 'Oppose') else 'Mixed'
        campaign_id_to_stance[cid] = stance
        # Oppose/Support split for the stacked bar (unclear excluded from the ratio).
        c_denom = oppose_n + support_n
        c_oppose_pct = round(oppose_n / c_denom * 100) if c_denom else 100
        c_support_pct = 100 - c_oppose_pct if c_denom else 0
        campaigns_list.append({
            'id': cid,
            'rank': rank + 1,
            'size': size,
            'exact_dupes': exact_dupes,
            'preview': preview,
            'canonical': canonical,
            'snippet': _snippet(canonical, 70),
            'sample_ids': g['ids'][:10],
            'stance': stance,
            'support': support_n,
            'oppose': oppose_n,
            'unclear': unclear_n,
            'oppose_pct': c_oppose_pct,
            'support_pct': c_support_pct,
        })

    return {
        'total_comments': total,
        'oppose_count': oppose_count,
        'oppose_pct': display_pct(oppose_count, total),
        'support_count': support_count,
        'support_pct': display_pct(support_count, total),
        'unclear_count': unclear_count,
        'unclear_pct': display_pct(unclear_count, total),
        'support_comments': support_comments,
        'unclear_comments': unclear_comments,
        'with_attachments': with_attachments,
        'date_range': get_date_range(comments),
        'concern_counts': concern_list,
        'entity_counts': entity_list,
        'state_counts': sorted(state_counts.items(), key=lambda x: x[1], reverse=True),
        'state_data': {st: subs[:200] for st, subs in state_comments.items()},
        'political_counts': sorted(political_counts.items(), key=lambda x: x[1], reverse=True),
        'political_data': {p: subs[:200] for p, subs in political_comments.items()},
        'campaign_count': len(campaign_groups),
        'campaign_comments_count': campaign_comments_count,
        'campaigns_list': campaigns_list,
        'campaign_id_to_stance': campaign_id_to_stance,
    }


def get_filter_values(comments: List[Dict[str, Any]]) -> Dict[str, list]:
    """Extract unique filter values from comments."""
    stances = set()
    entity_types = set()

    for c in comments:
        analysis = c.get('analysis') or {}
        s = analysis.get('stances', [])
        if hasattr(s, 'tolist'):
            s = s.tolist()
        if isinstance(s, list):
            for stance in s:
                if isinstance(stance, str):
                    stances.add(stance.strip())

        et = analysis.get('entity_type', '')
        if et:
            entity_types.add(et.strip())

    positions = sorted(s.replace('Position: ', '') for s in stances if s.startswith('Position:'))
    if 'Unclear' not in positions:
        positions.append('Unclear')
    positions.sort()
    concerns = sorted(s.replace('Concern: ', '') for s in stances if s.startswith('Concern:'))

    states = set()
    political = set()
    for c in comments:
        analysis = c.get('analysis') or {}
        state = (analysis.get('state_identified') or '').strip()
        if state:
            states.add(state)
        pol = (analysis.get('political_affiliation') or '').strip()
        if pol:
            political.add(pol)

    campaign_sizes = {}
    for c in comments:
        cid = c.get('campaign_id')
        if cid is not None and not (isinstance(cid, float) and cid != cid):
            cid = int(cid)
            campaign_sizes[cid] = campaign_sizes.get(cid, 0) + 1

    # Rank by size descending
    ranked = sorted(campaign_sizes.keys(), key=lambda k: -campaign_sizes[k])
    id_to_rank = {cid: rank + 1 for rank, cid in enumerate(ranked)}

    return {
        'stances': sorted(stances),
        'positions': positions,
        'concerns': concerns,
        'entity_types': sorted(entity_types),
        'states': sorted(states),
        'political': sorted(political),
        'campaigns': [f"Campaign {id_to_rank[cid]} ({campaign_sizes[cid]:,})" for cid in ranked],
        'campaign_id_to_rank': id_to_rank,
    }


def _safe_int(val):
    """Convert to int, returning None for None/NaN."""
    if val is None:
        return None
    if isinstance(val, float) and val != val:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def prepare_rows(comments: List[Dict[str, Any]], campaign_id_to_rank: dict = None, flag_keys: List[str] = None, campaign_id_to_stance: dict = None, regex_value_patterns: dict = None) -> List[Dict[str, Any]]:
    """Prepare comment data for table rows and modal detail.

    One row per submission. The table used to collapse identical texts, which made
    its row count, filters and CSV download disagree with the stat cards (those
    always counted every submission) and hid people who sent a form letter. Exact
    duplicates are still summarized as campaigns; this only affects the table.
    """
    campaign_id_to_rank = campaign_id_to_rank or {}
    campaign_id_to_stance = campaign_id_to_stance or {}
    flag_keys = flag_keys or []
    regex_value_patterns = regex_value_patterns or {}

    # The table's date column is the submitted date, which only exists in parquets
    # written after received_date was added to read_comments_from_csv. Rendering
    # anyway would publish a silently blank column — exactly the kind of
    # plausible-looking wrong answer this report must never give — so say what is
    # missing and how to fix it instead.
    if comments and 'received_date' not in comments[0]:
        raise SystemExit(
            "This parquet has no `received_date` column, so the Submitted date "
            "cannot be rendered. It predates that field; re-run pipeline.py to "
            "rebuild it from source.csv (analysis is text-keyed and will be "
            "reused, so nothing is re-analyzed).")

    rows = []
    for comment in comments:
        analysis = comment.get('analysis') or {}

        # Stances
        stance_data = analysis.get('stances', [])
        if hasattr(stance_data, 'tolist'):
            stance_data = stance_data.tolist()
        if isinstance(stance_data, str):
            stance_data = [stance_data] if stance_data else []
        elif not isinstance(stance_data, list):
            stance_data = []

        stances_html = ' '.join(f'<span class="stance-tag">{s}</span>' for s in stance_data) if stance_data else ''

        # Cosigner names (joint/coalition letters)
        cosigner_names = analysis.get('cosigner_names', [])
        if hasattr(cosigner_names, 'tolist'):
            cosigner_names = cosigner_names.tolist()
        if not isinstance(cosigner_names, list):
            cosigner_names = []

        positions = [s for s in stance_data if s.startswith('Position:')]
        concerns = [s for s in stance_data if s.startswith('Concern:')]
        position_html = ' '.join(f'<span class="stance-tag tag-position">{s.replace("Position: ", "")}</span>' for s in positions)
        concerns_html = ' '.join(f'<span class="stance-tag tag-concern">{s.replace("Concern: ", "")}</span>' for s in concerns)

        # Dates. `received_date` is when the commenter submitted, `date` is when
        # regulations.gov published it; the table shows the former and both are
        # filterable. See the guard in generate_html_report for what happens when
        # the parquet predates received_date.
        def _fmt_date(value):
            if not value:
                return ''
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00')).strftime('%Y-%m-%d')
            except Exception:
                return value[:10] if len(value) >= 10 else value

        formatted_date = _fmt_date(comment.get('date', ''))
        formatted_received = _fmt_date(comment.get('received_date', ''))

        # Comment preview for table
        comment_text = comment.get('comment_text', '') or ''
        comment_preview = comment_text[:200] + '...' if len(comment_text) > 200 else comment_text

        rows.append({
            'id': comment.get('id', ''),
            'date': formatted_date,
            'received_date': formatted_received,
            'submitter': 'Anonymous' if (comment.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else comment.get('submitter', '').strip(),
            'organization': comment.get('organization', '') or '',
            'entity_type': analysis.get('entity_type', 'Individual/Other'),
            'entity_name': analysis.get('entity_name', ''),
            'cosigner_names': cosigner_names,
            'cosigner_count': _safe_int(analysis.get('cosigner_count')) or 1,
            'stances_html': stances_html,
            'position_html': position_html,
            'concerns_html': concerns_html,
            'stances_list': stance_data,
            'flags': {k: bool(comment.get(k)) for k in flag_keys},
            'comment_preview': comment_preview,
            'comment_text': comment_text,
            'key_quote': analysis.get('key_quote', ''),
            'rationale': analysis.get('rationale', ''),
            'state_identified': analysis.get('state_identified', ''),
            'state_quote': analysis.get('state_quote', ''),
            'political_affiliation': analysis.get('political_affiliation', ''),
            'political_affiliation_quote': analysis.get('political_affiliation_quote', ''),
            'attachment_text': comment.get('attachment_text', '') or '',
            'campaign_id': _safe_int(comment.get('campaign_id')),
            'campaign_rank': campaign_id_to_rank.get(_safe_int(comment.get('campaign_id'))) if _safe_int(comment.get('campaign_id')) is not None else None,
            'campaign_size': _safe_int(comment.get('campaign_size')),
            'campaign_stance': campaign_id_to_stance.get(_safe_int(comment.get('campaign_id'))) or '',
            'multi_values': {name: extract_regex_values(comment.get('comment_text', '') or '', pat) for name, pat in regex_value_patterns.items()},
        })
    return rows


def _snippet(text: str, n: int = 70) -> str:
    """Collapse whitespace and ellipsize text to ~n chars (for campaign labels)."""
    t = ' '.join((text or '').split())
    return (t[:n].rstrip() + '…') if len(t) > n else t


def comment_position(c: Dict[str, Any]) -> str:
    """Bucket a comment into Oppose / Support / Unclear from already-computed data.

    Prefers the second-pass `verified_stance` when present, else the Position tag
    in the stances list. Used both for the stance stat cards and to derive each
    campaign's overall stance.
    """
    analysis = c.get('analysis') or {}
    verified = analysis.get('verified_stance')
    if verified in ('Oppose', 'Support', 'Unclear'):
        return verified
    stances = analysis.get('stances', [])
    if hasattr(stances, 'tolist'):
        stances = stances.tolist()
    if not isinstance(stances, list):
        stances = []
    for s in stances:
        if 'Position: Oppose' in s:
            return 'Oppose'
        if 'Position: Support' in s:
            return 'Support'
    return 'Unclear'


def extract_regex_values(text: str, compiled) -> List[str]:
    """Return the de-duplicated list of regex matches (in order) from text.

    Used by `source: regex, type: multi_value` fields (e.g. CFR section citations)
    to derive a multi-value dimension from the comment text at report time.
    """
    if not text:
        return []
    seen = []
    for m in compiled.finditer(text):
        v = m.group(0)
        if v not in seen:
            seen.append(v)
    return seen


def compute_value_sections(comments: List[Dict[str, Any]], fields) -> tuple:
    """Compute report-time breakdowns for `source: regex, type: multi_value` fields.

    Returns (value_sections, patterns) where value_sections is a list of
    {key, label, show, items:[{name,count}], distinct} (top 15 by comment count)
    and patterns maps field name -> compiled regex for per-row extraction.
    """
    value_sections = []
    patterns = {}
    for f in (fields or []):
        if f.get('source') != 'regex' or f.get('type') != 'multi_value':
            continue
        try:
            pat = re.compile(f.get('pattern', ''), re.IGNORECASE)
        except re.error:
            continue
        patterns[f['name']] = pat
        counts = {}
        stance_split = {}  # value -> {Oppose, Support, Unclear}
        for c in comments:
            pos = comment_position(c)
            for v in set(extract_regex_values(c.get('comment_text', '') or '', pat)):
                counts[v] = counts.get(v, 0) + 1
                ss = stance_split.setdefault(v, {'Oppose': 0, 'Support': 0, 'Unclear': 0})
                ss[pos] = ss.get(pos, 0) + 1
        items = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
        entries = []
        for n, ct in items[:15]:
            ss = stance_split.get(n, {})
            oppose = ss.get('Oppose', 0)
            support = ss.get('Support', 0)
            denom = oppose + support
            oppose_pct = round(oppose / denom * 100) if denom else 100
            support_pct = 100 - oppose_pct if denom else 0
            entries.append({'name': n, 'count': ct, 'oppose': oppose, 'support': support,
                            'oppose_pct': oppose_pct, 'support_pct': support_pct})
        value_sections.append({
            'key': f['name'],
            'label': f.get('label', f['name']),
            'show': list(f.get('show', [])),
            # NB: key is 'entries' not 'items' — Jinja `vs.items` would resolve to
            # the dict.items() method, not this value.
            'entries': entries,
            'distinct': len(counts),
        })
    return value_sections, patterns


def load_fields() -> List[Dict[str, Any]]:
    """Load the `fields:` block (options resolved) from analyzer_config.yaml, or None.

    The `fields:` block is the single source of truth for the report's
    columns/filters/cards; when absent, callers fall back to legacy behavior.
    """
    config_path = Path('analyzer_config.yaml')
    if not config_path.exists():
        return None
    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}
    fields = raw.get('fields')
    if not fields:
        return None
    stance_names = [s['name'] for s in raw.get('stances', [])]
    entity_types = list(raw.get('entity_types', []))
    out = []
    for fld in fields:
        fld = dict(fld)
        src = fld.get('options_from')
        if src == 'stances':
            fld['options'] = stance_names
        elif src == 'entity_types':
            fld['options'] = entity_types
        else:
            fld['options'] = fld.get('options', []) or []
        fld['show'] = list(fld.get('show', []) or [])
        out.append(fld)
    return out


def compute_field_meta(fields, report_config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """name -> {label, type, show} driving the report's columns/filters/cards.

    When a config declares `fields:`, that is authoritative. When it doesn't, we
    synthesize metadata matching the historical hardcoded behavior (full
    back-compat): stances/entity surfaced everywhere, quotes/rationale modal-only,
    and state/political gated by the legacy `report.show_state/show_political`.
    """
    if fields:
        return {f['name']: {'label': f.get('label', f['name']), 'type': f.get('type', ''), 'show': list(f.get('show', []))} for f in fields}
    col_filt = ['column', 'filter']
    return {
        'stances': {'label': 'Position & Concerns', 'type': 'multi_enum', 'show': ['cards', 'column', 'filter', 'modal']},
        'entity_type': {'label': 'Entity Type', 'type': 'single_enum', 'show': ['cards', 'column', 'filter', 'modal']},
        'entity_name': {'label': 'Identified As', 'type': 'quote', 'show': ['modal']},
        'key_quote': {'label': 'Key Quote', 'type': 'text', 'show': ['modal']},
        'rationale': {'label': 'Rationale', 'type': 'text', 'show': ['modal']},
        'state_identified': {'label': 'State', 'type': 'text', 'show': col_filt if report_config.get('show_state') else []},
        'state_quote': {'label': 'State Quote', 'type': 'quote', 'show': []},
        'political_affiliation': {'label': 'Political', 'type': 'enum_or_empty', 'show': col_filt if report_config.get('show_political') else []},
        'political_affiliation_quote': {'label': 'Political Quote', 'type': 'quote', 'show': []},
    }


# Full color palette — every --color-* token. House defaults are used for any
# key a regulation's `report.colors` omits. Editing the YAML is a one-line recolor.
DEFAULT_COLORS = {
    'bg': '#FFF8F0', 'surface': '#F5EDE0', 'text': '#3D2B1F', 'text_muted': '#7A6E62',
    'accent': '#1B3A5C', 'accent_hover': '#12293F', 'highlight': '#D4A03C',
    'border': '#E8DDD0', 'code_bg': '#2A211A', 'error': '#C0392B',
    'oppose': '#C0392B', 'support': '#2D6A4F', 'unclear': '#7A6E62', 'mixed': '#7A6E62',
}


def _hex_to_rgb(h: str) -> str:
    """'#1B3A5C' -> '27, 58, 92' (for --bs-primary-rgb / focus-ring rgba)."""
    h = (h or '').lstrip('#')
    if len(h) == 3:
        h = ''.join(c * 2 for c in h)
    try:
        return f"{int(h[0:2], 16)}, {int(h[2:4], 16)}, {int(h[4:6], 16)}"
    except (ValueError, IndexError):
        return "27, 58, 92"


def load_colors(report_config: Dict[str, Any]) -> Dict[str, str]:
    """Full palette from `report.colors`, falling back to the house defaults.

    Back-compat: a legacy `report.stance_colors` still overrides the stance keys.
    """
    colors = dict(DEFAULT_COLORS)
    cfg = report_config.get('colors') or {}
    for k, v in cfg.items():
        if k in colors and v:
            colors[k] = v
    legacy = report_config.get('stance_colors') or {}
    for k in ('oppose', 'support', 'unclear', 'mixed'):
        if legacy.get(k):
            colors[k] = legacy[k]
    return colors


def load_report_config() -> Dict[str, Any]:
    """Load the optional top-level `report:` display-gating section from config.

    Absent keys default to falsy, so any opt-in display sections only appear
    when a regulation explicitly enables them.
    """
    config_path = Path('analyzer_config.yaml')
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
        rc = config.get('report', {})
        return rc if isinstance(rc, dict) else {}
    return {}


def config_declares_rule_text() -> bool:
    """True when analyzer_config.yaml has a `rule_text:` block — i.e. this
    regulation asserts it has a Read-the-Rule page."""
    config_path = Path('analyzer_config.yaml')
    if not config_path.exists():
        return False
    with open(config_path) as f:
        config = yaml.safe_load(f) or {}
    return bool(config.get('rule_text'))


def load_changelog() -> List[Dict[str, Any]]:
    """Build the report's changelog: manual/methodology notes from
    analyzer_config.yaml (`changelog:`) merged with the auto-generated
    data-update entries in data_changelog.json (written by the pipeline when the
    comment count grows). Each entry is `{date, note}`; newest first.
    """
    entries: List[Dict[str, Any]] = []

    config_path = Path('analyzer_config.yaml')
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
        entries += [e for e in (config.get('changelog') or []) if isinstance(e, dict)]

    auto_path = Path('data_changelog.json')
    if auto_path.exists():
        try:
            with open(auto_path) as f:
                state = json.load(f)
            entries += [e for e in (state.get('entries') or []) if isinstance(e, dict)]
        except Exception:
            pass

    entries.sort(key=lambda e: str(e.get('date', '')), reverse=True)
    return entries


def determine_model(comments: List[Dict[str, Any]], override: str = None) -> str:
    """Determine the model to show in the report footer from the data itself.

    Precedence: explicit override (e.g. --model) > most-common `model_used`
    value recorded in the parquet > 'unknown'. Never a hardcoded model string.
    """
    if override:
        return override
    # Must be a non-empty STRING. Rows analyzed before model_used was recorded
    # read back from the parquet as float NaN, and NaN is truthy in Python — so a
    # bare truthiness test lets it win the count and prints the model as "nan".
    vals = [c.get('model_used') for c in comments
            if isinstance(c.get('model_used'), str) and c['model_used'].strip()]
    if vals:
        return Counter(vals).most_common(1)[0][0]
    return 'unknown'


def load_regex_flags() -> Dict[str, Dict[str, Any]]:
    """Load the full regex_flags config (patterns + description + optional label).

    Per-regulation config lives in the current working directory (the pipeline
    chdirs into regulations/<slug>/); the Jinja template stays next to the code.
    """
    config_path = Path('analyzer_config.yaml')
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
        regex_flags = config.get('regex_flags', {})
        return {name: flag for name, flag in regex_flags.items() if isinstance(flag, dict)}
    return {}


def load_regex_flag_patterns():
    """Load just the pattern lists per flag (for the search-patterns modal)."""
    return {name: flag.get('patterns', []) for name, flag in load_regex_flags().items()}


# Cloudflare Pages refuses any single file over 25 MiB, so the row data is split
# well under that. Chunking by bytes rather than row count means a corpus whose
# rows get fatter cannot silently drift over the limit.
ROW_CHUNK_MAX_BYTES = 8 * 1024 * 1024

# Positional, not keyed: repeating 16 key names across 65k rows costs several MB
# for nothing. This order is load-bearing — it must match COMMENT_FIELDS in
# report_template.html exactly, or every column in the table shifts.
def _row_to_list(r):
    return [
        r['id'],
        r['date'],
        r['received_date'],
        r['submitter'],
        r['organization'],
        r['entity_type'],
        r['cosigner_count'],
        r['stances_list'],
        r['flags'],
        r['state_identified'],
        r['political_affiliation'],
        bool(r.get('attachment_text')),
        r.get('campaign_id'),
        r.get('campaign_rank'),
        r.get('campaign_size') if r.get('campaign_size') is not None else 0,
        r['campaign_stance'],
        r['multi_values'],
    ]


def write_row_chunks(rows, out_dir, max_bytes=ROW_CHUNK_MAX_BYTES):
    """Write table rows to comment_rows/<n>.js and return the chunk count.

    These are plain blocking <script> tags in the page, not fetches, so they run
    in order before the inline script and commentDataRaw is ready synchronously —
    no downstream code has to become async.

    Inline, this data was 33.6 MB of a 36.3 MB index.html (93% of it): parsed
    before anything could render, and over Cloudflare Pages' 25 MiB per-file
    limit, which blocked hosting the report there at all.
    """
    chunk_dir = os.path.join(out_dir, 'comment_rows')
    if os.path.isdir(chunk_dir):
        shutil.rmtree(chunk_dir)
    os.makedirs(chunk_dir)

    def flush(buf, index):
        with open(os.path.join(chunk_dir, f'{index}.js'), 'w', encoding='utf-8') as f:
            # concat, not push(...spread): spreading tens of thousands of rows
            # into a call blows the argument limit in some browsers.
            f.write('window.__ROWS__ = (window.__ROWS__ || []).concat([\n')
            f.write(',\n'.join(buf))
            f.write('\n]);\n')

    buf, size, index = [], 0, 0
    for r in rows:
        line = json.dumps(_row_to_list(r), ensure_ascii=False, separators=(',', ':'))
        if buf and size + len(line) > max_bytes:
            flush(buf, index)
            index += 1
            buf, size = [], 0
        buf.append(line)
        size += len(line) + 2
    if buf:
        flush(buf, index)
        index += 1
    return index


DETAIL_SHARD_SIZE = 500

DETAIL_FIELDS = {
    'comment': 'comment_text',
    'key_quote': 'key_quote',
    'rationale': 'rationale',
    'entity_name': 'entity_name',
    'cosigner_names': 'cosigner_names',
    'state_quote': 'state_quote',
    'political_quote': 'political_affiliation_quote',
}


def write_detail_shards(rows, out_dir, shard_size=DETAIL_SHARD_SIZE):
    """Write per-comment detail to comment_detail/<n>.json and return (size, count).

    Detail-only fields (full comment text, key quote, rationale) are shown in the
    per-comment modal and searched by the full-text filter, but never rendered in
    the table — so they live in sidecar JSON rather than the page itself.

    Sharded, and NOT fetched on page load. As one file at 65k comments this
    reached 124 MB (~26 MB gzipped) and *every* visitor paid it in a background
    fetch whether or not they opened a single comment: about 3,700 pageviews
    exhausted a 100 GB monthly bandwidth allowance. Opening one comment needs one
    comment's detail, so the page now fetches only the shard containing it. The
    full-text filter is the one feature that genuinely needs the whole corpus,
    and it pulls every shard — but only once someone opens the search box.

    Shards are keyed by position in `rows`, which is the order the table is built
    from, so the page derives a comment's shard from its row index with no lookup
    table. Anything that reorders `rows` between here and the template render
    would silently send the browser to the wrong shard — see
    tests/test_detail_shards.py.
    """
    detail_dir = os.path.join(out_dir, 'comment_detail')
    if os.path.isdir(detail_dir):
        shutil.rmtree(detail_dir)
    os.makedirs(detail_dir)

    shard_count = math.ceil(len(rows) / shard_size) if rows else 0
    for shard in range(shard_count):
        chunk = rows[shard * shard_size:(shard + 1) * shard_size]
        payload = {
            r['id']: {key: r[source] for key, source in DETAIL_FIELDS.items()}
            for r in chunk
        }
        with open(os.path.join(detail_dir, f'{shard}.json'), 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))

    # The single-file sidecar this replaced. Deleting it stops a stale 124 MB
    # copy being picked up by a deploy or shadowing the new directory.
    legacy = os.path.join(out_dir, 'comment_detail.json')
    if os.path.exists(legacy):
        os.remove(legacy)

    return shard_size, shard_count


def load_derived_flags() -> Dict[str, Dict[str, Any]]:
    """Load derived_flags config: boolean flags computed from an analysis field
    (e.g. cosigner_count >= 2) rather than a regex. Same card/filter machinery as
    regex_flags; a comment matches when analysis[<from>] >= <min>."""
    config_path = Path('analyzer_config.yaml')
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
        derived = config.get('derived_flags', {}) or {}
        return {name: cfg for name, cfg in derived.items() if isinstance(cfg, dict)}
    return {}


def load_id_list_flags() -> Dict[str, Dict[str, Any]]:
    """Load id_list_flags config: boolean flags whose membership comes from a
    JSON file of document ids rather than the comment text. Same card/filter
    machinery as regex_flags.

    Used for facts the comment body cannot carry -- e.g. that the agency
    withdrew a comment after publication, which is recorded in the bulk export's
    metadata and vanishes from it once the row is blanked."""
    config_path = Path('analyzer_config.yaml')
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
        cfg = config.get('id_list_flags', {}) or {}
        return {name: c for name, c in cfg.items() if isinstance(c, dict)}
    return {}


def load_id_list(path: str) -> set:
    """Read an id-list file as {document_id: entry}.

    Accepts either a bare list of ids or {"comments": [{"document_id": ...}]}.
    Fails loudly on a missing file: a flag configured but silently empty would
    publish a report that quietly under-reports whatever it tracks."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"id_list_flags references {path}, which does not exist. "
            f"Remove the flag from analyzer_config.yaml or restore the file."
        )
    with open(p) as f:
        data = json.load(f)
    if isinstance(data, dict):
        data = data.get('comments', [])
    entries = {}
    for item in data:
        doc_id = item.get('document_id') if isinstance(item, dict) else item
        if doc_id:
            entries[str(doc_id)] = item if isinstance(item, dict) else {}
    return entries


def compute_flag_sections(comments: List[Dict[str, Any]], flags_cfg: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build one generic section per configured regex flag.

    Each section carries the flag's count, percentage, matching comments (with a
    highlighted sentence), its patterns, and a display label — all driven by the
    regulation's analyzer_config.yaml, nothing hardcoded per regulation.
    """
    total = len(comments)
    sections = []
    for key, cfg in flags_cfg.items():
        patterns = cfg.get('patterns', []) if isinstance(cfg, dict) else []
        description = cfg.get('description', '') if isinstance(cfg, dict) else ''
        derived = cfg.get('_derived') if isinstance(cfg, dict) else None
        label = humanize_flag_label(key, cfg)
        matched = []
        count = 0
        max_val = 0
        for c in comments:
            if c.get(key):
                count += 1
                if derived:
                    a = c.get('analysis') or {}
                    n = _safe_int(a.get(derived.get('from', 'cosigner_count'))) or 0
                    max_val = max(max_val, n)
                if len(matched) < 500:
                    if derived:
                        a = c.get('analysis') or {}
                        n = _safe_int(a.get(derived.get('from', 'cosigner_count'))) or 0
                        ename = (a.get('entity_name') or '').strip() if isinstance(a, dict) else ''
                        sentence = f"Cosigned by {n:,} organizations" + (f" — {_snippet(ename, 80)}" if ename else "")
                        sort_n = n
                    else:
                        notes = cfg.get('_id_list_notes') if isinstance(cfg, dict) else None
                        if notes:
                            cid = str(c.get('id', ''))
                            sentence = notes.get(cid) or notes.get(cid.split('#', 1)[0], '')
                        else:
                            ct = c.get('comment_text', '') or ''
                            sentence = extract_matching_sentence(ct, patterns)
                        sort_n = 0
                    matched.append({
                        'name': 'Anonymous' if (c.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else c.get('submitter', '').strip(),
                        'id': c.get('id', ''),
                        'sentence': sentence,
                        '_sort_n': sort_n,
                    })
        if derived and max_val > 1:
            description = (description + f" Largest: {max_val:,} cosigners.").strip()
        # Show the biggest coalitions first in the flag modal.
        if derived:
            matched.sort(key=lambda m: -m.get('_sort_n', 0))
        sections.append({
            'key': key,
            'label': label,
            'description': description,
            'count': count,
            'pct': display_pct(count, total),
            'patterns': patterns,
            'comments': matched,
        })
    return sections


def load_eval_scores():
    """Load eval/scores.json for the accuracy page, if score_stances.py has run.

    Absent is a normal state — a regulation with no evaluation set simply has no
    accuracy page and no link to one. Unlike rule_text there is nothing in the
    config declaring it, so there is no broken-build case to guard against.
    """
    p = Path('eval') / 'scores.json'
    if not p.exists():
        return None
    with open(p) as f:
        scores = json.load(f)
    return scores or None


def load_rule_sections():
    """Load the proposed-rule sections (rule_sections.json) for the Read-the-Rule
    page, or None when the regulation has no rule text prepared."""
    p = Path('rule_sections.json')
    if not p.exists():
        return None
    try:
        with open(p, encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) and data else None
    except Exception:
        return None


def compute_rule_page(comments, rule_sections, patterns, sample_n=8):
    """Per-section citing-comment counts, Oppose/Support stance split, and a small
    sample of citing comments, for the Read-the-Rule page.

    `patterns` maps regex-field name -> compiled pattern (from compute_value_sections);
    §values are unioned per comment. Returns (sections, other_sections): `sections`
    follows rule_sections' reading order (amended sections, with text); `other_sections`
    lists cited §numbers NOT amended by the rule (count + split only, no text).
    """
    section_counts = {}
    section_stance = {}   # number -> {Oppose, Support, Unclear}
    section_samples = {}
    for c in comments:
        text = c.get('comment_text', '') or ''
        vals = set()
        for pat in patterns.values():
            vals.update(extract_regex_values(text, pat))
        if not vals:
            continue
        pos = comment_position(c)
        name = 'Anonymous' if (c.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else c.get('submitter', '').strip()
        cid = c.get('id', '')
        # Prefer the extracted key_quote (substance) over the raw comment opening
        # (usually boilerplate), clamped to one line.
        analysis = c.get('analysis') or {}
        key_quote = (analysis.get('key_quote') or '').strip() if isinstance(analysis, dict) else ''
        snip = _snippet(key_quote or text, 120)
        for v in vals:
            section_counts[v] = section_counts.get(v, 0) + 1
            ss = section_stance.setdefault(v, {'Oppose': 0, 'Support': 0, 'Unclear': 0})
            ss[pos] = ss.get(pos, 0) + 1
            samp = section_samples.setdefault(v, [])
            if len(samp) < sample_n:
                samp.append({'name': name, 'id': cid, 'snippet': snip, 'position': pos})

    def _split(num):
        ss = section_stance.get(num, {})
        op, su = ss.get('Oppose', 0), ss.get('Support', 0)
        denom = op + su
        op_pct = round(op / denom * 100) if denom else 100
        return op, su, op_pct, (100 - op_pct if denom else 0)

    rule_numbers = set()
    sections = []
    for s in rule_sections:
        num = s.get('number', '')
        rule_numbers.add(num)
        op, su, op_pct, su_pct = _split(num)
        sections.append({
            'number': num,
            'sectno': s.get('sectno', num),
            'heading': s.get('heading', ''),
            'amendment': s.get('amendment', ''),
            'text': s.get('text', ''),
            'count': section_counts.get(num, 0),
            'oppose': op, 'support': su, 'oppose_pct': op_pct, 'support_pct': su_pct,
            'sample': section_samples.get(num, []),
        })
    other = []
    for v, ct in section_counts.items():
        if v in rule_numbers:
            continue
        op, su, op_pct, su_pct = _split(v)
        other.append({'number': v, 'count': ct, 'oppose_pct': op_pct, 'support_pct': su_pct})
    other.sort(key=lambda x: (-x['count'], x['number']))
    return sections, other


def generate_html(comments: List[Dict[str, Any]], stats: Dict[str, Any], field_analysis: Dict[str, Dict[str, Any]], output_file: str, model_used: str = None):
    """Generate HTML report using Jinja2 template."""
    template_dir = Path(__file__).parent
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=False)
    template = env.get_template('report_template.html')

    metadata = load_regulation_metadata()
    flags_cfg = load_regex_flags()
    # Derived flags: compute each comment's boolean from an analysis field
    # (e.g. cosigner_count >= 2) and fold them into the same card/filter path.
    derived_cfg = load_derived_flags()
    for key, dcfg in derived_cfg.items():
        src = dcfg.get('from', 'cosigner_count')
        minimum = dcfg.get('min', 2)
        for c in comments:
            a = c.get('analysis') or {}
            val = _safe_int(a.get(src)) if isinstance(a, dict) else None
            c[key] = bool(val is not None and val >= minimum)
        flags_cfg[key] = {
            'label': dcfg.get('label', key),
            'description': dcfg.get('description', ''),
            'patterns': [],
            '_derived': {'from': src, 'min': minimum},
        }
    # Id-list flags: membership comes from a JSON file of document ids rather
    # than the comment text. Ids in the parquet may be disambiguated as
    # "<id>#<tracking>" when the export reuses a Document ID, so match on the
    # bare id too.
    for key, icfg in load_id_list_flags().items():
        entries = load_id_list(icfg.get('file', ''))
        for c in comments:
            cid = str(c.get('id', ''))
            c[key] = cid in entries or cid.split('#', 1)[0] in entries
        # Show a per-comment note in the flag modal instead of the regex-matched
        # sentence this flag has no patterns to produce -- e.g. why the agency
        # withdrew that particular comment.
        note_field = icfg.get('note_field', '')
        notes = {}
        if note_field:
            notes = {i: str(e.get(note_field, '') or '') for i, e in entries.items()}
        flags_cfg[key] = {
            'label': icfg.get('label', key),
            'description': icfg.get('description', ''),
            'patterns': [],
            '_id_list_notes': notes,
        }
    flag_keys = list(flags_cfg.keys())
    report_config = load_report_config()
    colors = load_colors(report_config)
    accent_rgb = _hex_to_rgb(colors['accent'])
    source_url = report_config.get('source_url') or None
    full_export_url = (report_config.get('full_export') or {}).get('url') or None
    fields = load_fields()
    field_meta = compute_field_meta(fields, report_config)
    show_stance_cards = 'cards' in field_meta.get('stances', {}).get('show', [])
    show_entity_cards = 'cards' in field_meta.get('entity_type', {}).get('show', [])
    value_sections, regex_value_patterns = compute_value_sections(comments, fields)
    briefing = compute_briefing(comments)
    briefing['flag_sections'] = compute_flag_sections(comments, flags_cfg)
    flag_meta = [{'key': s['key'], 'label': s['label']} for s in briefing['flag_sections']]
    filter_values = get_filter_values(comments)
    rows = prepare_rows(
        comments,
        campaign_id_to_rank=filter_values.get('campaign_id_to_rank', {}),
        flag_keys=flag_keys,
        campaign_id_to_stance=briefing.get('campaign_id_to_stance', {}),
        regex_value_patterns=regex_value_patterns,
    )
    regex_patterns = load_regex_flag_patterns()
    show_cosigners = any(r.get('cosigner_count', 1) > 1 for r in rows)

    detail_shard_size, detail_shard_count = write_detail_shards(
        rows, os.path.dirname(output_file) or '.')
    row_chunk_count = write_row_chunks(rows, os.path.dirname(output_file) or '.')

    # Read-the-Rule page — only when the regulation has proposed-rule text prepared.
    #
    # A regulation that declares `rule_text:` is asserting it HAS a rule page, so a
    # missing rule_sections.json there is a broken build, not a configuration choice.
    # Treating the two cases alike is what quietly deleted this page for ten days:
    # CI checkouts never had the file (gitignored, and not synced from R2), so every
    # deploy dropped both the page and the link to it, and the report looked fine
    # because nothing pointed at the hole. Say so instead.
    rule_sections = load_rule_sections()
    if rule_sections is None and config_declares_rule_text():
        raise SystemExit(
            "analyzer_config.yaml declares `rule_text:`, so this regulation is "
            "expected to have a Read-the-Rule page, but rule_sections.json is "
            "missing or empty — the deploy would silently drop read-the-rule.html "
            "and the link to it. Run fetch_rule_text.py to rebuild it, or remove "
            "`rule_text:` from the config if the page is genuinely not wanted.")
    rule_page_url = 'read-the-rule.html' if rule_sections else None
    eval_scores = load_eval_scores()
    accuracy_page_url = 'accuracy.html' if eval_scores else None
    model_name = determine_model(comments, model_used)
    generated_time = datetime.now().strftime('%B %d, %Y at %I:%M %p')

    html = template.render(
        metadata=metadata,
        briefing=briefing,
        filter_values=filter_values,
        rows=rows,
        regex_patterns=regex_patterns,
        flag_meta=flag_meta,
        field_meta=field_meta,
        value_sections=value_sections,
        colors=colors,
        accent_rgb=accent_rgb,
        detail_shard_size=detail_shard_size,
        detail_shard_count=detail_shard_count,
        row_chunk_count=row_chunk_count,
        show_stance_cards=show_stance_cards,
        show_entity_cards=show_entity_cards,
        show_cosigners=show_cosigners,
        rule_page_url=rule_page_url,
        source_url=source_url,
        full_export_url=full_export_url,
        generated_time=generated_time,
        model_used=model_name,
        changelog=load_changelog(),
        accuracy_page_url=accuracy_page_url,
        eval_scores=eval_scores,
    )

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    if rule_sections:
        rule_template = env.get_template('rule_template.html')
        sections, other_sections = compute_rule_page(comments, rule_sections, regex_value_patterns)
        cited_total = sum(s['count'] for s in sections) + sum(o['count'] for o in other_sections)
        rule_html = rule_template.render(
            metadata=metadata,
            sections=sections,
            other_sections=other_sections,
            colors=colors,
            accent_rgb=accent_rgb,
            report_url=os.path.basename(output_file),
            section_field_key='sections_referenced',
            amended_count=len(sections),
            other_count=len(other_sections),
            source_url=source_url,
            generated_time=generated_time,
            model_used=model_name,
        )
        rule_output = os.path.join(os.path.dirname(output_file) or '.', 'read-the-rule.html')
        with open(rule_output, 'w', encoding='utf-8') as f:
            f.write(rule_html)

    if eval_scores:
        _render_accuracy_page(env, eval_scores, metadata, colors, output_file, generated_time)


def _render_accuracy_page(env, scores, metadata, colors, output_file, generated_time):
    """Render accuracy.html from eval/scores.json — same pattern as read-the-rule."""
    tpl = env.get_template('accuracy_template.html')
    html_out = tpl.render(
        metadata=metadata,
        scores=scores,
        colors=colors,
        report_url=os.path.basename(output_file),
        generated_time=generated_time,
    )
    out = os.path.join(os.path.dirname(output_file) or '.', 'accuracy.html')
    with open(out, 'w', encoding='utf-8') as f:
        f.write(html_out)


def _export_slug(s: str) -> str:
    """Column-safe slug for a config option name."""
    return re.sub(r'[^a-z0-9]+', '_', str(s).lower()).strip('_')


def _export_value(v) -> str:
    """Flatten one analysis value to a CSV cell. Lists/arrays become ' | '-joined."""
    if v is None:
        return ''
    if hasattr(v, 'tolist'):
        v = v.tolist()
    if isinstance(v, (list, tuple)):
        return ' | '.join(str(x) for x in v)
    if isinstance(v, float) and v != v:  # NaN
        return ''
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True, default=str)
    return str(v)


def match_source_rows(comments: List[Dict[str, Any]], source_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Pair each analyzed comment with the bulk-export row it came from.

    Document IDs are NOT unique in the government export, so this cannot be a plain
    id join (see CLAUDE.md). `read_comments_from_csv` suffixes repeat ids as
    `<Document ID>#<Tracking Number>`, so candidates are narrowed by tracking
    number, then by exact comment text, and every source row is claimed at most
    once. Raises if any comment is unmatched.
    """
    by_doc_id: Dict[str, List[int]] = {}
    for i, row in enumerate(source_rows):
        by_doc_id.setdefault((row.get('Document ID') or '').strip(), []).append(i)

    claimed = set()
    matched = []
    unmatched = []
    text_mismatch = 0
    for c in comments:
        cid = c.get('id', '') or ''
        base, _, suffix = cid.partition('#')
        candidates = [i for i in by_doc_id.get(base, []) if i not in claimed]
        if suffix:
            by_tn = [i for i in candidates if (source_rows[i].get('Tracking Number') or '').strip() == suffix]
            if by_tn:
                candidates = by_tn
        if len(candidates) > 1:
            ctext = (c.get('comment_text') or '').strip()
            by_text = [i for i in candidates if (source_rows[i].get('Comment') or '').strip() == ctext]
            if by_text:
                candidates = by_text
        if not candidates:
            unmatched.append(cid)
            matched.append(None)
            continue
        idx = candidates[0]
        claimed.add(idx)
        matched.append(source_rows[idx])
        if (source_rows[idx].get('Comment') or '').strip() != (c.get('comment_text') or '').strip():
            text_mismatch += 1

    if unmatched:
        raise RuntimeError(
            f"{len(unmatched)} analyzed comments have no row in the source CSV "
            f"(first few: {unmatched[:5]}). The CSV is older than the parquet — re-export it."
        )
    if text_mismatch:
        print(f"  note: {text_mismatch} matched rows differ in comment text "
              f"(attachment-only or re-edited submissions)")
    return matched


def export_comments_csv(comments: List[Dict[str, Any]], output_path: str, source_csv: str) -> None:
    """Write one row per comment: every original bulk-export column, then every
    covariate this tool derived (analysis fields, per-option indicators, regex
    flags and values, dedup/campaign membership, attachment text).

    Columns are derived from analyzer_config.yaml and the data, so this stays
    generic across regulations.
    """
    csv.field_size_limit(2 ** 31 - 1)
    with open(source_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        source_fields = list(reader.fieldnames or [])
        source_rows = list(reader)
    print(f"  source CSV: {len(source_rows):,} rows x {len(source_fields)} columns")

    matched = match_source_rows(comments, source_rows)

    fields = load_fields() or []
    _, regex_patterns = compute_value_sections(comments, fields)

    # Derived column set, all discovered rather than hardcoded.
    analysis_keys = sorted({k for c in comments for k in (c.get('analysis') or {})})
    skip_top = {'id', 'text', 'comment_text', 'analysis', 'attachment_status',
                'submitter', 'organization', 'date', 'received_date'}
    top_keys = [k for k in (comments[0].keys() if comments else []) if k not in skip_top]
    option_cols = []  # (column name, field name, option value) for multi/single enums
    for fld in fields:
        if fld.get('type') in ('multi_enum', 'single_enum') and fld.get('options'):
            for opt in fld['options']:
                option_cols.append((f"{fld['name']}__{_export_slug(opt)}", fld['name'], opt))

    derived = (['analyzer_id', 'position']
               + analysis_keys
               + [c[0] for c in option_cols]
               + [f'{name}_values' for name in regex_patterns]
               + top_keys
               + ['attachment_char_count', 'attachment_files_total', 'attachment_files_failed'])

    collisions = sorted(set(derived) & set(source_fields))
    if collisions:
        raise RuntimeError(f"derived column names collide with bulk-export columns: {collisions}")
    if len(derived) != len(set(derived)):
        dupes = sorted({c for c in derived if derived.count(c) > 1})
        raise RuntimeError(f"duplicate derived column names: {dupes}")

    header = source_fields + derived
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=header, extrasaction='raise')
        writer.writeheader()
        for c, src in zip(comments, matched):
            analysis = c.get('analysis') or {}
            status = c.get('attachment_status') or {}
            row = dict(src)
            row['analyzer_id'] = c.get('id', '')
            row['position'] = comment_position(c)
            for k in analysis_keys:
                row[k] = _export_value(analysis.get(k))
            for col, fname, opt in option_cols:
                val = analysis.get(fname)
                if hasattr(val, 'tolist'):
                    val = val.tolist()
                if isinstance(val, (list, tuple)):
                    row[col] = str(opt in val)
                else:
                    row[col] = str(val == opt)
            for name, pat in regex_patterns.items():
                row[f'{name}_values'] = ' | '.join(extract_regex_values(c.get('comment_text', '') or '', pat))
            for k in top_keys:
                row[k] = _export_value(c.get(k))
            row['attachment_char_count'] = len(c.get('attachment_text') or '')
            row['attachment_files_total'] = _export_value(status.get('total'))
            row['attachment_files_failed'] = _export_value(status.get('failed'))
            writer.writerow(row)

    size_mb = os.path.getsize(output_path) / 1e6
    print(f"  wrote {output_path}: {len(comments):,} rows x {len(header)} columns ({size_mb:.1f} MB)")
    print(f"  {len(source_fields)} bulk-export columns + {len(derived)} derived")


def main():
    parser = argparse.ArgumentParser(description='Generate HTML report from comment analysis results')
    parser.add_argument('--json', type=str, help='Input JSON file')
    parser.add_argument('--parquet', type=str, default='analyzed_comments.parquet', help='Input Parquet file')
    parser.add_argument('--output', type=str, default='index.html', help='Output HTML file')
    parser.add_argument('--model', type=str, default=None, help='Model name to show in the report footer (overrides the value recorded in the data)')
    parser.add_argument('--export-csv', type=str, default=None, help='Write one row per comment (bulk-export columns + derived covariates) to this CSV instead of rendering the report')
    parser.add_argument('--source-csv', type=str, default='source.csv', help='Bulk-export CSV to take the original columns from (with --export-csv)')

    args = parser.parse_args()

    if args.json and os.path.exists(args.json):
        print(f"Loading results from {args.json}...")
        comments, _ = load_results(args.json)
    elif os.path.exists(args.parquet):
        print(f"Loading results from {args.parquet}...")
        comments = load_results_parquet(args.parquet)
    else:
        print(f"Error: Neither JSON file '{args.json}' nor Parquet file '{args.parquet}' found")
        return

    if args.export_csv:
        print(f"Exporting one row per comment to {args.export_csv}...")
        export_comments_csv(comments, args.export_csv, args.source_csv)
        return

    # field_analysis still needed for pipeline.py compatibility
    field_analysis = {}

    print("Computing briefing stats...")
    print(f"Generating HTML report: {args.output}")
    generate_html(comments, {}, field_analysis, args.output, model_used=args.model)

    print(f"Report generated: {args.output}")
    print(f"{len(comments):,} comments analyzed")


if __name__ == "__main__":
    main()
