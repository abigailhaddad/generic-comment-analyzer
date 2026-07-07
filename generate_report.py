#!/usr/bin/env python3
"""
Generate HTML Report from Comment Analysis Results

Creates an interactive HTML report with briefing summary and searchable table.
Uses Jinja2 template (report_template.html) for HTML generation.
"""

import argparse
import json
import os
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

import yaml
import pandas as pd
from jinja2 import Environment, FileSystemLoader

_SMALL_WORDS = {'the', 'of', 'a', 'an', 'and', 'or', 'to', 'in', 'on', 'for'}


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
        entity_submitters[entity].append({
            'name': 'Anonymous' if (c.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else c.get('submitter', '').strip(),
            'org': c.get('organization', '').strip(),
            'id': c.get('id', ''),
            'entity_name': analysis.get('entity_name', ''),
            'entity_name_score': analysis.get('entity_name_match_score', ''),
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
        pct = round(count / total * 100, 1) if total else 0
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
        'oppose_pct': round(oppose_count / total * 100, 1) if total else 0,
        'support_count': support_count,
        'support_pct': round(support_count / total * 100, 1) if total else 0,
        'unclear_count': unclear_count,
        'unclear_pct': round(unclear_count / total * 100, 1) if total else 0,
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

    Deduplicates by comment text to keep the table manageable.
    """
    campaign_id_to_rank = campaign_id_to_rank or {}
    campaign_id_to_stance = campaign_id_to_stance or {}
    flag_keys = flag_keys or []
    regex_value_patterns = regex_value_patterns or {}
    seen_texts = set()
    rows = []
    for comment in comments:
        # Deduplicate: only include each unique text once in the table
        text_key = ((comment.get('comment_text') or '') + (comment.get('attachment_text') or '')).strip().lower()
        if text_key in seen_texts:
            continue
        seen_texts.add(text_key)

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

        positions = [s for s in stance_data if s.startswith('Position:')]
        concerns = [s for s in stance_data if s.startswith('Concern:')]
        position_html = ' '.join(f'<span class="stance-tag tag-position">{s.replace("Position: ", "")}</span>' for s in positions)
        concerns_html = ' '.join(f'<span class="stance-tag tag-concern">{s.replace("Concern: ", "")}</span>' for s in concerns)

        # Date
        date_str = comment.get('date', '')
        formatted_date = ''
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                formatted_date = dt.strftime('%Y-%m-%d')
            except Exception:
                formatted_date = date_str[:10] if len(date_str) >= 10 else date_str

        # Comment preview for table
        comment_text = comment.get('comment_text', '') or ''
        comment_preview = comment_text[:200] + '...' if len(comment_text) > 200 else comment_text

        rows.append({
            'id': comment.get('id', ''),
            'date': formatted_date,
            'submitter': 'Anonymous' if (comment.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else comment.get('submitter', '').strip(),
            'organization': comment.get('organization', '') or '',
            'entity_type': analysis.get('entity_type', 'Individual/Other'),
            'entity_name': analysis.get('entity_name', ''),
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


def determine_model(comments: List[Dict[str, Any]], override: str = None) -> str:
    """Determine the model to show in the report footer from the data itself.

    Precedence: explicit override (e.g. --model) > most-common `model_used`
    value recorded in the parquet > 'unknown'. Never a hardcoded model string.
    """
    if override:
        return override
    vals = [c.get('model_used') for c in comments if c.get('model_used')]
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
        label = humanize_flag_label(key, cfg)
        matched = []
        count = 0
        for c in comments:
            if c.get(key):
                count += 1
                if len(matched) < 500:
                    ct = c.get('comment_text', '') or ''
                    matched.append({
                        'name': 'Anonymous' if (c.get('submitter', '') or '').strip() in ('Anonymous Anonymous', '') else c.get('submitter', '').strip(),
                        'id': c.get('id', ''),
                        'sentence': extract_matching_sentence(ct, patterns),
                    })
        sections.append({
            'key': key,
            'label': label,
            'description': description,
            'count': count,
            'pct': round(count / total * 100, 1) if total else 0,
            'patterns': patterns,
            'comments': matched,
        })
    return sections


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
    flag_keys = list(flags_cfg.keys())
    report_config = load_report_config()
    colors = load_colors(report_config)
    accent_rgb = _hex_to_rgb(colors['accent'])
    source_url = report_config.get('source_url') or None
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

    # Read-the-Rule page — only when the regulation has proposed-rule text prepared.
    rule_sections = load_rule_sections()
    rule_page_url = 'read-the-rule.html' if rule_sections else None
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
        show_stance_cards=show_stance_cards,
        show_entity_cards=show_entity_cards,
        rule_page_url=rule_page_url,
        source_url=source_url,
        generated_time=generated_time,
        model_used=model_name,
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


def main():
    parser = argparse.ArgumentParser(description='Generate HTML report from comment analysis results')
    parser.add_argument('--json', type=str, help='Input JSON file')
    parser.add_argument('--parquet', type=str, default='analyzed_comments.parquet', help='Input Parquet file')
    parser.add_argument('--output', type=str, default='index.html', help='Output HTML file')
    parser.add_argument('--model', type=str, default=None, help='Model name to show in the report footer (overrides the value recorded in the data)')

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

    # field_analysis still needed for pipeline.py compatibility
    field_analysis = {}

    print("Computing briefing stats...")
    print(f"Generating HTML report: {args.output}")
    generate_html(comments, {}, field_analysis, args.output, model_used=args.model)

    print(f"Report generated: {args.output}")
    print(f"{len(comments):,} comments analyzed")


if __name__ == "__main__":
    main()
