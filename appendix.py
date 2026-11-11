#!/usr/bin/env python3
"""On-request appendix: ask one-off questions of the comments, answered by an LLM.

NOT part of the report build or the publish workflow. Nothing imports this,
no workflow calls it, and deploy_report.sh / the preview sync copy explicit file
allowlists that do not include its outputs. Run it by hand, locally:

    python appendix.py --regulation USBC-2026-0628 --profile      # what the parquet already holds that is reusable (free)
    python appendix.py --regulation USBC-2026-0628 --dry-run      # counts + token estimate, no LLM calls
    python appendix.py --regulation USBC-2026-0628 --plan         # only decide what to reuse from the parquet (tiny cost)
    python appendix.py --regulation USBC-2026-0628 --limit 25     # pilot on a random 25 units per question
    python appendix.py --regulation USBC-2026-0628                # every question in the file
    python appendix.py --regulation USBC-2026-0628 --question organizations
    python appendix.py --regulation USBC-2026-0628 --render-only  # rebuild the page from saved results
    python appendix.py --regulation USBC-2026-0628 --out-dir some/folder   # put every output (and read full_run.parquet) there

Questions live in regulations/<slug>/appendix_questions.yaml (local, untracked). Format:

    defaults:                         # optional; every key below can also be set per question
      map_model: gpt-5.4-nano         # reads each passage
      reduce_model: gpt-5.4           # plans the reuse, merges near-duplicates, writes the answer
      max_chars: 4000                 # passage size; longer comments are split into chunks
      max_chunks: 8                   # cap on chunks sent per comment (0 = no cap)
      max_themes: 15                  # target number of themes for summary / findings
      workers: 8
    questions:
      - id: gender_identity           # lowercase letters, digits, underscores
        title: Gender identity in the comments
        kind: summary                 # list (names) | summary (points made) | findings (information provided)
        scope: collapsed              # collapsed (campaigns read once) | rows (every comment read)
        instruction: |
          Free text: what to look for in each passage and how to report it.
        # Everything below is optional.
        prefilter:                    # free text-side filter; a text passes if it matches any part
          keywords: ['gender', 'transgender']         # regexes, case-insensitive
          entity_types_exclude: ['Individual/Other']  # also pass comments classified as any other entity type
        salvage: auto                 # auto (default) | off | {tags: [...], flags: [...], entity_types: [...], hints: [...]}
        hints: [rationale]            # force which stored fields the model sees; default: whatever the plan picks
        chunk_policy: auto            # auto | matching (keyword chunks only) | all
        roles:                        # classify how each named entity relates to the comment, then keep only some;
          submitter: "who wrote this comment"     #   this is a rule applied in code, which holds where "please do not
          addressee: "who it is directed to"      #   list X" in the instruction does not
        keep_roles: [submitter]       # items with any other role are dropped (and counted in the results)

Reusing the standard pipeline's work ("salvage"). For every question the script
first profiles what full_run.parquet already holds: the concern/position tags
the pipeline assigned, its boolean regex flags, entity types, and the stored
text fields (names, quotes, rationales). One small, cached planning call per
question then picks which of those are relevant to THAT question, choosing only
names that really exist in the profile (anything else is discarded). The plan is
used two ways:
  - as extra candidate signals: a comment the pipeline already tagged as relevant
    is read even when none of the question's keywords appear in it;
  - as context: the chosen stored fields are shown to the model beside the
    passage, so it verifies and fills gaps instead of starting cold.
Stored fields are never accepted as answers on their own: they are noisy (a stored
"entity name" is sometimes an organization, sometimes a justifying phrase, sometimes
a person), so the model always reads the text and every quote is checked against it.
A plan is cached on the question and on WHICH signals exist, not on their counts, so it
is reused as the corpus grows and redone only if the question changes or a tag, flag,
entity type or field appears or disappears. The plan is saved to the results and shown
on the page; --plan prints it, including
how well its candidates agree with the keywords, before any real spending. Set
`salvage: off` to ignore it, or give explicit lists to skip the planning call.

Output, all next to the parquet and all gitignored:

    appendix.html           the page
    appendix_<id>.csv       one table per question
    appendix_results.json   the structured answers (what --render-only reads)
    appendix_cache.jsonl    every LLM response, so re-runs and new questions reuse earlier work

How it keeps token use down (the parquet is read, never changed):
  - Comments are collapsed into "units": each campaign becomes ONE unit (its
    canonical text) weighted by its member count, and identical texts are
    merged. Counts are weighted back to real comment numbers.
  - Free candidate filters (keywords, plus the salvage signals) run before any
    reading call.
  - Long documents are split into chunks; with keywords, only the chunks that
    actually contain a keyword are sent.
  - Every response is cached on (prompt, model, text), so editing one question
    only re-runs that question.
  - Stage 1 (cheap model) extracts structured items per chunk; stage 2 merges
    near-duplicates and writes the answer from the merged items only.
Every quote an item cites must appear verbatim in the passage it came from, or
the item is dropped, so the page cannot show an invented quote.
"""
import argparse
import concurrent.futures as cf
import csv
import hashlib
import html
import json
import random
import re
import sys
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml
from pydantic import BaseModel, Field

import comments_received
from generate_report import DEFAULT_COLORS, comment_position

ROOT = Path(__file__).resolve().parent

PROMPT_VERSION = 2  # bump to invalidate every cached response when the prompts change
DEFAULT_MAP_MODEL = 'gpt-5.4-nano'   # same default as pipeline.py
DEFAULT_REDUCE_MODEL = 'gpt-5.4'     # planning, merge and write-up run once per question, so a stronger model is cheap here
CHARS_PER_TOKEN = 4
PARQUET_COLUMNS = ['id', 'text', 'campaign_id', 'campaign_size', 'campaign_canonical', 'analysis']
# 'state_map' is not an LLM question: it makes no model calls and needs no instruction/scope/prefilter,
# just a pure read of the already-stored `analysis.state_identified` + stance fields (see run_state_map_question).
KINDS = ('list', 'summary', 'findings', 'state_map')
SCOPES = ('collapsed', 'rows')
CHUNK_POLICIES = ('auto', 'matching', 'all')
SALVAGE_KEYS = ('tags', 'flags', 'entity_types', 'hints')

# Two-letter abbreviation -> full name, for the state_map question. Matches the VALID_STATES set that
# pipeline.py checks `state_identified` against: the 50 states, DC and the four territories. A territory
# shows zero comments unless a commenter's text actually places them there.
US_STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts',
    'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana',
    'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico',
    'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'DC': 'District of Columbia',
    'PR': 'Puerto Rico', 'GU': 'Guam', 'VI': 'U.S. Virgin Islands', 'AS': 'American Samoa',
}
QUESTION_KEYS = {'id', 'title', 'kind', 'scope', 'instruction', 'prefilter', 'hints', 'chunk_policy', 'salvage',
                 'roles', 'keep_roles', 'map_model', 'reduce_model', 'max_chars', 'max_chunks', 'max_themes'}
FIELD_SKIP = {'stances'}   # handled as tags, not as a text field
UNSELECTIVE_SHARE = 0.35   # a plan signal that matches more than this share of the texts is not narrowing anything
LIST_MERGE_BATCH = 150     # smaller than themes: the model checks names pairwise, and attention drops over long lists


# --------------------------------------------------------------------------- schemas

class Item(BaseModel):
    label: str = Field(description="Short name of the thing found, as the question's output guide describes.")
    category: str = Field(description="Type or theme of the item; empty string if not applicable.")
    detail: str = Field(description="What the passage says about it, as the output guide describes.")
    quote: str = Field(description="Exact words copied verbatim from the passage that support this item.")


class MapResult(BaseModel):
    items: List[Item] = Field(description="Every relevant item in the passage. Empty list if nothing is relevant.")


class RoleItem(BaseModel):
    label: str = Field(description="Short name of the thing found, as the question's output guide describes.")
    category: str = Field(description="Type or theme of the item; empty string if not applicable.")
    detail: str = Field(description="What the passage says about it, as the output guide describes.")
    quote: str = Field(description="Exact words copied verbatim from the passage that support this item.")
    role: str = Field(description="Exactly one of the role names listed in the instructions: how this item relates to the comment.")


class RoleMapResult(BaseModel):
    items: List[RoleItem] = Field(description="Every item the question concerns that the passage names, each with its role. Empty list if none.")


def map_schema(q: dict):
    """Questions that define `roles` get a stricter extraction schema; others keep the original one (and its cache)."""
    return RoleMapResult if q.get('roles') else MapResult


class MergedGroup(BaseModel):
    label: str = Field(description="Best label for the merged group.")
    category: str = Field(description="Category for the merged group; empty string if none.")
    member_numbers: List[int] = Field(description="Numbers of the entries merged into this group.")


class MergeResult(BaseModel):
    groups: List[MergedGroup] = Field(description="The merged groups.")


class GroupSummary(BaseModel):
    number: int = Field(description="The group number being summarized.")
    summary: str = Field(description="One to three neutral sentences on what this group's comments say.")


class SynthesisResult(BaseModel):
    answer: str = Field(description="The answer to the analyst's question, in a few short paragraphs separated by blank lines.")
    group_summaries: List[GroupSummary] = Field(description="A summary for each numbered group.")


class SalvagePlan(BaseModel):
    tags: List[str] = Field(description="Exact tag names from the TAGS list whose presence suggests a comment is relevant. Empty if none.")
    flags: List[str] = Field(description="Exact names from the FLAGS list that suggest relevance. Empty if none.")
    entity_types: List[str] = Field(description="Exact names from the ENTITY TYPES list of commenters likely to be relevant. Empty if none.")
    hints: List[str] = Field(description="Exact names from the FIELDS list whose stored values would help a reader answer the question. Few, or none.")
    reasoning: str = Field(description="One or two sentences on why these were chosen and what was left out.")


class CredentialsError(RuntimeError):
    """The API key was rejected or the account is out of credit: retrying cannot help."""


# --------------------------------------------------------------------------- text helpers

_SPACES = re.compile(r'\s+')
_PUNCT_MAP = str.maketrans({'‘': "'", '’': "'", '“': '"', '”': '"',
                            '–': '-', '—': '-', ' ': ' '})
_NAME_NOISE = re.compile(r'\b(the|inc|llc|ltd|corp|corporation|co|us|usa)\b')
_UNITED_STATES = re.compile(r'\bunited states( of america)?\b')


def norm_text(s: Optional[str]) -> str:
    """Case, whitespace, and typographic-quote insensitive form used for matching."""
    s = unicodedata.normalize('NFKC', s or '').translate(_PUNCT_MAP).lower()
    return _SPACES.sub(' ', s).strip()


def quote_in_text(quote: Optional[str], text_norm: str) -> bool:
    """True if every ellipsis-separated piece of the quote appears in the (already normalized) text."""
    q = norm_text(quote)
    if len(q) < 3:
        return False
    pieces = [p.strip(' .') for p in q.split('...')]
    pieces = [p for p in pieces if len(p) >= 3]
    return bool(pieces) and all(p in text_norm for p in pieces)


def label_key(label: str) -> str:
    """Grouping key, so trivial variants of a name collapse without a model call.

    Case, punctuation, periods, 'the', corporate suffixes and the country prefix are ignored, so "U.S. Census Bureau",
    "US Census Bureau", "United States Census Bureau" and "the Census Bureau" are one entry.
    """
    s = unicodedata.normalize('NFKC', label or '').lower().replace('.', '')
    s = _UNITED_STATES.sub(' ', s)
    s = re.sub(r'[^a-z0-9 ]+', ' ', s)
    s = _SPACES.sub(' ', _NAME_NOISE.sub(' ', s)).strip()
    return s or (label or '').strip().lower()


def sort_key(key: str) -> Tuple[str, str]:
    """Order entries so look-alike names sit next to each other (tokens sorted), which is where a merge model checks."""
    return ' '.join(sorted(key.split())), key


def chunk_text(text: str, size: int) -> List[str]:
    """Split on paragraph breaks into pieces of at most `size` characters (hard-splitting oversized paragraphs)."""
    text = text or ''
    if len(text) <= size:
        return [text]
    chunks, cur = [], ''
    for para in re.split(r'\n\s*\n', text):
        while len(para) > size:
            cut = para.rfind('. ', 0, size)
            cut = cut + 1 if cut > size // 2 else size
            if cur:
                chunks.append(cur)
                cur = ''
            chunks.append(para[:cut].strip())
            para = para[cut:].strip()
        if cur and len(cur) + len(para) + 2 > size:
            chunks.append(cur)
            cur = ''
        cur = f'{cur}\n\n{para}' if cur else para
    if cur:
        chunks.append(cur)
    return [c for c in chunks if c.strip()]


def short_hash(*parts: str) -> str:
    return hashlib.sha256('\x1f'.join(parts).encode('utf-8')).hexdigest()[:24]


# --------------------------------------------------------------------------- units

@dataclass
class Unit:
    uid: str
    text: str
    weight: int                       # real comments this unit stands for
    ids: List[str]                    # member comment ids (first is the representative)
    stances: Counter = field(default_factory=Counter)   # position -> member count
    entity_type: str = ''
    tags: set = field(default_factory=set)       # every concern/position tag any member carries
    flags: set = field(default_factory=set)      # every boolean flag any member has set
    fields: Dict[str, str] = field(default_factory=dict)   # the representative's stored text fields (truncated)


def flag_columns(df: pd.DataFrame) -> List[str]:
    """Boolean columns the pipeline wrote (the regex flags), excluding bookkeeping."""
    return [c for c in df.columns if c != 'is_unique' and df[c].dtype == bool]


def _facts(analysis: Any) -> Tuple[str, str, List[str], Dict[str, str]]:
    """(position, entity type, concern tags, stored text fields) from one comment's stored analysis."""
    a = analysis if isinstance(analysis, dict) else {}
    etype = str(a.get('verified_entity_type') or a.get('entity_type') or '')
    raw = a.get('stances')
    raw = raw.tolist() if hasattr(raw, 'tolist') else (raw or [])
    tags = [str(t) for t in raw]
    fields: Dict[str, str] = {}
    for k, v in a.items():
        if k in FIELD_SKIP:
            continue
        v = v.tolist() if hasattr(v, 'tolist') else v
        if isinstance(v, list):
            v = '; '.join(str(x) for x in v[:10] if isinstance(x, str) and x.strip())
        if isinstance(v, str) and v.strip():
            fields[k] = v.strip()[:300]
    return comment_position({'analysis': a}), etype, tags, fields


def build_units(df: pd.DataFrame, scope: str) -> List[Unit]:
    """Collapse the corpus into weighted units.

    collapsed: each campaign -> one unit (its canonical text) + identical-text merge for everything else.
    rows:      every comment is its own unit (identical texts still merged) -- use when the answer
               lives in the personalised part of a comment, e.g. who signed it.
    """
    df = df.reset_index(drop=True)
    ids = df['id'].astype(str).tolist()
    texts = df['text'].fillna('').astype(str).tolist()
    facts = [_facts(a) for a in df['analysis'].tolist()]
    fcols = flag_columns(df)
    fmat = df[fcols].to_numpy(dtype=bool) if fcols else None

    def make(uid: str, text: str, poss: List[int], rep: int) -> Unit:
        tags: set = set()
        for p in poss:
            tags.update(facts[p][2])
        flags = {fcols[i] for i in np.flatnonzero(fmat[poss].any(axis=0))} if fmat is not None else set()
        order = [rep] + [p for p in poss if p != rep]
        return Unit(uid=uid, text=text, weight=len(poss), ids=[ids[p] for p in order],
                    stances=Counter(facts[p][0] for p in poss), entity_type=facts[rep][1],
                    tags=tags, flags=flags, fields=dict(facts[rep][3]))

    units: List[Unit] = []
    remaining = list(range(len(df)))
    if scope == 'collapsed' and df['campaign_id'].notna().any():
        in_campaign = df['campaign_id'].notna()
        for cid, grp in df[in_campaign].groupby('campaign_id', sort=True):
            poss = grp.index.tolist()
            rep = poss[int(np.argmax([len(texts[p]) for p in poss]))]
            canon = grp['campaign_canonical'].dropna()
            canon = str(canon.iloc[0]) if len(canon) and str(canon.iloc[0]).strip() else texts[rep]
            if canon.strip():
                units.append(make(f'c{int(cid)}', canon, poss, rep))
        remaining = np.flatnonzero(~in_campaign.to_numpy()).tolist()

    buckets: Dict[str, List[int]] = {}
    for p in remaining:
        if texts[p].strip():
            buckets.setdefault(hashlib.md5(norm_text(texts[p]).encode('utf-8')).hexdigest(), []).append(p)
    for digest, poss in buckets.items():
        units.append(make('t' + digest[:12], texts[poss[0]], poss, poss[0]))
    return units


# --------------------------------------------------------------------------- candidate filter

@dataclass
class Prefilter:
    keywords: List[re.Pattern] = field(default_factory=list)
    entity_types_exclude: set = field(default_factory=set)   # pass any type NOT in here (manual rule)
    tags: set = field(default_factory=set)                   # salvage: pipeline concern tags
    flags: set = field(default_factory=set)                  # salvage: pipeline boolean flags
    entity_types: set = field(default_factory=set)           # salvage: pipeline entity types

    @property
    def has_salvage(self) -> bool:
        return bool(self.tags or self.flags or self.entity_types)

    @property
    def active(self) -> bool:
        return bool(self.keywords or self.entity_types_exclude or self.has_salvage)


def compile_prefilter(spec: Optional[dict], salvage: Optional[dict] = None) -> Prefilter:
    spec = spec or {}
    salvage = salvage or {}
    return Prefilter(keywords=[re.compile(k, re.IGNORECASE) for k in spec.get('keywords') or []],
                     entity_types_exclude=set(spec.get('entity_types_exclude') or []),
                     tags=set(salvage.get('tags') or []), flags=set(salvage.get('flags') or []),
                     entity_types=set(salvage.get('entity_types') or []))


def keyword_hit(unit: Unit, pf: Prefilter) -> bool:
    return any(k.search(unit.text) for k in pf.keywords)


def salvage_hit(unit: Unit, pf: Prefilter) -> bool:
    return bool((pf.tags & unit.tags) or (pf.flags & unit.flags) or (unit.entity_type and unit.entity_type in pf.entity_types))


def unit_matches(unit: Unit, pf: Prefilter) -> bool:
    if not pf.active:
        return True
    if keyword_hit(unit, pf) or salvage_hit(unit, pf):
        return True
    return bool(pf.entity_types_exclude and unit.entity_type and unit.entity_type not in pf.entity_types_exclude)


def select_chunks(text: str, size: int, kws: List[re.Pattern], policy: str, max_chunks: int) -> Tuple[List[str], int]:
    """Chunks worth sending, and how many were left out (by keyword relevance or the max_chunks cap)."""
    chunks = chunk_text(text, size)
    total = len(chunks)
    if policy == 'matching' and kws:
        hit = [c for c in chunks if any(k.search(c) for k in kws)]
        chunks = hit or chunks[:1]
    if max_chunks and len(chunks) > max_chunks:
        chunks = chunks[:max_chunks]
    return chunks, total - len(chunks)


# --------------------------------------------------------------------------- what the parquet holds

@dataclass
class Signal:
    kind: str          # 'tag' | 'flag' | 'entity_type' | 'field'
    name: str
    description: str
    comments: int
    examples: List[str] = field(default_factory=list)


def build_profile(df: pd.DataFrame, cfg: Optional[dict]) -> List[Signal]:
    """Everything the standard pipeline stored that a question could lean on, with counts.

    Built from the data itself; analyzer_config.yaml only supplies descriptions for names it recognizes.
    """
    cfg = cfg or {}
    tag_desc = {str(s.get('name', '')).strip(): str(s.get('indicator', '')).strip()
                for s in cfg.get('stances') or [] if isinstance(s, dict)}
    flag_desc = {k: str(v.get('description') or v.get('label') or '')
                 for k, v in (cfg.get('regex_flags') or {}).items() if isinstance(v, dict)}
    field_desc = {str(f.get('name')): ' '.join(x for x in (str(f.get('label') or ''), str(f.get('prompt') or '')) if x)
                  for f in cfg.get('fields') or [] if isinstance(f, dict)}
    tag_counts: Counter = Counter()
    type_counts: Counter = Counter()
    fill: Counter = Counter()
    examples: Dict[str, List[str]] = {}
    for a in df['analysis']:
        _, etype, tags, fields = _facts(a)
        tag_counts.update(set(tags))
        if etype:
            type_counts[etype] += 1
        for k, v in fields.items():
            fill[k] += 1
            ex = examples.setdefault(k, [])
            if len(ex) < 3 and v[:60] not in ex:
                ex.append(v[:60])
    out = [Signal('tag', n, tag_desc.get(n, ''), c) for n, c in sorted(tag_counts.items())]
    out += [Signal('flag', c, flag_desc.get(c, ''), int(df[c].sum())) for c in flag_columns(df) if int(df[c].sum())]
    out += [Signal('entity_type', n, '', c) for n, c in sorted(type_counts.items())]
    out.append(Signal('field', 'stance', "the comment's position on the rule (Oppose / Support / ...)", len(df)))
    out.append(Signal('field', 'entity_type', 'what kind of commenter the pipeline decided this is', sum(type_counts.values())))
    out += [Signal('field', k, field_desc.get(k, ''), n, examples.get(k, []))
            for k, n in sorted(fill.items()) if n >= 5 and k not in ('entity_type', 'verified_entity_type')]
    return out


def render_profile(profile: List[Signal]) -> str:
    def sec(kind: str, title: str, fmt) -> str:
        rows = [fmt(s) for s in profile if s.kind == kind]
        return f'{title}\n' + ('\n'.join(rows) if rows else '(none)')
    desc = lambda s: f' -- {s.description[:220]}' if s.description else ''
    return '\n\n'.join([
        sec('tag', 'TAGS (concern / position tags the earlier analysis assigned; count = comments tagged)',
            lambda s: f'- "{s.name}" ({s.comments:,}){desc(s)}'),
        sec('flag', 'FLAGS (yes/no markers the earlier analysis set from keyword patterns)',
            lambda s: f'- {s.name} ({s.comments:,}){desc(s)}'),
        sec('entity_type', 'ENTITY TYPES (kinds of commenter the earlier analysis assigned)',
            lambda s: f'- "{s.name}" ({s.comments:,})'),
        sec('field', 'FIELDS (stored values available per comment; count = comments where filled)',
            lambda s: f'- {s.name} ({s.comments:,}){desc(s)}' + (f' e.g. {s.examples}' if s.examples else '')),
    ])


PLAN_SYSTEM = (
    "You help an analyst answer a question about public comments on a federal rule. An earlier automated analysis "
    "already stored results for every comment (listed below). Decide which of those stored results are useful for "
    "THIS question.\n"
    "- tags / flags / entity_types: signals that a comment is likely relevant. They only choose which comments get "
    "read (a text check and a reader follow), so be inclusive, but skip ones unrelated to the question.\n"
    "- hints: stored fields whose values would help a reader of a passage answer the question. Choose few, or none.\n"
    "Copy names exactly from the lists. Never invent a name. If nothing in a category applies, return an empty list.")


def plan_messages(q: dict, profile: List[Signal]) -> List[dict]:
    user = (f"QUESTION TITLE: {q['title']}\n\nQUESTION:\n{q['instruction'].strip()}\n\n"
            f"STORED RESULTS AVAILABLE:\n\n{render_profile(profile)}")
    return [{'role': 'system', 'content': PLAN_SYSTEM}, {'role': 'user', 'content': user}]


def plan_key(q: dict, profile: List[Signal]) -> str:
    """Cache key for a reuse plan: the question plus WHICH signals exist, never their counts or example values.

    Counts and examples change every time the corpus grows; keying on them would re-plan after every data refresh.
    A plan is redone only when the question changes or a signal appears, disappears or is re-described.
    """
    structure = json.dumps([(s.kind, s.name, s.description) for s in profile])
    return short_hash(str(PROMPT_VERSION), q['reduce_model'], SalvagePlan.__name__, PLAN_SYSTEM,
                      q['title'], q['instruction'].strip(), structure)


def validate_plan(raw: dict, profile: List[Signal]) -> dict:
    """Keep only names that really exist in the profile; count what was discarded."""
    valid = {k: {s.name for s in profile if s.kind == k} for k in ('tag', 'flag', 'entity_type', 'field')}
    dropped = 0
    out: Dict[str, Any] = {}
    for key, kind in (('tags', 'tag'), ('flags', 'flag'), ('entity_types', 'entity_type'), ('hints', 'field')):
        vals = list(dict.fromkeys(raw.get(key) or []))
        out[key] = [v for v in vals if v in valid[kind]]
        dropped += len(vals) - len(out[key])
    out['dropped'] = dropped
    return out


def prune_unselective(salvage: dict, units: List[Unit], max_share: Optional[float] = None) -> dict:
    """Drop planned signals that match most of the texts: a filter that admits nearly everything filters nothing.

    Measured on this question's own units, so it adapts to the corpus and the scope. Only planned signals are pruned;
    explicit lists in the question file are taken as written.
    """
    if salvage.get('source') != 'planned' or not units:
        return salvage
    max_share = UNSELECTIVE_SHARE if max_share is None else max_share
    tests = {'tags': lambda u, n: n in u.tags, 'flags': lambda u, n: n in u.flags,
             'entity_types': lambda u, n: u.entity_type == n}
    out, dropped = dict(salvage), []
    for key, test in tests.items():
        keep = []
        for name in salvage.get(key, []):
            share = sum(1 for u in units if test(u, name)) / len(units)
            if share > max_share:
                dropped.append({'kind': key, 'name': name, 'share': round(share, 3)})
            else:
                keep.append(name)
        out[key] = keep
    out['unselective'] = dropped
    return out


def resolve_salvage(q: dict, profile: List[Signal], cache: 'Cache', stats: dict, allow_call: bool) -> dict:
    """Decide what to reuse from the parquet for this question.

    Returns {'source': 'off'|'explicit'|'planned'|'pending'|'failed', tags, flags, entity_types, hints, reasoning, dropped}.
    'pending' means no plan is cached yet and calling the model was not allowed (dry run).
    'failed' means the planning call errored; the run continues on the question's own filters.
    """
    empty = {'tags': [], 'flags': [], 'entity_types': [], 'hints': [], 'reasoning': '', 'dropped': 0}
    mode = q.get('salvage', 'auto')
    if mode == 'off':
        return {**empty, 'source': 'off'}
    if isinstance(mode, dict):
        checked = validate_plan(mode, profile)
        if checked['dropped']:
            bad = {k: [v for v in mode.get(k) or [] if v not in checked[k]] for k in SALVAGE_KEYS}
            raise SystemExit(f"question {q['id']}: salvage names not found in the parquet: "
                             f"{ {k: v for k, v in bad.items() if v} }")
        return {**empty, **checked, 'source': 'explicit', 'reasoning': 'set explicitly in the question file'}
    key = plan_key(q, profile)
    hit = cache.get(key)
    if hit is None:
        if not allow_call:
            return {**empty, 'source': 'pending'}
        try:
            parsed, usage = call_llm(q['reduce_model'], plan_messages(q, profile), SalvagePlan)
        except CredentialsError:
            raise
        except Exception as e:
            # Reuse is an optimization, so a failed plan must not sink the run. It is not cached, so the next run retries.
            msg = str(e)[:160]
            print(f"[{q['id']}] reuse plan failed ({msg}); continuing with the question's own filters only")
            return {**empty, 'source': 'failed', 'reasoning': f'The planning call failed ({msg}).'}
        hit = parsed.model_dump()
        cache.put(key, hit)
        stats['prompt_tokens'] = stats.get('prompt_tokens', 0) + usage.get('prompt', 0)
        stats['completion_tokens'] = stats.get('completion_tokens', 0) + usage.get('completion', 0)
    checked = validate_plan(hit, profile)
    return {**empty, **checked, 'source': 'planned', 'reasoning': (hit.get('reasoning') or '').strip()}


# --------------------------------------------------------------------------- prompts

KIND_GUIDE = {
    'list': ("Each item is one distinct named entity. `label` = its name exactly as written in the passage; "
             "`category` = its type if the passage states or clearly shows it, otherwise an empty string; "
             "`detail` = one short note on how it appears in the passage."),
    'summary': ("Each item is one distinct point the commenter makes. `label` = a short name for the point "
                "(2-8 words); `category` = a broad theme; `detail` = what the commenter says, in one or two sentences."),
    'findings': ("Each item is one distinct piece of information the commenter provides. `label` = a short title "
                 "for it; `category` = the type of information; `detail` = precisely what is provided, including any "
                 "figures and the source the commenter names (write 'no source given' if none)."),
}

SYSTEM_TEMPLATE = (
    "You extract information from public comments submitted to a federal rulemaking docket ({regulation}). "
    "You are given ONE passage from ONE comment and an analyst's QUESTION. Report only what the passage itself "
    "says; do not infer beyond it and do not use outside knowledge. Every item must include a `quote` copied "
    "verbatim from the passage (exact characters, kept short: usually one or two sentences). If the passage has "
    "nothing relevant to the question, return an empty `items` list.\n\n{guide}\n\nQUESTION:\n{instruction}"
)


def hints_line(unit: Unit, wanted: List[str]) -> str:
    parts = []
    for name in wanted:
        if name == 'entity_type':
            val = unit.entity_type
        elif name == 'stance':
            val = unit.stances.most_common(1)[0][0] if unit.stances else ''
        else:
            val = unit.fields.get(name, '')
        if val:
            parts.append(f"{name.replace('_', ' ')} = {str(val)[:200]}")
    if not parts:
        return ''
    return 'Earlier automated analysis (may be wrong; check it against the passage): ' + '; '.join(parts) + '.\n\n'


ROLE_BLOCK = ("\n\nROLES. Every item must also carry a `role`, exactly one of the names below. Create an item for EVERY "
              "such entity the passage names, whatever its role, so each one can be classified; do not skip an entity "
              "because it looks unimportant. Use the single best-fitting role.\n{roles}")


def build_messages(q: dict, regulation: str, unit: Unit, k: int, n: int, chunk: str) -> List[dict]:
    system = SYSTEM_TEMPLATE.format(regulation=regulation, guide=KIND_GUIDE[q['kind']],
                                    instruction=q['instruction'].strip())
    if q.get('roles'):
        system += ROLE_BLOCK.format(roles='\n'.join(f'- {name}: {desc}' for name, desc in q['roles'].items()))
    user = (f"{hints_line(unit, q['hints'])}PASSAGE (comment {unit.ids[0]}, part {k} of {n}):\n"
            f'"""\n{chunk}\n"""')
    return [{'role': 'system', 'content': system}, {'role': 'user', 'content': user}]


# --------------------------------------------------------------------------- LLM + cache

class Cache:
    """Append-only JSONL of LLM responses keyed by a hash of everything that shaped the request."""

    def __init__(self, path: Optional[Path]):
        self.path = Path(path) if path else None
        self.data: Dict[str, Any] = {}
        if self.path and self.path.exists():
            with open(self.path, encoding='utf-8') as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if 'key' in rec:
                        self.data[rec['key']] = rec.get('value')

    def get(self, key: str) -> Any:
        return self.data.get(key)

    def put(self, key: str, value: Any) -> None:
        self.data[key] = value
        if self.path:
            with open(self.path, 'a', encoding='utf-8') as f:
                f.write(json.dumps({'key': key, 'value': value}) + '\n')


def call_llm(model: str, messages: List[dict], schema, retries: int = 2):
    """One structured completion. Returns (parsed schema instance, {'prompt': n, 'completion': n})."""
    import litellm
    litellm.drop_params = True
    for attempt in range(retries + 1):
        try:
            resp = litellm.completion(model=model, messages=messages, response_format=schema, timeout=180)
            usage = getattr(resp, 'usage', None)
            return (schema(**json.loads(resp.choices[0].message.content)),
                    {'prompt': getattr(usage, 'prompt_tokens', 0) or 0,
                     'completion': getattr(usage, 'completion_tokens', 0) or 0})
        except litellm.AuthenticationError as e:
            raise CredentialsError(str(e)) from e
        except Exception as e:
            if 'insufficient_quota' in str(e) or 'exceeded your current quota' in str(e):
                raise CredentialsError(str(e)) from e
            if attempt == retries:
                raise
            time.sleep(2 ** (attempt + 1))


def cached_call(cache: Cache, model: str, messages: List[dict], schema, stats: dict):
    key = short_hash(str(PROMPT_VERSION), model, schema.__name__, json.dumps(messages, sort_keys=True))
    hit = cache.get(key)
    if hit is not None:
        stats['cached_calls'] = stats.get('cached_calls', 0) + 1
        return schema(**hit)
    parsed, usage = call_llm(model, messages, schema)
    cache.put(key, parsed.model_dump())
    stats['prompt_tokens'] = stats.get('prompt_tokens', 0) + usage.get('prompt', 0)
    stats['completion_tokens'] = stats.get('completion_tokens', 0) + usage.get('completion', 0)
    return schema(**parsed.model_dump())


# --------------------------------------------------------------------------- planning (no LLM)

@dataclass
class Task:
    key: str
    unit: Unit
    chunk: str
    messages: List[dict]


def plan_question(q: dict, units: List[Unit], regulation: str, cache: Cache, limit: int = 0, seed: int = 1,
                  salvage: Optional[dict] = None) -> dict:
    salvage = salvage or {}
    pf = compile_prefilter(q.get('prefilter'), salvage)
    matched = [u for u in units if unit_matches(u, pf)]
    pool = matched
    if limit and len(matched) > limit:
        pool = random.Random(seed).sample(matched, limit)

    # An explicit `hints` list in the question file wins; otherwise use what the salvage plan chose.
    qe = {**q, 'hints': q.get('hints') or salvage.get('hints') or []}

    tasks: List[Task] = []
    truncated = 0
    for u in pool:
        # 'auto': send only keyword-bearing chunks when the unit matched on keywords; a unit that matched only
        # on a pipeline signal or entity type has no keyword to anchor on (a signature can sit anywhere), so it
        # gets all its chunks.
        policy = q['chunk_policy']
        if policy == 'auto':
            policy = 'matching' if keyword_hit(u, pf) else 'all'
        chunks, left_out = select_chunks(u.text, q['max_chars'], pf.keywords, policy, q['max_chunks'])
        truncated += 1 if left_out else 0
        for i, c in enumerate(chunks, 1):
            msgs = build_messages(qe, regulation, u, i, len(chunks), c)
            key = short_hash(str(PROMPT_VERSION), q['map_model'], map_schema(q).__name__, json.dumps(msgs, sort_keys=True))
            tasks.append(Task(key, u, c, msgs))

    overlap = None
    if pf.keywords and pf.has_salvage:
        k_set = {u.uid for u in units if keyword_hit(u, pf)}
        s_set = {u.uid for u in units if salvage_hit(u, pf)}
        weight = {u.uid: u.weight for u in units}
        comments = lambda ids: sum(weight[i] for i in ids)
        overlap = {'keyword_units': len(k_set), 'salvage_units': len(s_set), 'both_units': len(k_set & s_set),
                   'salvage_only_units': len(s_set - k_set), 'keyword_only_units': len(k_set - s_set),
                   'keyword_comments': comments(k_set), 'salvage_comments': comments(s_set),
                   'salvage_only_comments': comments(s_set - k_set), 'keyword_only_comments': comments(k_set - s_set)}

    est = lambda ts: sum(len(m['content']) for t in ts for m in t.messages) // CHARS_PER_TOKEN
    uncached = [t for t in tasks if cache.get(t.key) is None]
    return {
        'tasks': tasks,
        'pool': pool,
        'overlap': overlap,
        'stats': {
            'units_in_scope': len(units), 'comments_in_scope': sum(u.weight for u in units),
            'units_matched': len(matched), 'comments_matched': sum(u.weight for u in matched),
            'units_sent': len(pool), 'chunks': len(tasks), 'cached_chunks': len(tasks) - len(uncached),
            'units_with_chunks_left_out': truncated,
            'est_input_tokens': est(tasks), 'est_input_tokens_uncached': est(uncached),
        },
    }


# --------------------------------------------------------------------------- map / merge / synthesize

def run_map(q: dict, plan: dict, cache: Cache, workers: int, stats: dict) -> Dict[str, dict]:
    results: Dict[str, dict] = {}
    todo = []
    for t in plan['tasks']:
        hit = cache.get(t.key)
        if hit is not None:
            results[t.key] = hit
            stats['cached_calls'] = stats.get('cached_calls', 0) + 1
        else:
            todo.append(t)
    stats.setdefault('failed_calls', 0)
    if not todo:
        return results
    done = 0
    with cf.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        schema = map_schema(q)
        futs = {ex.submit(call_llm, q['map_model'], t.messages, schema): t for t in todo}
        try:
            for fut in cf.as_completed(futs):
                t = futs[fut]
                try:
                    parsed, usage = fut.result()
                except CredentialsError:
                    raise
                except Exception as e:
                    stats['failed_calls'] += 1
                    print(f'\n  call failed for {t.unit.ids[0]}: {str(e)[:160]}')
                    continue
                value = parsed.model_dump()
                cache.put(t.key, value)
                results[t.key] = value
                stats['prompt_tokens'] = stats.get('prompt_tokens', 0) + usage.get('prompt', 0)
                stats['completion_tokens'] = stats.get('completion_tokens', 0) + usage.get('completion', 0)
                done += 1
                if done % 25 == 0 or done == len(todo):
                    print(f'  map {done}/{len(todo)}', end='\r', flush=True)
        except CredentialsError:
            for f in futs:
                f.cancel()
            raise
    print()
    return results


def collect_items(tasks: List[Task], results: Dict[str, dict], keep_roles: Optional[List[str]] = None
                  ) -> Tuple[List[dict], int, int, Counter, int]:
    """Verified items, plus (raw item count, dropped for an unverifiable quote, role tally, dropped for role).

    With `keep_roles`, an item whose role is not in the list is dropped here, by rule, not by the model's judgment:
    that is how an addressee or a merely-mentioned body is kept off a list of who actually submitted.
    """
    items, raw, dropped, dropped_role, role_counts = [], 0, 0, 0, Counter()
    keep = set(keep_roles) if keep_roles is not None else None
    for t in tasks:
        res = results.get(t.key)
        if not res:
            continue
        chunk_norm = norm_text(t.chunk)
        for it in res['items']:
            raw += 1
            if not (it.get('label') or '').strip() or not quote_in_text(it.get('quote'), chunk_norm):
                dropped += 1
                continue
            if keep is not None:
                role = (it.get('role') or '').strip()
                role_counts[role or 'unlabeled'] += 1
                if role not in keep:
                    dropped_role += 1
                    continue
            items.append({**it, 'uid': t.unit.uid})
    return items, raw, dropped, role_counts, dropped_role


def cluster_items(items: List[dict]) -> List[dict]:
    by_key: Dict[str, dict] = {}
    for it in items:
        c = by_key.setdefault(label_key(it['label']), {'labels': Counter(), 'cats': Counter(), 'uids': {},
                                                        'details': [], 'quotes': []})
        c['labels'][it['label'].strip()] += 1
        if (it.get('category') or '').strip():
            c['cats'][it['category'].strip()] += 1
        c['uids'][it['uid']] = True
        if (it.get('detail') or '').strip():
            c['details'].append(it['detail'].strip())
        c['quotes'].append((it['uid'], it['quote'].strip()))
    clusters = []
    for key in sorted(by_key, key=sort_key):
        c = by_key[key]
        clusters.append({**c, 'label': c['labels'].most_common(1)[0][0],
                         'category': c['cats'].most_common(1)[0][0] if c['cats'] else ''})
    return clusters


MERGE_LIST = ("You are consolidating a list extracted from public comments. Below are numbered entries. Group "
              "entries that refer to the SAME entity: spelling or case variants, abbreviations versus full names, "
              "punctuation differences. Do NOT merge entries that are genuinely different (for example a local "
              "chapter versus its national body, or an organization versus its parent) unless the label is clearly "
              "the same entity. Entries are ordered so look-alike names sit near each other, but compare each entry "
              "against ALL others, not just its neighbors. Examples that ARE the same entity: \"Census Bureau\" and "
              "\"U.S. Census Bureau\"; \"AAPOR\" and \"American Association for Public Opinion Research\"; \"California "
              "Attorney General\" and \"Attorney General of California\". Every entry number must appear in exactly one "
              "group; an entry with no match is a group of one. For each group give the best `label` (the most "
              "complete correct form), a `category`, and the `member_numbers`.")
MERGE_THEMES = ("You are consolidating points extracted from public comments. Below are numbered entries. Group "
                "entries that make the same point or provide the same kind of information into coherent themes. "
                "Aim for at most about {max_themes} groups overall, with clear plain-language labels. For each group "
                "give a `label`, a broader `category`, and the `member_numbers` merged. If this is one batch of a "
                "larger list, keep theme names general so batches can be combined later. Entries you leave out stay "
                "as their own group.")


def _merge_pass(entries: List[dict], q: dict, cache: Cache, stats: dict, batch_size: int) -> List[dict]:
    system = (MERGE_LIST if q['kind'] == 'list' else MERGE_THEMES.format(max_themes=q['max_themes']))
    system += f"\n\nThe analyst's question, for context:\n{q['instruction'].strip()}"
    out: List[dict] = []
    for start in range(0, len(entries), batch_size):
        batch = entries[start:start + batch_size]
        lines = []
        for n, e in enumerate(batch, 1):
            line = f"{n}. {e['label']}"
            if e.get('category'):
                line += f" [{e['category']}]"
            line += f" (x{e.get('n', 1)})"
            if e.get('detail'):
                line += f" - {e['detail'][:120]}"
            lines.append(line)
        messages = [{'role': 'system', 'content': system}, {'role': 'user', 'content': '\n'.join(lines)}]
        try:
            res = cached_call(cache, q['reduce_model'], messages, MergeResult, stats)
            groups = [g.model_dump() for g in res.groups]
        except CredentialsError:
            raise
        except Exception as e:
            stats['merge_failed'] = stats.get('merge_failed', 0) + 1
            print(f'  merge step failed ({str(e)[:120]}); leaving that batch unmerged')
            groups = []
        assigned = set()
        for g in groups:
            nums = [n for n in g['member_numbers'] if 1 <= n <= len(batch) and n not in assigned]
            if not nums:
                continue
            assigned.update(nums)
            out.append({'label': (g['label'] or '').strip() or batch[nums[0] - 1]['label'],
                        'category': (g['category'] or '').strip(), 'members': [start + n - 1 for n in nums]})
        if groups and len(assigned) < len(batch):
            stats['merge_omitted'] = stats.get('merge_omitted', 0) + len(batch) - len(assigned)   # kept as singles
        for n in range(1, len(batch) + 1):
            if n not in assigned:
                out.append({'label': batch[n - 1]['label'], 'category': batch[n - 1].get('category', ''),
                            'members': [start + n - 1]})
    return out


def merge_clusters(clusters: List[dict], q: dict, cache: Cache, stats: dict, batch_size: Optional[int] = None) -> List[dict]:
    batch_size = batch_size or (LIST_MERGE_BATCH if q['kind'] == 'list' else 300)
    if len(clusters) <= 1:
        return [{'label': c['label'], 'category': c['category'], 'members': [i]} for i, c in enumerate(clusters)]
    entries = [{'label': c['label'], 'category': c['category'], 'n': len(c['uids']),
                'detail': c['details'][0] if c['details'] else ''} for c in clusters]
    groups = _merge_pass(entries, q, cache, stats, batch_size)
    if len(entries) > batch_size and len(groups) > 1:   # combine what the separate batches decided
        pseudo = [{'label': g['label'], 'category': g['category'], 'n': len(g['members'])} for g in groups]
        top = _merge_pass(pseudo, q, cache, stats, batch_size)
        groups = [{'label': t['label'], 'category': t['category'],
                   'members': sorted({m for gi in t['members'] for m in groups[gi]['members']})} for t in top]
    return groups


def assemble_groups(merged: List[dict], clusters: List[dict], units_by_uid: Dict[str, Unit]) -> List[dict]:
    out = []
    for g in merged:
        uids: Dict[str, bool] = {}
        details, quotes, cats = [], [], Counter()
        for m in g['members']:
            c = clusters[m]
            uids.update(c['uids'])
            details.extend(c['details'])
            quotes.extend(c['quotes'])
            cats.update(c['cats'])
        us = sorted((units_by_uid[u] for u in uids), key=lambda u: -u.weight)
        stance: Counter = Counter()
        for u in us:
            stance.update(u.stances)
        seen, picked = set(), []
        for uid, quote in quotes:
            if uid not in seen and len(picked) < 4:
                seen.add(uid)
                picked.append({'id': units_by_uid[uid].ids[0], 'quote': quote})
        out.append({'label': g['label'], 'category': g['category'] or (cats.most_common(1)[0][0] if cats else ''),
                    'texts': len(us), 'comments': sum(u.weight for u in us), 'stance': dict(stance),
                    'example_ids': [u.ids[0] for u in us[:5]], 'quotes': picked,
                    'details': list(dict.fromkeys(details))[:5], 'summary': ''})
    out.sort(key=lambda g: (-g['comments'], g['label'].lower()))
    return out


def synthesize(q: dict, groups: List[dict], cache: Cache, stats: dict, relevant_comments: int, corpus_comments: int) -> Tuple[str, Dict[int, str]]:
    top = groups[:40]
    blocks = []
    for n, g in enumerate(top, 1):
        lines = [f"Group {n}: {g['label']} [{g['category']}] - {g['texts']} distinct texts, {g['comments']} comments"]
        lines += [f"  detail: {d[:250]}" for d in g['details'][:5]]
        lines += [f"  quote: \"{x['quote'][:250]}\"" for x in g['quotes'][:2]]
        blocks.append('\n'.join(lines))
    system = ("You are writing the answer to an analyst's question from structured evidence extracted from public "
              "comments on a federal rule. Write `answer`: a clear, neutral, factual response of a few short paragraphs "
              "that directly answers the question, citing counts where useful, with no claim beyond the evidence. Also "
              "write `group_summaries`: for each numbered group, one to three neutral sentences on what its comments "
              "say. Stay neutral on partisan matters.\n\n"
              f"QUESTION:\n{q['instruction'].strip()}")
    user = (f"{relevant_comments} of {corpus_comments} comments in scope were relevant. "
            f"{len(groups)} groups were found; the {len(top)} largest are shown.\n\n" + '\n\n'.join(blocks))
    res = cached_call(cache, q['reduce_model'],
                      [{'role': 'system', 'content': system}, {'role': 'user', 'content': user}], SynthesisResult, stats)
    return res.answer, {s.number: s.summary for s in res.group_summaries}


# --------------------------------------------------------------------------- one question end to end

def run_question(q: dict, units: List[Unit], regulation: str, cache: Cache, workers: int, limit: int,
                 salvage: Optional[dict] = None) -> dict:
    t0 = time.time()
    plan = plan_question(q, units, regulation, cache, limit=limit, salvage=salvage)
    stats = dict(plan['stats'])
    results = run_map(q, plan, cache, workers, stats)
    items, raw, dropped, role_counts, dropped_role = collect_items(plan['tasks'], results, q.get('keep_roles'))
    stats.update(items_raw=raw, items_dropped_unverified_quote=dropped, items_kept=len(items))
    if q.get('roles'):
        stats.update(items_dropped_role=dropped_role, role_counts=dict(role_counts))
    units_by_uid = {u.uid: u for u in units}
    clusters = cluster_items(items)
    groups = assemble_groups(merge_clusters(clusters, q, cache, stats), clusters, units_by_uid) if clusters else []
    relevant = {u for g_c in clusters for u in g_c['uids']}
    stats['units_relevant'] = len(relevant)
    stats['comments_relevant'] = sum(units_by_uid[u].weight for u in relevant)
    stats['groups'] = len(groups)
    # Position of every comment in scope vs. of the relevant ones, each comment counted once (a group-level sum would
    # count a comment once per group it appears in).
    in_scope: Counter = Counter()
    for u in units:
        in_scope.update(u.stances)
    on_topic: Counter = Counter()
    for uid in relevant:
        on_topic.update(units_by_uid[uid].stances)
    stats['stance_in_scope'], stats['stance_relevant'] = dict(in_scope), dict(on_topic)

    answer = ''
    if q['kind'] == 'list':
        answer = (f"{len(groups)} distinct {'entry' if len(groups) == 1 else 'entries'} found across "
                  f"{stats['units_relevant']} distinct texts ({stats['comments_relevant']} comments).")
    elif groups:
        try:
            answer, summaries = synthesize(q, groups, cache, stats, stats['comments_relevant'], stats['comments_in_scope'])
            for n, g in enumerate(groups[:40], 1):
                g['summary'] = summaries.get(n, '')
        except CredentialsError:
            raise
        except Exception as e:
            stats['synthesis_failed'] = True
            print(f'  write-up step failed ({str(e)[:120]}); showing grouped evidence without a narrative')
    else:
        answer = 'Nothing relevant was found in the passages that were checked.'
    stats['seconds'] = round(time.time() - t0, 1)
    return {'id': q['id'], 'title': q['title'], 'kind': q['kind'], 'scope': q['scope'],
            'instruction': q['instruction'].strip(), 'pilot': bool(limit), 'models': [q['map_model'], q['reduce_model']],
            'salvage': {**(salvage or {'source': 'off'}), 'hints_used': q.get('hints') or (salvage or {}).get('hints') or [],
                        'overlap': plan['overlap']},
            'answer': [p.strip() for p in re.split(r'\n\s*\n', answer or '') if p.strip()],
            'groups': groups, 'stats': stats}


def run_state_map_question(df: pd.DataFrame, q: dict) -> dict:
    """Support/oppose counts per US state -- no LLM calls, no chunking, no units.

    Reads two fields the main pipeline already computed and verified for every comment:
    `state_identified` (a two-letter state guessed from any clue in the text, cleared by
    pipeline.py's validate_analysis if it isn't a real state) and the comment's position, via
    the same `comment_position` helper the rest of this file already uses. A comment with no
    state identified (about 28% of this docket, per inspection) is counted but excluded from
    the per-state breakdown rather than guessed at.
    """
    t0 = time.time()
    per_state: Dict[str, Counter] = {}
    position_totals: Counter = Counter()
    no_state = 0
    for a in df['analysis']:
        a = a if isinstance(a, dict) else {}
        position_totals[comment_position({'analysis': a})] += 1
        state = str(a.get('state_identified') or '').strip().upper()
        if state not in US_STATE_NAMES:
            no_state += 1
            continue
        per_state.setdefault(state, Counter())[comment_position({'analysis': a})] += 1

    states = []
    for abbr, name in US_STATE_NAMES.items():
        counts = dict(per_state.get(abbr) or {})
        states.append({'state': abbr, 'name': name, 'counts': counts, 'total': sum(counts.values())})
    states.sort(key=lambda s: -s['total'])

    comments_total = len(df)
    comments_with_state = comments_total - no_state
    represented = sum(1 for s in states if s['total'])
    answer = (f"{comments_with_state:,} of {comments_total:,} comments ({100 * comments_with_state / comments_total:.1f}%) "
              f"have an identifiable state.")
    return {'id': q['id'], 'title': q['title'], 'kind': 'state_map', 'scope': '', 'instruction': '',
            'pilot': False, 'models': [], 'salvage': None, 'answer': [answer], 'groups': [],
            'states': states,
            'stats': {'comments_in_scope': comments_total, 'comments_with_state': comments_with_state,
                      'comments_no_state': no_state, 'comments_relevant': comments_with_state,
                      'units_relevant': represented, 'units_sent': len(states), 'groups': 0,
                      'position_totals': dict(position_totals), 'seconds': round(time.time() - t0, 1)}}


# --------------------------------------------------------------------------- config + data loading

def load_questions(path: Path) -> List[dict]:
    if not path.exists():
        raise SystemExit(f'No question file at {path}. The expected format is in the docstring at the top of appendix.py.')
    cfg = yaml.safe_load(open(path, encoding='utf-8')) or {}
    defaults = {'map_model': DEFAULT_MAP_MODEL, 'reduce_model': DEFAULT_REDUCE_MODEL, 'max_chars': 4000,
                'max_chunks': 8, 'max_themes': 15, 'workers': 8, 'chunk_policy': 'auto', 'scope': 'collapsed',
                'hints': [], 'salvage': 'auto'}
    defaults.update(cfg.get('defaults') or {})
    out, seen = [], set()
    for raw in cfg.get('questions') or []:
        unknown = set(raw) - QUESTION_KEYS
        qid = raw.get('id', '?')
        if unknown:
            raise SystemExit(f'question {qid}: unknown key(s) {sorted(unknown)}')
        q = {**defaults, **raw}
        if not re.fullmatch(r'[a-z0-9_]+', str(q.get('id', ''))):
            raise SystemExit(f'question id {q.get("id")!r} must be lowercase letters, digits and underscores')
        if q['id'] in seen:
            raise SystemExit(f'duplicate question id {q["id"]}')
        seen.add(q['id'])
        if q.get('kind') not in KINDS:
            raise SystemExit(f'question {qid}: kind must be one of {KINDS}')
        if q['scope'] not in SCOPES:
            raise SystemExit(f'question {qid}: scope must be one of {SCOPES}')
        if q['chunk_policy'] not in CHUNK_POLICIES:
            raise SystemExit(f'question {qid}: chunk_policy must be one of {CHUNK_POLICIES}')
        if not str(q.get('title') or '').strip():
            raise SystemExit(f'question {qid}: title is required')
        if q['kind'] != 'state_map' and not str(q.get('instruction') or '').strip():
            raise SystemExit(f'question {qid}: instruction is required')
        q['instruction'] = str(q.get('instruction') or '').strip()
        q['hints'] = list(q.get('hints') or [])
        if q['salvage'] is False or q['salvage'] is True:   # YAML reads an unquoted `off` / `on` as a boolean
            q['salvage'] = 'auto' if q['salvage'] else 'off'
        sv = q['salvage']
        if not (sv in ('auto', 'off') or (isinstance(sv, dict) and set(sv) <= set(SALVAGE_KEYS))):
            raise SystemExit(f"question {qid}: salvage must be 'auto', 'off', or a mapping with keys from {SALVAGE_KEYS}")
        try:
            compile_prefilter(q.get('prefilter'))
        except re.error as e:
            raise SystemExit(f'question {qid}: bad prefilter regex: {e}')
        roles, keep = q.get('roles'), q.get('keep_roles')
        if bool(roles) != bool(keep):
            raise SystemExit(f'question {qid}: roles and keep_roles must be given together')
        if roles:
            if not (isinstance(roles, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in roles.items())):
                raise SystemExit(f'question {qid}: roles must map each role name to its definition')
            if not (isinstance(keep, list) and set(keep) <= set(roles)):
                raise SystemExit(f'question {qid}: keep_roles must be a list of names defined in roles')
        out.append(q)
    if not out:
        raise SystemExit(f'{path} defines no questions')
    return out


def load_corpus(path: Path) -> pd.DataFrame:
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pq.ParquetFile(path).schema_arrow
    names = schema.names
    missing = [c for c in ('id', 'text', 'analysis') if c not in names]
    if missing:
        raise SystemExit(f'{path} is missing required column(s) {missing}')
    bool_cols = [f.name for f in schema if pa.types.is_boolean(f.type) and f.name != 'is_unique']
    cols = [c for c in PARQUET_COLUMNS if c in names] + [c for c in bool_cols if c not in PARQUET_COLUMNS]
    df = pd.read_parquet(path, columns=cols)
    for c in PARQUET_COLUMNS:
        if c not in df.columns:
            df[c] = None
    # Stored comment text sometimes carries raw HTML entities (e.g. "&rsquo;" for an apostrophe) from
    # the source HTML. Decode them here, once, so chunking, quote verification and what the model reads
    # all see the same real punctuation; a quote the model copies verbatim otherwise fails to match text
    # that still has the entity in it.
    for c in ('text', 'campaign_canonical'):
        if c in df.columns:
            df[c] = df[c].map(lambda v: html.unescape(v) if isinstance(v, str) else v)
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- output

def write_csvs(reg_dir: Path, results: List[dict]) -> None:
    for r in results:
        path = reg_dir / f"appendix_{r['id']}.csv"
        if r['kind'] == 'state_map':
            position_cols = sorted({k for s in r['states'] for k in s['counts']})
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.writer(f)
                w.writerow(['state', 'name'] + position_cols + ['total'])
                for s in r['states']:
                    w.writerow([s['state'], s['name']] + [s['counts'].get(k, 0) for k in position_cols] + [s['total']])
            continue
        stance_cols = sorted({k for g in r['groups'] for k in g['stance']})
        with open(path, 'w', newline='', encoding='utf-8-sig') as f:
            w = csv.writer(f)
            w.writerow(['label', 'category', 'distinct_texts', 'total_comments'] + stance_cols + ['example_comment_ids'])
            for g in r['groups']:
                w.writerow([g['label'], g['category'], g['texts'], g['comments']]
                           + [g['stance'].get(k, 0) for k in stance_cols] + [' '.join(g['example_ids'])])


def render_page(reg_dir: Path, payload: dict, regulation_name: str, colors: dict) -> Path:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    from appendix_charts import build_glance, build_visuals
    questions = [{**q, 'visuals': build_visuals(q, colors['accent'])} for q in payload['questions']]
    env = Environment(loader=FileSystemLoader(str(ROOT)), autoescape=select_autoescape(['html']))
    html = env.get_template('appendix_template.html').render(
        regulation_name=regulation_name, colors=colors, questions=questions, glance=build_glance(questions),
        generated=payload['generated'], parquet=payload['parquet'],
        comments_received=comments_received.load(reg_dir, reg_dir / 'full_run.parquet'))
    out = reg_dir / 'appendix.html'
    out.write_text(html, encoding='utf-8')
    return out


def read_config(reg_dir: Path) -> dict:
    return yaml.safe_load(open(reg_dir / 'analyzer_config.yaml', encoding='utf-8')) or {}


def display_settings(cfg: dict, reg_dir: Path) -> Tuple[str, dict]:
    colors = dict(DEFAULT_COLORS)
    colors.update({k: v for k, v in ((cfg.get('report') or {}).get('colors') or {}).items() if k in colors and v})
    return cfg.get('regulation_name') or reg_dir.name, colors


def print_plan(q: dict, plan: dict, salvage: dict, limit: int) -> None:
    s = plan['stats']
    print(f"[{q['id']}] kind={q['kind']} scope={q['scope']} models={q['map_model']} / {q['reduce_model']}")
    src = salvage['source']
    if src == 'pending':
        print('  salvage: no plan cached yet -- showing keyword/entity filters only (run with --plan to create one)')
    elif src == 'failed':
        print('  salvage: the planning call failed -- using the question\'s own filters only (retried next run)')
    elif src == 'off':
        print('  salvage: off')
    else:
        print(f"  salvage ({src}): tags={salvage['tags']} flags={salvage['flags']} "
              f"entity_types={salvage['entity_types']} hints={salvage['hints']}"
              + (f" [{salvage['dropped']} invented name(s) discarded]" if salvage['dropped'] else ''))
        if salvage['reasoning']:
            print(f"    why: {salvage['reasoning']}")
        for d in salvage.get('unselective', []):
            print(f"    ignored as non-selective: {d['name']!r} matches {d['share']:.0%} of the texts")
        o = plan['overlap']
        if o:
            print(f"    vs keywords: {o['keyword_units']:,} keyword units, {o['salvage_units']:,} salvage units, "
                  f"{o['both_units']:,} in both; salvage adds {o['salvage_only_units']:,} units "
                  f"({o['salvage_only_comments']:,} comments) keywords miss, keywords add {o['keyword_only_units']:,} "
                  f"({o['keyword_only_comments']:,} comments) salvage misses")
    print(f"  units in scope: {s['units_in_scope']:,} ({s['comments_in_scope']:,} comments)")
    print(f"  match the filters: {s['units_matched']:,} units ({s['comments_matched']:,} comments)"
          + (f"; piloting {s['units_sent']:,}" if limit else ''))
    print(f"  chunks to send: {s['chunks']:,} ({s['cached_chunks']:,} already cached)")
    print(f"  estimated input tokens: ~{s['est_input_tokens']:,} total, ~{s['est_input_tokens_uncached']:,} not yet cached"
          " (plus a small merge/write-up step)")
    if s['units_with_chunks_left_out']:
        print(f"  {s['units_with_chunks_left_out']:,} unit(s) have chunks left out (no keyword hit, or over max_chunks)")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description='On-request appendix of one-off LLM questions (local only).')
    ap.add_argument('--regulation', required=True)
    ap.add_argument('--questions', help='question file (default: regulations/<slug>/appendix_questions.yaml)')
    ap.add_argument('--question', action='append', help='run only this question id (repeatable)')
    ap.add_argument('--parquet', help='parquet to read (default: regulations/<slug>/full_run.parquet)')
    ap.add_argument('--out-dir', help='write everything (page, CSVs, results, cache) here instead of next to the '
                                      'regulation; if full_run.parquet is also here it is used unless --parquet is given. '
                                      'The question file and analyzer_config.yaml still come from the regulation directory')
    ap.add_argument('--regulations-dir', default=str(ROOT / 'regulations'))
    ap.add_argument('--dry-run', action='store_true', help='show counts and token estimates; make no LLM calls and write nothing')
    ap.add_argument('--plan', action='store_true',
                    help='only decide what to reuse from the parquet (one small call per question, cached) and show it')
    ap.add_argument('--profile', action='store_true',
                    help='print what the parquet already holds that a question could reuse (no LLM, no question file needed)')
    ap.add_argument('--render-only', action='store_true', help='rebuild appendix.html and CSVs from appendix_results.json')
    ap.add_argument('--limit', type=int, default=0, help='pilot: send only a random N matched units per question')
    ap.add_argument('--max-tokens', type=int, default=2_000_000,
                    help='refuse to start a question whose uncached input would exceed this many estimated tokens')
    a = ap.parse_args(argv)
    if a.dry_run and a.plan:
        raise SystemExit('--dry-run makes no LLM calls and --plan makes one per question; choose one')

    reg_dir = Path(a.regulations_dir) / a.regulation
    if not reg_dir.is_dir():
        raise SystemExit(f'No such regulation directory: {reg_dir}')
    cfg = read_config(reg_dir)
    regulation_name, colors = display_settings(cfg, reg_dir)
    out_dir = Path(a.out_dir) if a.out_dir else reg_dir
    results_path = out_dir / 'appendix_results.json'

    if a.render_only:
        if not results_path.exists():
            raise SystemExit(f'{results_path} does not exist yet; run without --render-only first')
        payload = json.load(open(results_path, encoding='utf-8'))
        write_csvs(out_dir, payload['questions'])
        print(f'wrote {render_page(out_dir, payload, regulation_name, colors)}')
        return 0

    if a.parquet:
        parquet = Path(a.parquet)
    elif a.out_dir and (out_dir / 'full_run.parquet').exists():
        parquet = out_dir / 'full_run.parquet'
    else:
        parquet = reg_dir / 'full_run.parquet'
    if not parquet.exists():
        raise SystemExit(f'{parquet} not found. Put a copy there, pass --parquet, or pull it: '
                         f'python sync_state.py pull --regulation {a.regulation} --files full_run.parquet')
    df = load_corpus(parquet)
    stat = parquet.stat()
    parquet_info = {'path': str(parquet), 'rows': len(df),
                    'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M')}
    print(f'{parquet}: {len(df):,} comments (modified {parquet_info["modified"]})')
    profile = build_profile(df, cfg)
    if a.profile:
        print('\nWhat the standard pipeline already stored (a question can reuse these):\n')
        print(render_profile(profile))
        return 0

    questions = load_questions(Path(a.questions) if a.questions else reg_dir / 'appendix_questions.yaml')
    if a.question:
        unknown = set(a.question) - {q['id'] for q in questions}
        if unknown:
            raise SystemExit(f'unknown question id(s): {sorted(unknown)}')
        selected = [q for q in questions if q['id'] in set(a.question)]
    else:
        selected = questions

    if not a.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
    cache = Cache(out_dir / 'appendix_cache.jsonl')   # loading only reads; --dry-run never calls put()
    units_by_scope: Dict[str, List[Unit]] = {}
    new_results: Dict[str, dict] = {}
    env_ready = False
    plans_failed = 0
    for q in selected:
        if q['kind'] == 'state_map':
            result = run_state_map_question(df, q)
            s = result['stats']
            print(f"[{q['id']}] kind=state_map -- {s['comments_with_state']:,} of {s['comments_in_scope']:,} "
                  f"comments have a state identified, across {s['units_relevant']} states/territories")
            if not (a.dry_run or a.plan):
                new_results[q['id']] = result
            continue
        if q['scope'] not in units_by_scope:
            units_by_scope[q['scope']] = build_units(df, q['scope'])
        units = units_by_scope[q['scope']]
        call_stats: dict = {}
        if not a.dry_run and not env_ready:
            from dotenv import load_dotenv
            load_dotenv()
            env_ready = True
        try:
            salvage = resolve_salvage(q, profile, cache, call_stats, allow_call=not a.dry_run)
        except CredentialsError as e:
            raise SystemExit(f'\nThe LLM API rejected the key or is out of credit: {str(e)[:200]}')
        if call_stats.get('prompt_tokens'):
            print(f"[{q['id']}] planning call: {call_stats['prompt_tokens']:,} prompt + "
                  f"{call_stats.get('completion_tokens', 0):,} completion tokens")
        salvage = prune_unselective(salvage, units)
        plan = plan_question(q, units, regulation_name, cache, limit=a.limit, salvage=salvage)
        print_plan(q, plan, salvage, a.limit)
        plans_failed += salvage['source'] == 'failed'
        if a.dry_run or a.plan:
            continue
        if plan['stats']['est_input_tokens_uncached'] > a.max_tokens:
            raise SystemExit(f"[{q['id']}] needs ~{plan['stats']['est_input_tokens_uncached']:,} uncached input tokens, over "
                             f'--max-tokens {a.max_tokens:,}. Tighten the prefilter, use --limit, or raise --max-tokens.')
        try:
            new_results[q['id']] = run_question(q, units, regulation_name, cache, q['workers'], a.limit, salvage)
        except CredentialsError as e:
            raise SystemExit(f'\nThe LLM API rejected the key or is out of credit: {str(e)[:200]}\n'
                             'Everything already answered is cached; re-run once the key works.')
        s = new_results[q['id']]['stats']
        print(f"  done: {s['groups']} group(s) from {s['items_kept']} verified item(s) "
              f"({s['items_dropped_unverified_quote']} dropped: quote not in text); "
              f"{s.get('prompt_tokens', 0):,} prompt + {s.get('completion_tokens', 0):,} completion tokens this run")

    if a.plan:
        return 1 if plans_failed else 0   # a plan-only run that planned nothing should not look like success
    if a.dry_run:
        return 0

    previous = {}
    if results_path.exists():
        previous = {r['id']: r for r in json.load(open(results_path, encoding='utf-8')).get('questions', [])}
    previous.update(new_results)
    order = [q['id'] for q in questions]
    ordered = [previous[i] for i in order if i in previous] + [r for i, r in previous.items() if i not in order]
    payload = {'generated': datetime.now().strftime('%Y-%m-%d %H:%M'), 'regulation': a.regulation,
               'parquet': parquet_info, 'questions': ordered}
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    write_csvs(out_dir, ordered)
    print(f'wrote {render_page(out_dir, payload, regulation_name, colors)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
