"""Display-ready chart and tile data for appendix.html.

Pure functions from a saved question result (a dict from appendix_results.json) to plain structures the template
renders as HTML/CSS bars. No model calls and no I/O, so they are cheap to test and safe to re-run with --render-only.

Every chart follows the same rules: one baseline, thin bars with a rounded data end, a 2px gap between touching
segments, values printed only where they fit (the tooltip and the table view carry the rest), and a table-view twin so
no value is reachable only by hovering.
"""
from collections import Counter
from typing import Any, Dict, List, Optional

# Position colors, as given (not re-run through the colorblind/chroma validator the previous
# red/blue pair was checked with -- worth doing if that guarantee matters here).
POSITION_COLORS = {'Oppose': '#cf5d00', 'Support': '#2072b1', 'Other': '#898781'}
POSITIONS = tuple(POSITION_COLORS)
INK = '#0b0b0b'
TOP_N = 12                 # bars per chart before the tail is left to the table
TYPE_TOP_N = 8             # categories shown before folding the rest into "Other"
BAR_SCALE = 88.0           # % of the track the longest bar may fill; the rest is room for the value label
LABEL_MIN_SHARE = 0.12     # a value is printed inside a segment only if the segment is at least this share of the track


# --------------------------------------------------------------------------- small helpers

def _lin(c: int) -> float:
    c = c / 255
    return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4


def _luminance(hex_color: str) -> float:
    h = hex_color.lstrip('#')
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return 0.2126 * _lin(r) + 0.7152 * _lin(g) + 0.0722 * _lin(b)


def _contrast(a: str, b: str) -> float:
    hi, lo = sorted((_luminance(a), _luminance(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def text_on(background: str) -> str:
    """White or ink, whichever clears more contrast on a filled segment."""
    return '#ffffff' if _contrast('#ffffff', background) >= _contrast(INK, background) else INK


def bucket_position(counts: Optional[Dict[str, Any]]) -> Dict[str, int]:
    """Fold every position label the pipeline produced into Oppose / Support / Other."""
    out = {p: 0 for p in POSITIONS}
    for key, n in (counts or {}).items():
        out['Oppose' if key == 'Oppose' else 'Support' if key == 'Support' else 'Other'] += int(n)
    return out


def _fmt(n: int) -> str:
    return f'{int(n):,}'


_SINGULAR = {'comments': 'comment', 'entries': 'entry'}


def _count(n: int, plural: str) -> str:
    return f'{_fmt(n)} {_SINGULAR.get(plural, plural) if int(n) == 1 else plural}'


def _pct(part: int, whole: int) -> str:
    return f'{100 * part / whole:.0f}%' if whole else '0%'


# --------------------------------------------------------------------------- bars

def _segment(name: str, color: str, value: int, row_label: str, row_total: int, scale_total: int,
             as_share: bool) -> Dict[str, Any]:
    track_share = value / scale_total * BAR_SCALE / 100 if scale_total else 0
    return {
        'name': name, 'color': color, 'text_color': text_on(color), 'value': value,
        'text': _pct(value, row_total) if as_share else _fmt(value),
        'show_text': track_share >= LABEL_MIN_SHARE,
        'tip_value': _count(value, 'comments') + (f' ({_pct(value, row_total)})' if as_share or row_total != value else ''),
        'tip_label': f'{row_label} · {name}',
    }


def _stack_row(label: str, counts: Dict[str, int], scale_total: int, as_share: bool) -> Dict[str, Any]:
    total = sum(counts.values())
    segs = [_segment(p, POSITION_COLORS[p], counts[p], label, total, scale_total, as_share)
            for p in POSITIONS if counts.get(p)]
    return {'label': label, 'total_text': _fmt(total) + (' comments' if as_share else ''),
            'width': round(total / scale_total * BAR_SCALE, 2) if scale_total else 0, 'segments': segs}


def _single_row(label: str, value: int, scale_total: int, color: str, unit: str) -> Dict[str, Any]:
    return {'label': label, 'total_text': _fmt(value),
            'width': round(value / scale_total * BAR_SCALE, 2) if scale_total else 0,
            'segments': [{'name': unit, 'color': color, 'text_color': text_on(color), 'value': value, 'text': _fmt(value),
                          'show_text': False, 'tip_value': _count(value, unit), 'tip_label': label}]}


def skew_note(totals: List[int]) -> str:
    """Say so when one bar dwarfs the rest. The scale stays linear (a log scale misstates the sizes); the value at
    each bar's end and the table view stay exact for the small ones."""
    ranked = sorted(totals, reverse=True)
    if len(ranked) >= 4 and ranked[3] and ranked[0] >= 10 * ranked[3]:
        return ' Bars share one linear scale, so groups far smaller than the largest look thin; the number at each bar end is exact.'
    return ''


def _legend_used(rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    used = {s['name'] for r in rows for s in r['segments']}
    return [{'name': p, 'color': POSITION_COLORS[p]} for p in POSITIONS if p in used]


# --------------------------------------------------------------------------- charts

def position_comparison(q: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Do the people who raised this differ in position from everyone else? Two 100% bars, no double counting."""
    s = q.get('stats') or {}
    rel, allc = bucket_position(s.get('stance_relevant')), bucket_position(s.get('stance_in_scope'))
    if not sum(rel.values()) or not sum(allc.values()):
        return None
    rows = [_stack_row('All comments in scope', allc, sum(allc.values()), True),
            _stack_row('Relevant to this question', rel, sum(rel.values()), True)]
    table = [[r['label']] + [_fmt(c[p]) + f' ({_pct(c[p], sum(c.values()))})' for p in POSITIONS] + [_fmt(sum(c.values()))]
             for r, c in zip(rows, (allc, rel))]
    return {'id': f"{q['id']}-position", 'title': 'Position on the rule: relevant comments vs. all comments in scope',
            'caption': 'Share of comments by position. Shows whether the commenters who raised this differ from everyone else.',
            'legend': _legend_used(rows), 'rows': rows,
            'table': {'headers': ['', *POSITIONS, 'Total'], 'rows': table}}


def groups_by_position(q: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    groups = q.get('groups') or []
    if len(groups) < 2:
        return None
    ranked = sorted(groups, key=lambda g: -g['comments'])[:TOP_N]
    counts = [bucket_position(g.get('stance')) for g in ranked]
    scale = max(sum(c.values()) for c in counts) or 1
    rows = [_stack_row(g['label'], c, scale, False) for g, c in zip(ranked, counts)]
    table = [[g['label'], g.get('category') or '', *[_fmt(c[p]) for p in POSITIONS], _fmt(g['comments'])]
             for g, c in zip(ranked, counts)]
    more = len(groups) - len(ranked)
    return {'id': f"{q['id']}-groups", 'title': 'Comments per group, split by position',
            'caption': (f'The {len(ranked)} largest of {len(groups)} groups.' if more > 0 else f'All {len(groups)} groups.')
                       + ' A comment can appear in more than one group, so do not add the bars together.'
                       + skew_note([sum(c.values()) for c in counts]),
            'legend': _legend_used(rows), 'rows': rows,
            'table': {'headers': ['Group', 'Category', *POSITIONS, 'Comments'], 'rows': table}}


def entries_by_type(q: Dict[str, Any], color: str) -> Optional[Dict[str, Any]]:
    groups = q.get('groups') or []
    if len(groups) < 3:
        return None
    counts: Counter = Counter()
    shown: Dict[str, Counter] = {}
    for g in groups:
        raw = (g.get('category') or '').strip()
        key = raw.lower() or 'not stated'
        counts[key] += 1
        shown.setdefault(key, Counter())[raw or 'Not stated'] += 1
    ordered = counts.most_common()
    head, tail = ordered[:TYPE_TOP_N], ordered[TYPE_TOP_N:]
    labels = [(shown[k].most_common(1)[0][0][:1].upper() + shown[k].most_common(1)[0][0][1:], n) for k, n in head]
    if tail:
        labels.append((f'Other ({len(tail)} types)', sum(n for _, n in tail)))
    scale = max(n for _, n in labels)
    rows = [_single_row(label, n, scale, color, 'entries') for label, n in labels]
    return {'id': f"{q['id']}-types", 'title': 'Entries by type',
            'caption': 'Number of distinct entries per type, as described in the comments. Types are the reader\'s wording, so near-duplicates are possible.',
            'legend': [], 'rows': rows,
            'table': {'headers': ['Type', 'Entries'], 'rows': [[label, _fmt(n)] for label, n in labels]}}


def top_entries(q: Dict[str, Any], color: str) -> Optional[Dict[str, Any]]:
    groups = sorted(q.get('groups') or [], key=lambda g: -g['comments'])[:TOP_N]
    if not groups or groups[0]['comments'] < 2:
        return None
    scale = groups[0]['comments']
    return {'id': f"{q['id']}-top", 'title': 'Entries with the most comments',
            'caption': f'The {len(groups)} entries backed by the most comments (a comment can name more than one entry).'
                       + skew_note([g['comments'] for g in groups]),
            'legend': [], 'rows': [_single_row(g['label'], g['comments'], scale, color, 'comments') for g in groups],
            'table': {'headers': ['Entry', 'Comments'], 'rows': [[g['label'], _fmt(g['comments'])] for g in groups]}}


# --------------------------------------------------------------------------- state map (tile cartogram)

# Schematic tile grid (row, col), 1-indexed -- not real geography, just a layout that keeps each
# state roughly where a reader expects it (Pacific at the left, Northeast clustered at the right,
# AK/HI inset at the bottom-left), verified to have exactly one cell per state/DC and no collisions.
US_STATE_GRID = {
    'WA': (1, 2), 'OR': (2, 2), 'CA': (3, 2),
    'ID': (1, 3), 'NV': (2, 3), 'AZ': (3, 3),
    'MT': (1, 4), 'WY': (2, 4), 'UT': (3, 4),
    'ND': (1, 5), 'SD': (2, 5), 'CO': (3, 5), 'NM': (4, 5),
    'MN': (1, 6), 'IA': (2, 6), 'NE': (3, 6), 'KS': (4, 6),
    'WI': (1, 7), 'IL': (2, 7), 'MO': (3, 7), 'OK': (4, 7),
    'MI': (1, 8), 'IN': (2, 8), 'KY': (3, 8), 'AR': (4, 8), 'TX': (5, 7),
    'OH': (2, 9), 'TN': (3, 9), 'LA': (5, 8), 'MS': (4, 9), 'AL': (4, 10),
    'NY': (1, 10), 'PA': (2, 10), 'WV': (3, 10),
    'VT': (1, 11), 'NH': (1, 12), 'ME': (1, 13),
    'MA': (2, 12), 'CT': (2, 11), 'RI': (2, 13),
    'NJ': (3, 11), 'DE': (3, 12), 'MD': (3, 13),
    'VA': (4, 11), 'NC': (4, 12), 'SC': (5, 12),
    'GA': (5, 11), 'FL': (6, 11),
    'DC': (4, 13),
    'AK': (8, 1), 'HI': (8, 2),
    'PR': (7, 13), 'VI': (7, 12),   # Caribbean territories, inset near FL
    'GU': (8, 4), 'AS': (8, 5),     # Pacific territories, in the AK/HI inset row
}
STATE_GRID_ROWS = 8
STATE_GRID_COLS = 13
EMPTY_TILE_COLOR = '#e8e2d8'
EMPTY_TILE_TEXT = '#9a8f80'


def _abbr_count(n: int) -> str:
    """Compact form for a tile too small for a full thousands-separated number: 4431 -> '4.4k'."""
    if n < 1000:
        return str(n)
    return f'{n / 1000:.1f}'.rstrip('0').rstrip('.') + 'k'


def _blend_toward_white(hex_color: str, frac: float) -> str:
    """hex_color at frac=1, fading toward white as frac -> 0. Keeps full-opacity text legible at
    any blend (unlike CSS opacity, which fades the label along with the fill)."""
    h = hex_color.lstrip('#')
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    mix = lambda c: round(c + (255 - c) * (1 - frac))
    return f'#{mix(r):02x}{mix(g):02x}{mix(b):02x}'


def _choropleth(color: str, count: int, max_count: int) -> str:
    """Shade `color` by this state's share of the highest count any state has FOR THIS SAME
    POSITION -- Oppose and Support are each scaled against their own max, not a shared one, so a
    state that leads narrowly in a small-volume position (e.g. Support, where even the top state
    has single digits) still reads as the deepest tile in its column instead of looking uniformly
    pale next to Oppose's much larger counts."""
    if not count or not max_count:
        return '#ffffff'
    frac = 0.15 + 0.85 * (count / max_count) ** 0.5   # sqrt: a lead doesn't need 10x the count to show
    return _blend_toward_white(color, frac)


def build_state_map(q: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    states = q.get('states') or []
    if not states:
        return None
    by_abbr = {s['state']: s for s in states}
    max_oppose = max((bucket_position(s['counts'])['Oppose'] for s in states), default=0)
    max_support = max((bucket_position(s['counts'])['Support'] for s in states), default=0)

    cells = []
    for abbr, (row, col) in US_STATE_GRID.items():
        s = by_abbr.get(abbr) or {'name': abbr, 'total': 0, 'counts': {}}
        bucket = bucket_position(s['counts'])
        total = s['total']
        oppose_color = _choropleth(POSITION_COLORS['Oppose'], bucket['Oppose'], max_oppose)
        support_color = _choropleth(POSITION_COLORS['Support'], bucket['Support'], max_support)
        cells.append({
            'abbr': abbr, 'row': row, 'col': col, 'total': total, 'empty': not total,
            'oppose': bucket['Oppose'], 'oppose_text': _abbr_count(bucket['Oppose']), 'oppose_color': oppose_color,
            'oppose_text_color': text_on(oppose_color),
            'support': bucket['Support'], 'support_text': _abbr_count(bucket['Support']), 'support_color': support_color,
            'support_text_color': text_on(support_color),
            'tip': (f"{s['name']}: {_fmt(total)} comment(s) with a state identified — "
                    f"{bucket['Oppose']} oppose, {bucket['Support']} support, {bucket['Other']} other"
                    if total else f"{s['name']}: no comment identified this state"),
        })

    table = {'headers': ['State', *POSITIONS, 'Total'],
             'rows': [[s['name'], *[_fmt(bucket_position(s['counts'])[p]) for p in POSITIONS], _fmt(s['total'])]
                      for s in states if s['total']]}
    return {'rows': STATE_GRID_ROWS, 'cols': STATE_GRID_COLS, 'cells': cells,
            # Only Oppose/Support: those are the only two the tiles themselves show (see the template);
            # "Other" is real in the table/CSV, just not on the map, so it has no legend swatch here.
            'legend': [{'name': p, 'color': POSITION_COLORS[p]} for p in ('Oppose', 'Support')],
            'table': table}


def state_map_tiles(q: Dict[str, Any]) -> List[Dict[str, str]]:
    s = q.get('stats') or {}
    total, with_state = s.get('comments_in_scope') or 0, s.get('comments_with_state') or 0
    states = q.get('states') or []
    top = max(states, key=lambda x: x['total']) if states else None
    out = [{'label': 'Comments with a state identified', 'value': _fmt(with_state),
            'note': f'{100 * with_state / total:.1f}% of {_fmt(total)}' if total else ''},
           {'label': 'States represented', 'value': _fmt(s.get('units_relevant') or 0), 'note': 'of 50 + DC'}]
    if top and top['total']:
        out.append({'label': 'Most comments from', 'value': top['name'], 'note': f"{_fmt(top['total'])} comments"})
    return out


# --------------------------------------------------------------------------- tiles + entry points

def tiles(q: Dict[str, Any]) -> List[Dict[str, str]]:
    s = q.get('stats') or {}
    scope = s.get('comments_in_scope') or 0
    rel = s.get('comments_relevant') or 0
    noun = 'Entries' if q.get('kind') == 'list' else 'Groups'
    out = [{'label': 'Relevant comments', 'value': _fmt(rel),
            'note': f'{100 * rel / scope:.1f}% of {_fmt(scope)} in scope' if scope else ''},
           {'label': 'Distinct texts', 'value': _fmt(s.get('units_relevant') or 0),
            'note': f"of {_fmt(s.get('units_sent') or 0)} read"},
           {'label': noun, 'value': _fmt(s.get('groups') or 0), 'note': 'after merging near-duplicates'}]
    if s.get('items_dropped_role'):
        out.append({'label': 'Excluded by role', 'value': _fmt(s['items_dropped_role']),
                    'note': 'named, but not in a role this question keeps'})
    return out


def build_visuals(q: Dict[str, Any], accent: str) -> Dict[str, Any]:
    if q.get('kind') == 'state_map':
        return {'tiles': state_map_tiles(q), 'charts': [], 'state_map': build_state_map(q)}
    if q.get('kind') == 'list':
        charts = [entries_by_type(q, accent), top_entries(q, accent), position_comparison(q)]
    else:
        charts = [groups_by_position(q), position_comparison(q)]
    return {'tiles': tiles(q), 'charts': [c for c in charts if c]}


def build_glance(questions: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    out = []
    for q in questions:
        t = tiles(q)[0]
        out.append({'id': q['id'], 'title': q['title'], 'value': t['value'], 'note': t['note']})
    return out
