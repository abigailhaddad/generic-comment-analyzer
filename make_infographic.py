#!/usr/bin/env python3
"""One-slide (16:9) infographic for a regulation's analyzed comments.

Reads the same full_run.parquet that generate_report.py reads and draws:
  1. total comments          2. % opposed (gauge)
  3. oppose / support word clouds
  4. topic (concern) breakdown
  5. who is commenting (entity types)
plus the rule ID and the comment date range.

Nothing in here is specific to one docket: stances, entity types and topics come
from the parquet's own `analysis` column, the rule ID and title from
regulation_metadata.json, and every look-and-feel choice (five palette roles,
sizes, word/topic counts) can be overridden from the CLI or from an optional
`infographic:` block in analyzer_config.yaml.

    python make_infographic.py --regulation <slug>
    python make_infographic.py --regulation <slug> --palette plum-gold \
           --color oppose=#7A3E9D --topics 5 --words 50 --formats png,svg

What goes on the slide. Right of the count and meter are four quadrants (tl tr / bl br); any figure can go in any:
    --layout tl=cloud_oppose,bl=cloud_support,tr=topics,br=timeline     (the default)
    --clouds combined                one word cloud for both sides (sizes relative within each side, colour = side);
                                     the freed quadrant gets --extra-figure (default state_map)
    --layout top=state_map,bl=cloud_both,br=timeline      'top' / 'bottom' merge a row's two quadrants
Figures: cloud_oppose cloud_support cloud_both topics timeline entities state_map backlog cumulative none.
Any figure can also be a slide of its own, in the same frame and header (written as <name>_<figure>.png):
    --slides state_map                  one figure
    --slides summary,state_map,topics   the usual slide plus two single-figure slides
Figures for single-figure slides: meter cloud_oppose cloud_support cloud_both topics timeline entities state_map
backlog cumulative heatmap overlap.
The state map is the appendix's tile map (oppose and support per state, shaded within each position) and fits one
quadrant: tiles are side by side when there is room and stacked when not (--map-tile, --map-counts). The same choices
can live in the docket's infographic: block (clouds, extra_figure, layout, map_counts, map_tile).
Stance tags are matched by substring: oppose_tags / support_tags (config) or --oppose-tag / --support-tag change
which tags count as oppose / support when a docket words them differently from 'Position: Oppose' / 'Position: Support'.

Only numpy / pandas / matplotlib / pillow / scipy / pyyaml are needed, all of
which are already in requirements.txt (the word cloud is drawn in-house).
"""
import argparse
import gzip
import html
import io
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import yaml  # noqa: E402
from matplotlib import font_manager  # noqa: E402
from matplotlib.patches import Circle, FancyBboxPatch, Polygon, Rectangle, Wedge  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from PIL import Image, ImageDraw, ImageFont  # noqa: E402
from scipy.ndimage import binary_dilation  # noqa: E402

# --------------------------------------------------------------------------
# Palette: exactly five roles. Defaults are the Okabe-Ito blue / vermillion
# pair, which stays separable under all common colour-vision deficiencies.
# --------------------------------------------------------------------------
ROLES = ('bg', 'ink', 'oppose', 'support', 'neutral')
PALETTES = {
    'okabe-ito': dict(bg='#FAF7F0', ink='#1B1B1B', oppose='#D55E00', support='#0072B2', neutral='#D3CCBD'),
    'plum-gold': dict(bg='#F6F2EA', ink='#1B1B1B', oppose='#6E1F63', support='#A66F00', neutral='#D8D0C0'),
    'dark': dict(bg='#12161C', ink='#F1EFE8', oppose='#E69F00', support='#56B4E9', neutral='#3A4250'),
    'mono': dict(bg='#FFFFFF', ink='#111111', oppose='#111111', support='#8A8A8A', neutral='#E2E2E2'),
}
DEFAULT_PALETTE = 'okabe-ito'


# ----------------------------- colour helpers ------------------------------
def hex_rgb(h):
    h = h.lstrip('#')
    if len(h) == 3:
        h = ''.join(c * 2 for c in h)
    return tuple(int(h[i:i + 2], 16) / 255 for i in (0, 2, 4))


def rgb_hex(rgb):
    return '#' + ''.join(f'{int(round(min(max(c, 0), 1) * 255)):02X}' for c in rgb)


def mix(a, b, t):
    """Blend hex colour a toward b by t (0 = a, 1 = b)."""
    ra, rb = hex_rgb(a), hex_rgb(b)
    return rgb_hex(tuple(x + (y - x) * t for x, y in zip(ra, rb)))


def _lin(c):
    c = np.asarray(c, float)
    return np.where(c <= 0.04045, c / 12.92, ((c + 0.055) / 1.055) ** 2.4)


def _delin(c):
    c = np.clip(np.asarray(c, float), 0, 1)
    return np.where(c <= 0.0031308, c * 12.92, 1.055 * c ** (1 / 2.4) - 0.055)


def luminance(h):
    r, g, b = _lin(hex_rgb(h))
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast(a, b):
    la, lb = sorted((luminance(a), luminance(b)), reverse=True)
    return (la + 0.05) / (lb + 0.05)


# Machado, Oliveira & Fernandes (2009), severity 1.0, applied in linear RGB.
CVD = {
    'protanopia': [[0.152286, 1.052583, -0.204868], [0.114503, 0.786281, 0.099216], [-0.003882, -0.048116, 1.051998]],
    'deuteranopia': [[0.367322, 0.860646, -0.227968], [0.280085, 0.672501, 0.047413], [-0.011820, 0.042940, 0.968881]],
    'tritanopia': [[1.255528, -0.076749, -0.178779], [-0.078411, 0.930809, 0.147602], [0.004733, 0.691367, 0.303900]],
}


def _lab(rgb_lin):
    m = np.array([[0.4124, 0.3576, 0.1805], [0.2126, 0.7152, 0.0722], [0.0193, 0.1192, 0.9505]])
    xyz = m @ rgb_lin / np.array([0.95047, 1.0, 1.08883])
    f = np.where(xyz > 216 / 24389, np.cbrt(xyz), (24389 / 27 * xyz + 16) / 116)
    return np.array([116 * f[1] - 16, 500 * (f[0] - f[1]), 200 * (f[1] - f[2])])


def cvd_delta_e(a, b, kind=None):
    la, lb = _lin(hex_rgb(a)), _lin(hex_rgb(b))
    if kind:
        m = np.array(CVD[kind])
        la, lb = m @ la, m @ lb
    return float(np.linalg.norm(_lab(np.clip(la, 0, 1)) - _lab(np.clip(lb, 0, 1))))


def check_palette(p, min_delta_e=25.0):
    """Return a list of human-readable accessibility warnings (empty = fine)."""
    warns = []
    if contrast(p['ink'], p['bg']) < 7:
        warns.append(f"ink on bg contrast {contrast(p['ink'], p['bg']):.1f}:1 (<7:1)")
    for role in ('oppose', 'support'):
        c = contrast(p[role], p['bg'])
        if c < 3:
            warns.append(f"{role} on bg contrast {c:.1f}:1 (<3:1)")
    for kind in (None, *CVD):
        d = cvd_delta_e(p['oppose'], p['support'], kind)
        if d < min_delta_e:
            warns.append(f"oppose vs support only ΔE {d:.0f} apart under {kind or 'normal vision'} (<{min_delta_e:.0f})")
    return warns


# ------------------------------- data layer --------------------------------
def read_parquet_any(path):
    """Read a parquet file, transparently handling the .gz-wrapped copy kept in state/."""
    raw = Path(path).read_bytes()
    if raw[:2] == b'\x1f\x8b':
        raw = gzip.decompress(raw)
    import pyarrow.parquet as pq
    table = pq.read_table(io.BytesIO(raw))
    df = table.to_pandas()
    # Some pipelines record how many comments were submitted (as opposed to
    # published) as JSON under the file's `comments_received` metadata key.
    try:
        info = json.loads((table.schema.metadata or {})[b'comments_received'])
        df.attrs['comments_received'] = dict(count=int(info['count']), as_of=str(info.get('as_of') or ''))
    except (KeyError, ValueError, TypeError):
        pass
    return df


def _as_list(x):
    if x is None:
        return []
    if hasattr(x, 'tolist'):
        x = x.tolist()
    return x if isinstance(x, list) else []


# Substrings of the pipeline's stance tags that mean oppose / support. The defaults are the repo's convention;
# another docket whose tags read differently sets oppose_tags / support_tags in its config or passes
# --oppose-tag / --support-tag.
POSITION_TAGS = {'Oppose': ['Position: Oppose'], 'Support': ['Position: Support']}


def is_position_tag(tag):
    return tag.startswith('Position:') or any(t in tag for ts in POSITION_TAGS.values() for t in ts)


def position(analysis):
    """Oppose / Support / Unclear. Mirrors generate_report.comment_position()."""
    a = analysis if isinstance(analysis, dict) else {}
    if a.get('verified_stance') in ('Oppose', 'Support', 'Unclear'):
        return a['verified_stance']
    s = _as_list(a.get('stances'))
    if any(t in x for x in s for t in POSITION_TAGS['Oppose']):
        return 'Oppose'
    if any(t in x for x in s for t in POSITION_TAGS['Support']):
        return 'Support'
    return 'Unclear'


def load_comments(parquet):
    df = read_parquet_any(parquet)
    df['_pos'] = df['analysis'].map(position)
    df['_entity'] = df['analysis'].map(lambda a: (a or {}).get('entity_type') or 'Unknown')
    return df


def date_range_label(df, field):
    if field not in df:
        return ''
    d = pd.to_datetime(df[field], errors='coerce', utc=True).dropna()
    if d.empty:
        return ''
    lo, hi = d.min(), d.max()
    if (lo.year, lo.month) == (hi.year, hi.month):
        return f"{lo:%b} {lo.day}–{hi.day}, {hi.year}" if lo.day != hi.day else f"{lo:%b} {lo.day}, {lo.year}"
    if lo.year == hi.year:
        return f"{lo:%b} {lo.day} – {hi:%b} {hi.day}, {hi.year}"
    return f"{lo:%b} {lo.day}, {lo.year} – {hi:%b} {hi.day}, {hi.year}"


def timeline_counts(df, field, weekly=True):
    """Comments per day (weekly if the span is very long) by position, or None if the date field is unusable."""
    if field not in df:
        return None
    d = pd.to_datetime(df[field], errors='coerce', utc=True).dt.tz_localize(None).dt.normalize()
    if d.notna().sum() == 0:
        return None
    t = pd.crosstab(d, df['_pos']).reindex(columns=['Oppose', 'Support', 'Unclear'], fill_value=0)
    t = t.reindex(pd.date_range(t.index.min(), t.index.max(), freq='D'), fill_value=0)
    if weekly and len(t) > 62:
        t = t.resample('W').sum()
    return t


def topic_counts(df, prefix, top):
    """Count comments per topic tag and split each by position.

    Topics are the analysis `stances` entries starting with `prefix` (the
    pipeline's "Concern:" tags). If a docket has none, every non-"Position:"
    stance is treated as a topic."""
    rows = {}
    any_prefixed = False
    for a, pos in zip(df['analysis'], df['_pos']):
        for s in _as_list((a or {}).get('stances')):
            if s.startswith(prefix):
                any_prefixed = True
    for a, pos in zip(df['analysis'], df['_pos']):
        for s in _as_list((a or {}).get('stances')):
            if is_position_tag(s):
                continue
            if any_prefixed and not s.startswith(prefix):
                continue
            name = s[len(prefix):].strip() if s.startswith(prefix) else s.strip()
            r = rows.setdefault(name, Counter())
            r[pos] += 1
            r['n'] += 1
    ranked = sorted(rows.items(), key=lambda kv: -kv[1]['n'])[:top]
    return [dict(name=k, n=v['n'], oppose=v['Oppose'], support=v['Support'], other=v['Unclear']) for k, v in ranked]


def entity_summary(df, top):
    counts = df['_entity'].value_counts()
    total = int(counts.sum())
    primary, primary_n = counts.index[0], int(counts.iloc[0])
    rest = counts.iloc[1:]
    shown = [(k, int(v)) for k, v in rest.iloc[:top].items()]
    other_n = int(rest.iloc[top:].sum())
    return dict(total=total, primary=primary, primary_n=primary_n, org_total=int(rest.sum()),
                shown=shown, other_n=other_n, other_types=max(len(rest) - top, 0))


# ------------------------------- word clouds -------------------------------
STOP = set("""
a about above after again against all also am an and any are aren't as at be because been before being below
between both but by can can't cannot could couldn't did didn't do does doesn't doing don't down during each
few for from further had hadn't has hasn't have haven't having he her here hers herself him himself his how
i if in into is isn't it its itself just let's me more most much must my myself no nor not of off on once
only or other ought our ours ourselves out over own same shall she should shouldn't so some such than that
the their theirs them themselves then there these they this those through to too under until up upon very
was wasn't we were weren't what when where which while who whom why will with won't would wouldn't you your
yours yourself yourselves via per etc however therefore thus also may might get got would make made many
one two like even still yet since among within without across whether whose ever every either neither
comment comments commenter commenters docket regulation regulations rule rules proposed proposal propose
submit submitted submission writing write wrote sincerely dear regards thank thanks please federal register
notice rin cfr http https www com org gov html pdf attached attachment see writing nprm quot rdquo ldquo rsquo lsquo amp nbsp
oppos* support*
""".split())
class StopSet(set):
    """Stop words. An entry ending in '*' is a prefix: 'oppos*' drops oppose, opposed, opposing, opposition ..."""

    def __init__(self, items=()):
        super().__init__(items)
        self.prefixes = tuple(w[:-1] for w in self if w.endswith('*') and len(w) > 1)

    def __contains__(self, word):
        return super().__contains__(word) or (bool(self.prefixes) and word.startswith(self.prefixes))


_TOKEN = re.compile(r"[a-z][a-z'’-]{2,}")
_STRIP = re.compile(r"<[^>]+>|https?://\S+|www\.\S+|\S+@\S+")


def raw_tokens(text):
    txt = html.unescape(_STRIP.sub(' ', html.unescape((text or '')[:20000])).lower())
    out = []
    for t in _TOKEN.findall(txt):
        t = t.replace('’', "'")
        t = t[:-2] if t.endswith("'s") else t
        t = t.strip("'-")
        if len(t) >= 3:
            out.append(t)
    return out


def strip_boilerplate(toks, frac, n=4, cap=1500):
    """Drop token runs that appear (as a 4-gram) in more than `frac` of all voices.

    Form templates that people personalise with a name still share long runs of
    identical words; removing them leaves each writer's own words to be counted."""
    if frac <= 0:
        return toks
    thr = max(8, int(frac * len(toks)))
    cnt = Counter()
    for t in toks:
        cnt.update({hash(tuple(t[i:i + n])) for i in range(min(len(t), cap) - n + 1)})
    hot = {g for g, c in cnt.items() if c >= thr}
    out = []
    for t in toks:
        keep = [True] * len(t)
        for i in range(min(len(t), cap) - n + 1):
            if hash(tuple(t[i:i + n])) in hot:
                keep[i:i + n] = [False] * n
        out.append([w for w, k in zip(t, keep) if k])
    return out


def singularizer(vocab):
    """Fold plurals onto a singular only when that singular really occurs in the corpus
    (so 'communities' -> 'community' but 'james' stays 'james')."""
    def canon(t):
        for suf, rep in (('ies', 'y'), ('s', ''), ('es', '')):
            if t.endswith(suf) and t[:-len(suf)] + rep in vocab and len(t) > 4:
                return t[:-len(suf)] + rep
        return t
    return canon


def voices(df):
    """One row per distinct voice: every non-campaign comment plus one per form-letter
    campaign, so one widely copied template counts once instead of drowning out everyone else."""
    if 'campaign_id' not in df:
        return df
    in_camp = df['campaign_id'].notna()
    return pd.concat([df[~in_camp], df[in_camp].drop_duplicates('campaign_id')])


def doc_terms(tokens, canon, stop):
    return {c for t, c in ((t, canon(t)) for t in tokens) if c not in stop and t not in stop}


def cloud_weights(v, group, other, text_col, stop, n_words, contrast_exp, min_df, boilerplate=0.03):
    """term -> weight for one stance group.

    weight = p(term|group) * (p(term|group) / p(term|other)) ** contrast_exp
    p is the share of that group's voices using the term. contrast_exp=0 gives
    plain frequency; higher values favour words that set the group apart from
    the opposite side (so words common to both sides fade out)."""
    toks = pd.Series(strip_boilerplate(list(v[text_col].map(raw_tokens)), boilerplate), index=v.index)
    canon = singularizer(set().union(*toks))

    def dfc(mask):
        c = Counter()
        for t in toks[mask]:
            c.update(doc_terms(t, canon, stop))
        return c, int(mask.sum())
    cg, ng = dfc(v['_pos'] == group)
    co, no = dfc(v['_pos'] == other)
    out = {}
    for t, k in cg.items():
        if k < max(min_df, 1):
            continue
        pg = (k + .5) / (ng + 1)
        po = (co.get(t, 0) + .5) / (no + 1)
        if contrast_exp > 0 and pg <= po:
            continue
        out[t] = pg * (pg / po) ** contrast_exp
    return dict(sorted(out.items(), key=lambda kv: -kv[1])[:n_words])


def load_stopwords_file(path):
    """Plain text: words separated by newlines, spaces or commas; '#' starts a comment."""
    words = set()
    for line in Path(path).read_text(encoding='utf-8').splitlines():
        line = line.split('#', 1)[0]
        words.update(w.lower() for w in re.split(r'[\s,]+', line) if w)
    return words


FONT_EXTS = ('.ttf', '.otf', '.ttc')


def resolve_font(spec, role):
    """(family, file path) for a font given by installed name or by a .ttf/.otf path.

    A missing font is a hard error: matplotlib would otherwise silently substitute
    its default, and a CI runner without the font would publish a different-looking slide."""
    p = Path(spec).expanduser()
    if p.suffix.lower() in FONT_EXTS or p.is_file():
        if not p.is_file():
            sys.exit(f'{role} font file not found: {spec}')
        font_manager.fontManager.addfont(str(p))
        return font_manager.FontProperties(fname=str(p)).get_name(), str(p)
    try:
        path = font_manager.findfont(font_manager.FontProperties(family=spec, weight='bold'),
                                     fallback_to_default=False)
    except ValueError:
        sys.exit(f"{role} font {spec!r} is not installed on this machine. Install it, or pass the path "
                 f"to a .ttf/.otf file instead (e.g. --{role}-font path/to/font.ttf).")
    return spec, path


def render_cloud(weights, w, h, color, font_path, min_px, max_px, seed=7, margin=4, colors=None):
    """Greedy spiral packing (largest word first) into a w x h RGBA image.

    `colors` (word -> hex) colours words individually; otherwise every word is `color`.
    """
    rng = np.random.default_rng(seed)
    occ = np.zeros((h, w), bool)
    fill = tuple(int(c * 255) for c in hex_rgb(color)) + (255,)
    # transparent but same RGB as the ink, so resampling never blends in black fringes
    img = Image.new('RGBA', (w, h), fill[:3] + (0,))
    draw = ImageDraw.Draw(img)
    layers = {color: (img, draw)}          # one layer per ink colour, each transparent in its own colour

    def layer_for(col):
        if col not in layers:
            rgb = tuple(int(c * 255) for c in hex_rgb(col))
            im = Image.new('RGBA', (w, h), rgb + (0,))
            layers[col] = (im, ImageDraw.Draw(im))
        return layers[col]

    if not weights:
        return img
    wmax = max(weights.values())
    for word, wt in weights.items():
        size = int(min_px + (max_px - min_px) * math.sqrt(wt / wmax))
        while size >= min_px * 0.7:
            font = ImageFont.truetype(font_path, size)
            l, t, r, b = font.getbbox(word)
            bw, bh = r - l, b - t
            if bw + 2 * margin >= w or bh + 2 * margin >= h:
                size = int(size * 0.9)
                continue
            m = Image.new('L', (bw + 2 * margin, bh + 2 * margin), 0)
            ImageDraw.Draw(m).text((margin - l, margin - t), word, font=font, fill=255)
            mask = binary_dilation(np.array(m) > 0, iterations=margin)
            mh, mw = mask.shape
            placed = None
            phase = rng.uniform(0, 2 * math.pi)
            for step in range(6000):
                th = phase + step * 0.12
                rad = 1.6 * step ** 0.9 * 0.55
                cx = w / 2 + rad * math.cos(th) * (w / h) ** 0.75
                cy = h / 2 + rad * math.sin(th)
                x, y = int(cx - mw / 2), int(cy - mh / 2)
                if x < 0 or y < 0 or x + mw > w or y + mh > h:
                    if rad > max(w, h):
                        break
                    continue
                if not (occ[y:y + mh, x:x + mw] & mask).any():
                    placed = (x, y)
                    break
            if placed:
                x, y = placed
                occ[y:y + mh, x:x + mw] |= mask
                col = (colors or {}).get(word, color)
                ink = tuple(int(c * 255) for c in hex_rgb(col)) + (255,)
                layer_for(col)[1].text((x + margin - l, y + margin - t), word, font=font, fill=ink)
                break
            size = int(size * 0.9)
    if len(layers) == 1:
        return img
    out = img
    for col, (im, _) in layers.items():
        if im is not img:
            out = Image.alpha_composite(out, im)
    return out


# --------------------------------- drawing ---------------------------------
# Tile-cartogram layout of the states, shared with appendix_charts.py (the appendix's state map) when that module
# is importable, so the two maps always agree; the copy below is the fallback. (row, col), 1-indexed.
_GRID_FALLBACK = {
    'WA': (1, 2), 'OR': (2, 2), 'CA': (3, 2), 'ID': (1, 3), 'NV': (2, 3), 'AZ': (3, 3), 'MT': (1, 4), 'WY': (2, 4),
    'UT': (3, 4), 'ND': (1, 5), 'SD': (2, 5), 'CO': (3, 5), 'NM': (4, 5), 'MN': (1, 6), 'IA': (2, 6), 'NE': (3, 6),
    'KS': (4, 6), 'WI': (1, 7), 'IL': (2, 7), 'MO': (3, 7), 'OK': (4, 7), 'MI': (1, 8), 'IN': (2, 8), 'KY': (3, 8),
    'AR': (4, 8), 'TX': (5, 7), 'OH': (2, 9), 'TN': (3, 9), 'LA': (5, 8), 'MS': (4, 9), 'AL': (4, 10), 'NY': (1, 10),
    'PA': (2, 10), 'WV': (3, 10), 'VT': (1, 11), 'NH': (1, 12), 'ME': (1, 13), 'MA': (2, 12), 'CT': (2, 11),
    'RI': (2, 13), 'NJ': (3, 11), 'DE': (3, 12), 'MD': (3, 13), 'VA': (4, 11), 'NC': (4, 12), 'SC': (5, 12),
    'GA': (5, 11), 'FL': (6, 11), 'DC': (4, 13), 'AK': (8, 1), 'HI': (8, 2), 'PR': (7, 13), 'VI': (7, 12),
    'GU': (8, 4), 'AS': (8, 5)}
try:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from appendix_charts import US_STATE_GRID, STATE_GRID_ROWS, STATE_GRID_COLS  # noqa: E402
except Exception:  # not run from the repo, or the module moved: use the copy above
    US_STATE_GRID, STATE_GRID_ROWS, STATE_GRID_COLS = _GRID_FALLBACK, 8, 13
TERRITORIES = ('PR', 'VI', 'GU', 'AS')   # drawn only when they actually have comments


def pct_str(n, d):
    if not d:
        return '0%'
    p = 100 * n / d
    return '<1%' if 0 < p < 0.5 else ('>99%' if 99.5 <= p < 100 else f'{round(p)}%')


def shorten(s, n):
    return s if len(s) <= n else s[:n - 1].rstrip() + '…'


def draw_gauge(ax, frac, pal, color, n_seg=20):
    ax.set_aspect('equal')
    ax.set_xlim(-1.3, 1.3)
    ax.set_ylim(-0.3, 1.25)
    ax.axis('off')
    r_out, r_in = 1.0, 0.6
    ang = lambda p: 180 - 180 * p  # 0 -> left, 1 -> right
    # filled portion + remainder, then bg-coloured gaps cut radial slots
    ax.add_patch(Wedge((0, 0), r_out, ang(frac), 180, width=r_out - r_in, fc=color, ec='none'))
    ax.add_patch(Wedge((0, 0), r_out, 0, ang(frac), width=r_out - r_in, fc=pal['neutral'], ec='none'))
    for i in range(1, n_seg):
        a = math.radians(ang(i / n_seg))
        ax.plot([r_in * math.cos(a), r_out * math.cos(a)], [r_in * math.sin(a), r_out * math.sin(a)],
                color=pal['bg'], lw=3.2, solid_capstyle='butt', zorder=3)
    for r in (r_out, r_in):
        ax.add_patch(Wedge((0, 0), r, 0, 180, width=0.012, fc=pal['ink'], ec='none', zorder=4))
    for x0, x1 in ((-r_out, -r_in), (r_in, r_out)):
        ax.plot([x0, x1], [0, 0], color=pal['ink'], lw=2.2, solid_capstyle='butt', zorder=4)
    for i in range(0, n_seg + 1):
        a = math.radians(ang(i / n_seg))
        major = i % (n_seg // 4) == 0
        r0, r1 = r_out + 0.05, r_out + (0.15 if major else 0.10)
        ax.plot([r0 * math.cos(a), r1 * math.cos(a)], [r0 * math.sin(a), r1 * math.sin(a)],
                color=pal['ink'], lw=2 if major else 1.1, solid_capstyle='butt')
        if major:
            rl = r_out + 0.30
            ax.text(rl * math.cos(a), rl * math.sin(a) - 0.02, f'{int(round(i / n_seg * 100))}%',
                    ha='center', va='center', fontsize=9, fontweight='bold', color=pal['ink'])
    # needle + hub
    a = math.radians(ang(frac))
    ux, uy = math.cos(a), math.sin(a)
    px, py = -uy, ux
    tip = (0.93 * ux, 0.93 * uy)
    ax.add_patch(Polygon([(0.06 * px, 0.06 * py), tip, (-0.06 * px, -0.06 * py)], closed=True,
                         fc=pal['ink'], ec='none', zorder=6))
    ax.add_patch(Circle((0, 0), 0.13, fc=pal['ink'], ec='none', zorder=7))
    ax.add_patch(Circle((0, 0), 0.055, fc=pal['bg'], ec='none', zorder=8))


class Canvas:
    """One slide: figure, shared frame + header (title, rule ID, date range, optional logo and
    website) and the text helpers both slides use."""

    def __init__(self, df, meta, cfg, pal, args):
        self.W, self.H = args.size
        self.dpi = 144
        self.pal, self.args = pal, args
        self.fig = plt.figure(figsize=(self.W / self.dpi, self.H / self.dpi), dpi=self.dpi, facecolor=pal['bg'])
        self.muted = mix(pal['ink'], pal['bg'], 0.38)
        self.disp = args.fonts['display'][0]
        plt.rcParams.update({'font.family': args.fonts['body'][0], 'text.parse_math': False,
                             'svg.fonttype': 'path'})
        self.renderer = self.fig.canvas.get_renderer()
        self.rule_id = meta.get('docket_id') or args.rule_id
        self.dates = date_range_label(df, args.date_field)
        self.received = df.attrs.get('comments_received')        # {count, as_of} or None
        self.title = cfg.get('title', args.title)
        self._chrome()

    def T(self, x, y, s, **k):
        return self.fig.text(x, y, s, **{'color': self.pal['ink'], **k})

    def fit(self, x, y, s, max_w, **k):
        """Left-aligned text, ellipsised until it fits max_w (figure-width fraction)."""
        t = self.T(x, y, s, ha='left', **k)
        while len(s) > 4 and t.get_window_extent(self.renderer).width / self.W > max_w:
            s = s[:-2].rstrip() + '\u2026'
            t.set_text(s)
        return t

    def wrap(self, s, fontsize, max_w):
        """Greedy word-wrap to max_w (figure-width fraction). Never truncates."""
        probe = self.fig.text(0, 0, '', fontsize=fontsize)
        lines, cur = [], ''
        for word in s.split():
            trial = f'{cur} {word}'.strip()
            probe.set_text(trial)
            if cur and probe.get_window_extent(self.renderer).width / self.W > max_w:
                lines.append(cur)
                cur = word
            else:
                cur = trial
        probe.remove()
        return lines + ([cur] if cur else [])

    def _x_extent(self, artist):
        bb = artist.get_window_extent(self.renderer)
        return bb.x0 / self.W, bb.x1 / self.W

    def _chrome(self):
        pal, args, fig = self.pal, self.args, self.fig
        fig.add_artist(Rectangle((0.008, 0.014), 0.984, 0.972, transform=fig.transFigure, fill=False,
                                 ec=pal['ink'], lw=2.4))
        title = self.T(0.03, 0.925, self.title.upper(), fontsize=19, fontweight='bold', va='center', ha='left',
                       family=self.disp)
        docket = self.T(0.97, 0.925, f'{self.rule_id}   \u00b7   {self.dates}' if self.dates else self.rule_id,
                        fontsize=15, ha='right', va='center', family=args.fonts['mono'][0], fontweight='bold')
        fig.add_artist(Line2D([0.03, 0.97], [0.875, 0.875], transform=fig.transFigure, color=pal['ink'], lw=1.6))
        if not (args.logo or args.website):
            return
        x = self._x_extent(title)[1] + 0.03          # logo + website sit just after the title
        h = args.logo_height
        if args.logo:
            img = Image.open(args.logo).convert('RGBA')
            w = h * self.H * img.width / img.height / self.W
            px = (max(int(w * self.W * 2), 1), max(int(h * self.H * 2), 1))
            ax = fig.add_axes([x, 0.925 - h / 2, w, h])
            ax.imshow(img.resize(px, Image.LANCZOS), interpolation='none', aspect='auto')
            ax.axis('off')
            x += w + 0.012
        if args.website:
            site = self.T(x, 0.925, args.website, fontsize=13, fontweight='bold', va='center', ha='left',
                          color=self.muted)
            x = self._x_extent(site)[1]
        if x > self._x_extent(docket)[0] - 0.015:
            print('warning: logo/website run into the rule ID in the header; reduce --logo-height or '
                  'shorten --website', file=sys.stderr)

    def save(self, out_dir, name, formats):
        out_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for fmt in formats:
            p = out_dir / f'{name}.{fmt}'
            self.fig.savefig(p, dpi=self.dpi, facecolor=self.pal['bg'])
            paths.append(p)
        plt.close(self.fig)
        return paths


# ----------------------- slide 1: quadrants and the figures that go in them -----------------------
# The count and meter sit at the left. To their right is a 2 x 2 grid of figures:
#
#                middle column    right column
#       top          tl               tr          'top'    = tl + tr merged
#       bottom       bl               br          'bottom' = bl + br merged
#
# Any figure can go in any quadrant (PANEL_NAMES). DEFAULT_LAYOUT is the original slide.
SLOTS = {'tl': dict(x=0.375, w=0.30, row='top'), 'bl': dict(x=0.375, w=0.30, row='bottom'),
         'tr': dict(x=0.715, w=0.255, row='top'), 'br': dict(x=0.715, w=0.255, row='bottom'),
         # a row's two quadrants merged: room for figures that need width (the state map's counts)
         'top': dict(x=0.375, w=0.595, row='top', wide=True), 'bottom': dict(x=0.375, w=0.595, row='bottom', wide=True)}
WIDE = {'top': ('tl', 'tr'), 'bottom': ('bl', 'br')}
ROWS_GEOM = {
    'top': dict(head_y=0.848, cloud=(0.485, 0.335), chart_top=0.825, chart_bottom=0.425, budget=0.40, ax_y=0.543,
                map_bottom=0.455),
    'bottom': dict(head_y=0.40, cloud=(0.075, 0.335), chart_top=0.377, chart_bottom=0.06, budget=0.317, ax_y=0.095),
}
DEFAULT_LAYOUT = {'tl': 'cloud_oppose', 'bl': 'cloud_support', 'tr': 'topics', 'br': 'timeline'}
PANEL_NAMES = ('cloud_oppose', 'cloud_support', 'cloud_both', 'topics', 'timeline', 'entities', 'state_map',
               'backlog', 'cumulative', 'none')


class Ctx:
    """What a figure needs to draw itself, plus a place to leave numbers for the JSON."""

    def __init__(self, **kw):
        self.__dict__.update(kw)
        self.data = {}
        self.cache = {}


def slot_geometry(slot):
    s = SLOTS[slot]
    return {**s, **ROWS_GEOM[s['row']]}


def _head(ctx, hs=1.0):
    return dict(fontsize=12 * hs, fontweight='bold', va='center', ha='left', color=ctx.cv.muted, family=ctx.cv.disp)


# ---- word clouds
def _cloud_weights(ctx, grp, n_words):
    key = ('cloud', grp, n_words)
    if key not in ctx.cache:
        df, args, cfg = ctx.df, ctx.args, ctx.cfg
        text_col = 'text' if 'text' in df else 'comment_text'
        stop = set(STOP) | {w.lower() for w in cfg.get('stopwords', [])} | {w.lower() for w in args.stopwords}
        stop |= args.stopwords_from_file
        stop |= {t for t in re.split(r'[^a-z]+', ctx.rule_id.lower()) if len(t) >= 3}
        stop = StopSet(stop)
        min_df = max(3, math.ceil(args.min_df_frac * len(ctx.v)))
        other = {'Oppose': 'Support', 'Support': 'Oppose'}[grp]
        wts = cloud_weights(ctx.v, grp, other, text_col, stop, n_words, args.contrast, min_df, args.boilerplate)
        if len(wts) < 5 and args.boilerplate:  # everything was template text: fall back to raw words
            wts = cloud_weights(ctx.v, grp, other, text_col, stop, n_words, args.contrast, min_df, 0)
        ctx.cache[key] = wts
    return ctx.cache[key]


def _draw_cloud(ctx, x, y, w, h, weights, color, colors=None, scale=1.0):
    cv, args = ctx.cv, ctx.args
    W, H = cv.W, cv.H
    ss = 2
    pw, ph = int(w * W) * ss, int(h * H) * ss
    img = render_cloud(weights, pw, ph, color, args.fonts['cloud'][1], min_px=int(14 * ss * W / 1920 * 1.6 * scale),
                       max_px=int(64 * ss * W / 1920 * 1.6 * scale), seed=args.seed, colors=colors,
                       margin=max(4, round(4 * scale)))
    ax = cv.fig.add_axes([x, y, w, h])
    img = img.resize((pw // ss, ph // ss), Image.LANCZOS)  # constant RGB, so no halo
    ax.imshow(img, interpolation='none', aspect='auto')
    ax.axis('off')


def _chip(ctx, x, rect_y, text_y, col, label, hs=1.0):
    cv = ctx.cv
    cv.fig.add_artist(Rectangle((x, rect_y), 0.014 * hs, 0.024 * hs, transform=cv.fig.transFigure, fc=col, ec='none'))
    return cv.T(x + 0.022 * hs, text_y, label, fontsize=12 * hs, fontweight='bold', va='center', ha='left', family=cv.disp)


def panel_cloud(ctx, slot, grp):
    x, w = slot['x'], slot['w']
    y, h = slot['cloud']
    col = ctx.pal[grp.lower()]
    sc, hs = slot.get('scale', 1.0), slot.get('hscale', 1.0)
    wts = _cloud_weights(ctx, grp, round(ctx.args.words * slot.get('words_x', 1)))
    ctx.data.setdefault('cloud_words', {})[grp] = list(wts)
    _draw_cloud(ctx, x, y, w, h, wts, col, scale=sc)
    _chip(ctx, x, y + h + 0.012, y + h + 0.024, col, grp.upper(), hs)
    return f'cloud_{grp.lower()}'


def panel_cloud_both(ctx, slot):
    """One cloud for both sides, coloured by side. Each side keeps its own ordering of words, but by default the two
    sides are given the SAME size profile rank by rank (--combined-balance equal): the 1st oppose word and the 1st
    support word are equally big, and so on down, so a lopsided corpus cannot let one side own all the heavy words.
    With 'relative', sizes are each word's weight relative to the biggest word on its own side instead."""
    pal, args = ctx.pal, ctx.args
    x, w = slot['x'], slot['w']
    y, h = slot['cloud']
    sc, hs = slot.get('scale', 1.0), slot.get('hscale', 1.0)
    n_each = max(8, round(args.words * args.combined_share * slot.get('words_x', 1)))
    sides = {}
    for grp in ('Oppose', 'Support'):
        wts = _cloud_weights(ctx, grp, n_each)
        ctx.data.setdefault('cloud_words', {})[grp] = list(wts)
        top = max(wts.values()) if wts else 1
        sides[grp] = [(word, wt / top) for word, wt in wts.items()]       # already sorted, biggest first
    mode = args.combined_balance or ctx.cfg.get('combined_balance', 'equal')
    if mode == 'equal':
        n_common = min(len(v) for v in sides.values())
        profile = [sum(sides[g][i][1] for g in sides) / len(sides) for i in range(n_common)]
        for g, items in sides.items():
            tail = (profile[-1] / items[n_common - 1][1]) if n_common and items[n_common - 1][1] else 1.0
            sides[g] = [(word, profile[i] if i < n_common else wt * tail) for i, (word, wt) in enumerate(items)]
    ranked = []                                    # interleave sides so ties keep oppose / support alternating
    for i in range(max(len(v) for v in sides.values())):
        for g in ('Oppose', 'Support'):
            if i < len(sides[g]):
                ranked.append((sides[g][i][0], sides[g][i][1], g))
    merged, colors = {}, {}
    for word, wt, g in sorted(ranked, key=lambda t: -t[1]):
        if wt > merged.get(word, 0):
            merged[word], colors[word] = wt, pal[g.lower()]
    _draw_cloud(ctx, x, y, w, h, merged, pal['oppose'], colors=colors, scale=sc)
    first = _chip(ctx, x, y + h + 0.012, y + h + 0.024, pal['oppose'], 'OPPOSE', hs)
    x2 = ctx.cv._x_extent(first)[1] + 0.025 * hs
    _chip(ctx, x2, y + h + 0.012, y + h + 0.024, pal['support'], 'SUPPORT', hs)
    return 'cloud_both'


# ---- topics
def panel_topics(ctx, slot):
    cv, pal, df, args = ctx.cv, ctx.pal, ctx.df, ctx.args
    T, wrap = cv.T, cv.wrap
    rx, rw = slot['x'], slot['w']
    sc, hs = slot.get('scale', 1.0), slot.get('hscale', 1.0)
    T(rx, slot['head_y'], 'TOPICS', **_head(ctx, hs))
    topics = topic_counts(df, ctx.cfg.get('topic_prefix', args.topic_prefix), args.topics)
    ctx.data['topics'] = topics
    tmax = max((t['n'] for t in topics), default=1)
    bh, gap = 0.017 * sc, 0.012 * sc
    shown = list(topics)
    fs = 10.5 * sc

    def measure(font_size):
        wr = [wrap(t['name'], font_size, rw) for t in shown]
        return wr, sum((0.0185 + (len(w_) - 1) * 0.0205) * sc + 0.006 * sc + bh + gap for w_ in wr)

    while True:  # shrink the label font a little if wrapping would crowd the quadrant
        wrapped, need = measure(fs)
        if need <= slot['budget'] or fs <= 8.5 * sc:
            break
        fs -= 0.5 * sc
    while need > slot['budget'] and len(shown) > 3:  # still too tall at the smallest font: show fewer topics
        shown = shown[:-1]
        wrapped, need = measure(fs)
    cursor = slot['chart_top']
    for t, lines in zip(shown, wrapped):
        label_h = (0.0185 + (len(lines) - 1) * 0.0205) * fs / 10.5
        T(rx, cursor, chr(10).join(lines), fontsize=fs, va='top', ha='left', linespacing=1.15)
        bar_y = cursor - label_h - 0.006 * sc - bh
        T(rx + rw, bar_y + bh / 2, pct_str(t['n'], ctx.total), fontsize=10.5 * sc, fontweight='bold', va='center', ha='right')
        x0 = rx
        for key, col in (('oppose', pal['oppose']), ('support', pal['support']), ('other', pal['neutral'])):
            wseg = rw * 0.85 * t[key] / tmax
            cv.fig.add_artist(Rectangle((x0, bar_y), wseg, bh, transform=cv.fig.transFigure, fc=col, ec='none'))
            x0 += wseg
        cursor = bar_y - gap
    return 'topics'


# ---- timeline / entities
def panel_timeline(ctx, slot):
    cv, pal, df, args, cfg = ctx.cv, ctx.pal, ctx.df, ctx.args, ctx.cfg
    rx, rw = slot['x'], slot['w']
    tl_field = args.timeline_date_field or cfg.get('timeline_date_field', 'received_date')
    tl = timeline_counts(df, tl_field if tl_field in df else args.date_field)
    if tl is None:
        print('no usable date column for the timeline; showing the entity summary instead', file=sys.stderr)
        return panel_entities(ctx, slot)
    ctx.data['timeline'] = [dict(date=f'{i:%Y-%m-%d}', **{k.lower(): int(r[k]) for k in tl.columns})
                            for i, r in tl.iterrows()]
    sc, hs = slot.get('scale', 1.0), slot.get('hscale', 1.0)
    per = 'WEEK' if (len(tl) > 1 and (tl.index[1] - tl.index[0]).days == 7) else 'DAY'
    cv.T(rx, slot['head_y'], f'COMMENTS PER {per}', **_head(ctx, hs))
    lm = 0.03 if sc == 1 else 0.045                 # room for the y label
    ax = cv.fig.add_axes([rx + lm, slot['ax_y'], rw - lm, slot.get('chart_h', 0.255)])
    ax.patch.set_alpha(0)
    xs = np.arange(len(tl))
    bottom = np.zeros(len(tl))
    for col, colr in (('Oppose', pal['oppose']), ('Support', pal['support']), ('Unclear', pal['neutral'])):
        ax.bar(xs, tl[col].values, bottom=bottom, color=colr, width=0.8, linewidth=0)
        bottom = bottom + tl[col].values
    peak = int(bottom.max()) if len(bottom) else 0
    ax.axhline(peak, color=pal['neutral'], lw=1, zorder=0)
    ax.set_xlim(-0.6, len(tl) - 0.4)
    ax.set_ylim(0, peak * 1.04 if peak else 1)
    for side in ('top', 'right', 'left'):
        ax.spines[side].set_visible(False)
    ax.spines['bottom'].set_color(pal['ink'])
    ax.spines['bottom'].set_linewidth(1.6)
    ax.set_yticks([peak])
    ax.set_yticklabels([f'{peak:,}'])
    idx = sorted(set(np.linspace(0, len(tl) - 1, min(len(tl), 4 if sc == 1 else 8)).round().astype(int)))
    ax.set_xticks(idx)
    ax.set_xticklabels([f'{tl.index[i]:%b} {tl.index[i].day}' for i in idx])
    ax.tick_params(axis='y', length=0, labelsize=9 * sc, colors=pal['ink'], pad=3)
    ax.tick_params(axis='x', length=4, width=1.2, labelsize=9 * sc, colors=pal['ink'], color=pal['ink'])
    return 'timeline'


def panel_entities(ctx, slot):
    cv, pal, args, cfg = ctx.cv, ctx.pal, ctx.args, ctx.cfg
    T, fit = cv.T, cv.fit
    rx, rw, hy = slot['x'], slot['w'], slot['head_y']
    sc, hs = slot.get('scale', 1.0), slot.get('hscale', 1.0)
    ent = entity_summary(ctx.df, args.entities)
    T(rx, hy, 'WHO IS COMMENTING', **_head(ctx, hs))
    big = T(rx, hy - 0.075 * sc, pct_str(ent['primary_n'], ent['total']).replace('<', '').replace('>', ''),
            fontsize=40 * sc, fontweight='bold', va='center', ha='left', family=cv.disp)
    label_x = rx + big.get_window_extent(cv.renderer).width / cv.W + 0.014  # follows the number's real width
    T(label_x, hy - 0.075 * sc, shorten(cfg.get('entity_primary_label', ent['primary']), 26), fontsize=13 * sc,
      va='center', ha='left', color=cv.muted)
    y0 = hy - 0.155 * sc
    rows = list(ent['shown']) + ([(f"+{ent['other_types']} more types", ent['other_n'])] if ent['other_n'] else [])
    rmax = max((n for _, n in rows), default=1)
    step = min(0.05 * sc, 0.19 * sc / max(len(rows), 1))
    for i, (name, n) in enumerate(rows):
        y = y0 - i * step
        cv.fig.add_artist(Rectangle((rx, y - 0.016 * sc), rw * 0.62 * n / rmax + 0.002, 0.032 * sc, transform=cv.fig.transFigure,
                                    fc=pal['neutral'], ec='none'))
        fit(rx + 0.004, y, name, rw - 0.05, fontsize=9.5 * sc, va='center')
        T(rx + rw, y, f'{n:,}', fontsize=10 * sc, fontweight='bold', va='center', ha='right')
    return 'entities'


# ---- state map (tile cartogram; same data as the appendix: analysis.state_identified + each comment's position)
def state_counts(df):
    per = {}
    for a, pos in zip(df['analysis'], df['_pos']):
        st = str(a.get('state_identified') or '').strip().upper() if isinstance(a, dict) else ''
        if st in US_STATE_GRID:
            per.setdefault(st, Counter())[pos] += 1
    return per


def _abbr_count(n):
    """4431 -> '4.4k' (same rule as the appendix map)."""
    if n < 1000:
        return str(n)
    return f'{n / 1000:.1f}'.rstrip('0').rstrip('.') + 'k'


MAP_OUTLYING = ('AK', 'HI', 'PR', 'VI', 'GU', 'AS')   # drawn as one line under the mainland


def _map_cells():
    """Appendix grid -> ({abbr: (row, col)}, rows, cols). The mainland keeps the appendix's layout minus its unused
    left column; the six outlying entries sit in a single row beneath it, which saves a column and a row."""
    mainland = {a: rc for a, rc in US_STATE_GRID.items() if a not in MAP_OUTLYING}
    shift = min(c for _, c in mainland.values()) - 1
    cells = {a: (r, c - shift) for a, (r, c) in mainland.items()}
    last = max(r for r, _ in cells.values()) + 1
    for i, a in enumerate(t for t in MAP_OUTLYING if t in US_STATE_GRID):
        cells[a] = (last, i + 1)
    return cells, last, max(c for _, c in cells.values())


def panel_state_map(ctx, slot):
    """Tile map in the appendix's style: each state is an abbreviation over an oppose cell and a support cell.
    Each cell is shaded by its share of the highest count ANY state has for that same position, so the small
    support side is not washed out next to the large oppose side. Fits one quadrant; tiles are side-by-side
    (oppose | support) when there is room and stacked (oppose over support) when the tile is narrow."""
    cv, pal, args, cfg = ctx.cv, ctx.pal, ctx.args, ctx.cfg
    W, H = cv.W, cv.H
    x, w, hy = slot['x'], slot['w'], slot['head_y']
    wide = bool(slot.get('wide'))
    sc, hs = slot.get('scale', 1.0), slot.get('hscale', 1.0)
    cv.T(x, hy, 'COMMENTS BY STATE', **_head(ctx, hs))
    per = state_counts(ctx.df)
    with_state = sum(sum(c.values()) for c in per.values())
    if not with_state:
        cv.T(x, hy - 0.04, 'no state information in this data', fontsize=10 * hs, va='center', ha='left', color=cv.muted)
        return 'state_map'
    op = {s_: c['Oppose'] for s_, c in per.items()}
    su = {s_: c['Support'] for s_, c in per.items()}
    max_op, max_su = max(op.values(), default=0), max(su.values(), default=0)
    light = '#FFFFFF' if luminance(pal['bg']) > 0.5 else pal['bg']      # what a zero shades toward
    header_fill = mix(pal['bg'], pal['neutral'], 0.55)
    empty_fill = mix(pal['bg'], pal['neutral'], 0.85)
    text_on = lambda f: '#FFFFFF' if contrast('#FFFFFF', f) >= contrast(pal['ink'], f) else pal['ink']

    def shade(color, n, n_max):
        if not n or not n_max:
            return light
        return mix(light, color, 0.15 + 0.85 * math.sqrt(n / n_max))

    cv.T(x, hy - 0.028 * hs, f'{with_state:,} of {ctx.total:,} comments name a state', fontsize=9.5 * hs, va='center',
         ha='left', color=cv.muted)
    cells, n_rows, n_cols = _map_cells()
    top = hy - 0.052 * hs
    avail_w, avail_h = w * W, (top - slot.get('map_bottom', slot['chart_bottom'])) * H   # stay clear of the chips below
    tw, th = avail_w / n_cols, avail_h / n_rows
    tw = min(tw, 1.6 * th)             # don't let tiles stretch into slabs
    th = min(th, 1.25 * tw)
    gw, gh = tw * n_cols, th * n_rows
    ax = cv.fig.add_axes([x, top - gh / H, gw / W, gh / H])
    ax.set_xlim(0, gw)
    ax.set_ylim(gh, 0)                 # pixel units, y downward: round corners stay round at any tile shape
    ax.axis('off')
    gap = max(1.5, tw * 0.04)
    tile_w, tile_h = tw - gap, th - gap
    head_h = max(tile_h * 0.30, 13.0)
    body_h = tile_h - head_h
    style = args.map_tile or cfg.get('map_tile', 'auto')
    if style == 'auto':
        style = 'split' if tile_w / 2 >= 34 else 'stack'
    cell_w, cell_h = (tile_w / 2, body_h) if style == 'split' else (tile_w, body_h / 2)
    mode = args.map_counts or cfg.get('map_counts', 'auto')
    show_counts = (cell_h >= 12 and cell_w >= 18) if mode == 'auto' else mode == 'on'
    abbr_pt = max(6.0, min(9.0 * sc, head_h * 0.42))
    longest = max((len(_abbr_count(n)) for n in list(op.values()) + list(su.values())), default=1)
    for abbr, (r, c) in cells.items():
        x0, y0 = (c - 1) * tw + gap / 2, (r - 1) * th + gap / 2
        clip = FancyBboxPatch((x0, y0), tile_w, tile_h, boxstyle=f'round,pad=0,rounding_size={min(tile_w, tile_h) * 0.12}',
                              fc='none', ec='none')
        ax.add_patch(clip)

        def block(bx, by, bw, bh, fc):
            rect = Rectangle((bx, by), bw, bh, fc=fc, ec='none')
            ax.add_patch(rect)
            rect.set_clip_path(clip)

        block(x0, y0, tile_w, head_h, header_fill)
        ax.text(x0 + tile_w / 2, y0 + head_h / 2, abbr, ha='center', va='center', fontsize=abbr_pt,
                fontweight='bold', color=pal['ink'])
        if abbr not in per:
            block(x0, y0 + head_h, tile_w, body_h, empty_fill)
            if show_counts:
                ax.text(x0 + tile_w / 2, y0 + head_h + body_h / 2, '—', ha='center', va='center',
                        fontsize=7 * sc, color=cv.muted)
            continue
        for k, (n, n_max, col) in enumerate(((op[abbr], max_op, pal['oppose']), (su[abbr], max_su, pal['support']))):
            fill = shade(col, n, n_max)
            cx = x0 + (k * tile_w / 2 if style == 'split' else 0)
            cy = y0 + head_h + (0 if style == 'split' else k * body_h / 2)
            block(cx, cy, cell_w, cell_h, fill)
            if show_counts:
                label = _abbr_count(n)
                pt = max(5.0, min(9.5 * sc, cell_h * 0.33, cell_w * (0.9 if sc <= 1 else 0.84) / (0.62 * (len(label) if sc <= 1 else longest)) / 2))
                ax.text(cx + cell_w / 2, cy + cell_h / 2, label, ha='center', va='center', fontsize=pt,
                        fontweight='bold', color=text_on(fill))

    # In a layout the colours are the ones in the word cloud, so no key; just say what the shading means.
    # On a slide of its own there is no cloud to carry the key, so draw one.
    if slot.get('solo'):
        xk = x + w - 0.215
        _chip(ctx, xk, hy - 0.012 * hs, hy, pal['oppose'], 'OPPOSE', hs)
        _chip(ctx, xk + 0.108, hy - 0.012 * hs, hy, pal['support'], 'SUPPORT', hs)
        cv.T(x + w, hy - 0.028 * hs, 'darker = more, within each side', fontsize=9.5 * hs, va='center', ha='right',
             color=cv.muted)
    elif not wide:
        ax.text(0.2 * tw, 4.6 * th, 'darker = more', ha='left', va='center', fontsize=7.5, color=cv.muted)
    ctx.data['states'] = sorted(
        [dict(state=s_, oppose=int(per[s_]['Oppose']), support=int(per[s_]['Support']),
              neither=int(sum(per[s_].values()) - per[s_]['Oppose'] - per[s_]['Support']),
              total=int(sum(per[s_].values()))) for s_ in per], key=lambda d: -d['total'])
    return 'state_map'


# ---- page-2 charts, usable in any quadrant
def panel_backlog(ctx, slot):
    x, w = slot['x'], slot['w']
    sc, hs = slot.get('scale', 1.0), slot.get('hscale', 1.0)
    lm = 0.04 if sc == 1 else 0.055
    draw_backlog(ctx.cv, ctx.df, ctx.args, ctx.cfg, ctx.pal, (x, slot['head_y']), slot['head_y'] - 0.028 * hs,
                 [x + lm, slot['ax_y'], w - lm, slot.get('chart_h', 0.245)], ctx.data, sc=sc, hs=hs)
    return 'backlog'


def panel_cumulative(ctx, slot):
    x, w = slot['x'], slot['w']
    sc, hs = slot.get('scale', 1.0), slot.get('hscale', 1.0)
    lm = 0.045 if sc == 1 else 0.06
    draw_cumulative(ctx.cv, ctx.df, ctx.args, ctx.cfg, ctx.pal, (x, slot['head_y']),
                    [x + lm, slot['ax_y'], w - lm, slot.get('chart_h', 0.255)], ctx.data, head_max_w=w, label_pad_px=10,
                    days_left_right=True, sc=sc, hs=hs)
    return 'cumulative'


PANELS = {'cloud_oppose': lambda c, s: panel_cloud(c, s, 'Oppose'),
          'cloud_support': lambda c, s: panel_cloud(c, s, 'Support'),
          'cloud_both': panel_cloud_both, 'topics': panel_topics, 'timeline': panel_timeline,
          'entities': panel_entities, 'state_map': panel_state_map, 'backlog': panel_backlog,
          'cumulative': panel_cumulative}


def parse_layout(spec, where):
    out = {}
    for part in re.split(r'[,\s]+', (spec or '').strip()):
        if not part:
            continue
        slot, _, panel = part.partition('=')
        slot, panel = slot.strip().lower(), panel.strip().lower().replace('-', '_')
        if slot not in SLOTS:
            sys.exit(f"{where}: unknown quadrant {slot!r}; use {', '.join(SLOTS)}")
        if panel not in PANEL_NAMES:
            sys.exit(f"{where}: unknown figure {panel!r}; use one of {', '.join(PANEL_NAMES)}")
        out[slot] = panel
    return out


def resolve_layout(args, cfg):
    """Quadrant -> figure. Later sources win: defaults < --panel < --clouds combined < config layout < --layout.
    A full-width slot ('top' = tl+tr, 'bottom' = bl+br) replaces the two quadrants under it."""
    layout = dict(DEFAULT_LAYOUT)
    explicit = set()                                              # quadrants the user named, as opposed to defaults
    if (args.panel or cfg.get('panel')) == 'entities':           # the older switch for the bottom-right figure
        layout['br'] = 'entities'
    clouds = args.clouds or cfg.get('clouds', 'separate')
    if clouds not in ('separate', 'combined'):
        sys.exit(f"clouds must be 'separate' or 'combined', got {clouds!r}")
    if clouds == 'combined':                                      # one cloud frees a quadrant for another figure
        layout['tl'] = 'cloud_both'
        extra = (args.extra_figure or cfg.get('extra_figure') or 'state_map').strip().lower().replace('-', '_')
        if extra not in PANEL_NAMES:
            sys.exit(f"extra figure {extra!r} unknown; use one of {', '.join(PANEL_NAMES)}")
        layout['bl'] = extra
    cfg_layout = cfg.get('layout')
    chosen = {}
    if isinstance(cfg_layout, dict):
        chosen.update(parse_layout(','.join(f'{k}={v}' for k, v in cfg_layout.items()), 'config layout'))
    chosen.update(parse_layout(args.layout, '--layout'))
    layout.update(chosen)
    explicit |= set(chosen)
    for wide, (a, b) in WIDE.items():
        if wide not in layout:
            continue
        clash = [q for q in (a, b) if q in explicit and layout.get(q) != 'none']
        if clash:
            sys.exit(f"'{wide}' spans {a} and {b}; take {', '.join(clash)} out of the layout (or drop '{wide}')")
        layout.pop(a, None)
        layout.pop(b, None)
    seen = {}
    for slot, panel in layout.items():
        if panel == 'none':
            continue
        if panel in seen:
            sys.exit(f"figure {panel!r} is placed in both {seen[panel]} and {slot}")
        seen[panel] = slot
    if 'cloud_both' in seen and {'cloud_oppose', 'cloud_support'} & set(seen):
        sys.exit("cloud_both already shows both sides; take cloud_oppose / cloud_support out of the layout")
    return layout


def draw_meter(cv, pal, total, n_op, n_su, n_un, meter, dx=0.0):
    """The comment count and the oppose / support dial. `dx` shifts the block sideways (0 = its place on the slide)."""
    fig, T, muted, disp = cv.fig, cv.T, cv.muted, cv.disp
    n_meter = n_op if meter == 'oppose' else n_su
    frac = n_meter / total if total else 0
    T(0.03 + dx, 0.785, f'{total:,}', fontsize=62, fontweight='bold', va='center', ha='left', family=disp)
    T(0.033 + dx, 0.685, 'comments published', fontsize=19, va='center', ha='left', color=muted)
    if cv.received:                                           # small disclosure; the published count stays the figure
        T(0.034 + dx, 0.640, f"{cv.received['count']:,} comments submitted", fontsize=10.5, va='center', ha='left', color=muted)
    gax = fig.add_axes([0.03 + dx, 0.235, 0.31, 0.40])
    draw_gauge(gax, frac, pal, pal[meter])
    T(0.185 + dx, 0.185, pct_str(n_meter, total).replace('<', '').replace('>', ''), fontsize=54, fontweight='bold',
      color=pal[meter], ha='center', va='center', family=disp)
    T(0.185 + dx, 0.105, meter.upper(), fontsize=20, fontweight='bold', ha='center', va='center', family=disp)
    T(0.185 + dx, 0.055, f'{n_op:,} oppose  \u00b7  {n_su:,} support  \u00b7  {n_un:,} neither', fontsize=10.5,
      ha='center', va='center', color=muted)


def build(df, meta, cfg, pal, out_dir, args):
    cv = Canvas(df, meta, cfg, pal, args)
    fig, W, H, dpi, T = cv.fig, cv.W, cv.H, cv.dpi, cv.T
    muted, disp = cv.muted, cv.disp

    total = len(df)
    n_op = int((df['_pos'] == 'Oppose').sum())
    n_su = int((df['_pos'] == 'Support').sum())
    n_un = total - n_op - n_su
    meter = args.meter or cfg.get('meter', 'oppose')
    if meter not in ('oppose', 'support'):
        sys.exit(f"meter must be 'oppose' or 'support', got {meter!r}")
    rule_id, dates = cv.rule_id, cv.dates

    draw_meter(cv, pal, total, n_op, n_su, n_un, meter)

    # ---- the four quadrants
    v = voices(df)
    ctx = Ctx(cv=cv, df=df, meta=meta, cfg=cfg, pal=pal, args=args, total=total, v=v, rule_id=rule_id)
    drawn = {}
    for slot, name in args.layout_resolved.items():
        if name != 'none':
            drawn[slot] = PANELS[name](ctx, slot_geometry(slot)) or name

    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for fmt in args.formats:
        p = out_dir / f'{args.name}.{fmt}'
        fig.savefig(p, dpi=dpi, facecolor=pal['bg'])
        written.append(p)
    plt.close(fig)

    topics = ctx.data.get('topics') or topic_counts(df, cfg.get('topic_prefix', args.topic_prefix), args.topics)
    data = dict(comments_received=cv.received, rule_id=rule_id, date_range=dates, total=total, oppose=n_op, support=n_su, neither=n_un,
                meter=meter, pct_oppose=round(100 * n_op / total, 1) if total else 0,
                pct_support=round(100 * n_su / total, 1) if total else 0, topics=topics,
                entities=entity_summary(df, args.entities), cloud_words=ctx.data.get('cloud_words', {}),
                voices_used_for_clouds=int(len(v)), palette=pal, panel=drawn.get('br') or drawn.get('bottom') or 'none',
                timeline=ctx.data.get('timeline'), layout=drawn)
    data.update({k: val for k, val in ctx.data.items() if k not in data})
    jp = out_dir / f'{args.name}.json'
    jp.write_text(json.dumps(data, indent=2, default=str), encoding='utf-8')
    return written + [jp], data


# --------------------------------- slide 2 ---------------------------------
def parse_day(value, what):
    try:
        return pd.Timestamp(value).tz_localize(None).normalize()
    except (ValueError, TypeError):
        sys.exit(f'{what} must be a date like 2026-10-13, got {value!r}')


def topic_names_per_row(df, prefix):
    stances = [_as_list((a or {}).get('stances')) for a in df['analysis']]
    any_prefixed = any(x.startswith(prefix) for row in stances for x in row)
    rows = []
    for row in stances:
        names = set()
        for x in row:
            if is_position_tag(x) or (any_prefixed and not x.startswith(prefix)):
                continue
            names.add(x[len(prefix):].strip() if x.startswith(prefix) else x.strip())
        rows.append(names)
    return rows


def topic_matrices(df, prefix, k, basis):
    """Top-k topics; share of each position's comments raising each topic; topic-pair overlap (Jaccard)."""
    names = [t['name'] for t in topic_counts(df, prefix, k)]
    sub = voices(df) if basis == 'voices' else df
    rows = topic_names_per_row(sub, prefix)
    B = np.array([[n in r for n in names] for r in rows], bool).reshape(len(rows), len(names))
    pos = sub['_pos'].to_numpy()
    heat = np.array([[B[pos == p, i].mean() if (pos == p).any() else 0.0 for p in ('Oppose', 'Support', 'Unclear')]
                     for i in range(len(names))])
    inter = B.T.astype(int) @ B.astype(int)
    diag = np.diag(inter)
    union = diag[:, None] + diag[None, :] - inter
    jac = np.divide(inter, union, out=np.zeros(inter.shape, float), where=union > 0)
    return names, heat, jac, int(len(sub))


def style_axes(ax, pal, ylabel_at=None, size=9):
    ax.patch.set_alpha(0)
    for side in ('top', 'right', 'left'):
        ax.spines[side].set_visible(False)
    ax.spines['bottom'].set_color(pal['ink'])
    ax.spines['bottom'].set_linewidth(1.6)
    ax.tick_params(axis='y', length=0, labelsize=size, colors=pal['ink'], pad=3)
    ax.tick_params(axis='x', length=4, width=1.2, labelsize=size, colors=pal['ink'], color=pal['ink'])
    if ylabel_at is not None:
        ax.set_yticks([ylabel_at])
        ax.set_yticklabels([f'{int(ylabel_at):,}'])
        ax.axhline(ylabel_at, color=pal['neutral'], lw=1, zorder=0)


def day_ticks(ax, index, n=4):
    """Label up to n evenly spaced dates; bars/areas are drawn at x = 0..len(index)-1."""
    idx = sorted(set(np.linspace(0, len(index) - 1, min(len(index), n)).round().astype(int)))
    ax.set_xticks(idx)
    ax.set_xticklabels([f'{index[i]:%b} {index[i].day}' for i in idx])


def draw_backlog(cv, df, args, cfg, pal, head_xy, sub_y, rect, out, sc=1.0, hs=1.0):
    """Received minus posted over time. Used by page 2 and by any slide-1 quadrant."""
    fig, T, muted, disp = cv.fig, cv.T, cv.muted, cv.disp
    head = dict(fontsize=12 * hs, fontweight='bold', va='center', ha='left', color=muted, family=disp)
    sub = dict(fontsize=9.5 * hs, va='center', ha='left', color=muted)
    rec_field = args.received_field or cfg.get('received_field', 'received_date')
    post_field = args.posted_field or cfg.get('posted_field', 'date')
    rec = timeline_counts(df, rec_field, weekly=False)
    post = timeline_counts(df, post_field, weekly=False)

    mode = args.difference or cfg.get('difference', 'cumulative')
    T(head_xy[0], head_xy[1], 'RECEIVED − POSTED', **head)
    if rec is not None and post is not None:
        idx = pd.date_range(min(rec.index.min(), post.index.min()), max(rec.index.max(), post.index.max()), freq='D')
        r = rec.sum(axis=1).reindex(idx, fill_value=0)
        q = post.sum(axis=1).reindex(idx, fill_value=0)
        daily = r - q
        diff = daily.cumsum() if mode == 'cumulative' else daily
        T(head_xy[0], sub_y, 'received but not yet posted' if mode == 'cumulative' else 'received minus posted, per day',
          **sub)
        ax = fig.add_axes(rect)
        ax.bar(np.arange(len(idx)), diff.values, color=mix(pal['bg'], pal['ink'], 0.55), width=0.8, linewidth=0)
        ax.axhline(0, color=pal['ink'], lw=1.2, zorder=3)
        hi, lo = int(diff.max()), int(diff.min())
        style_axes(ax, pal, size=9 * sc)
        ticks = sorted({hi} | ({lo} if lo < 0 else set()))
        ax.set_yticks(ticks)
        ax.set_yticklabels([f'{t:,}' for t in ticks])
        for t in ticks:
            ax.axhline(t, color=pal['neutral'], lw=1, zorder=0)
        top = max(hi, 0) * 1.08
        bottom = min(lo, 0) * 1.12
        ax.set_ylim(bottom, top if top > bottom else 1)
        ax.set_xlim(-0.8, len(idx) - 0.2)
        day_ticks(ax, idx, n=4 if sc == 1 else 8)
        out['received_minus_posted'] = [dict(date=f'{d:%Y-%m-%d}', received=int(r[d]), posted=int(q[d]),
                                             daily_difference=int(daily[d]), backlog=int(daily.cumsum()[d]))
                                        for d in idx]
        out['difference_mode'] = mode
    else:
        print('note: the received-vs-posted panel needs both date columns', file=sys.stderr)
        T(head_xy[0], sub_y, 'needs both received and posted dates', **sub)


def draw_cumulative(cv, df, args, cfg, pal, head_xy, rect, out, head_max_w=None, label_pad_px=None,
                    days_left_right=False, sc=1.0, hs=1.0):
    """Cumulative comments against a stated deadline. Used by page 2 and by any slide-1 quadrant."""
    fig, T, muted, disp = cv.fig, cv.T, cv.muted, cv.disp
    head = dict(fontsize=12 * hs, fontweight='bold', va='center', ha='left', color=muted, family=disp)
    rec_field = args.received_field or cfg.get('received_field', 'received_date')
    post_field = args.posted_field or cfg.get('posted_field', 'date')
    deadline_raw = args.deadline or cfg.get('deadline')
    deadline = parse_day(deadline_raw, '--deadline') if deadline_raw else None
    ht = T(head_xy[0], head_xy[1], 'CUMULATIVE COMMENTS' + (' VS DEADLINE' if deadline is not None else ''), **head)
    if head_max_w and ht.get_window_extent(cv.renderer).width / cv.W > head_max_w:   # narrow quadrant
        ht.set_text('CUMULATIVE VS DEADLINE' if deadline is not None else 'CUMULATIVE')
    if deadline is None:
        print('note: no --deadline given, so the cumulative curve is drawn without a deadline marker',
              file=sys.stderr)
    cum_df = timeline_counts(df, rec_field, weekly=False)
    if cum_df is None:
        cum_df = timeline_counts(df, post_field, weekly=False)
    if cum_df is not None:
        cum = cum_df.cumsum()
        start, last = cum.index[0], cum.index[-1]
        days = np.array([(d - start).days for d in cum.index])
        end_day = max((deadline - start).days, days[-1]) if deadline is not None else days[-1]
        ax = fig.add_axes(rect)
        ax.stackplot(days, cum['Oppose'].values, cum['Support'].values, cum['Unclear'].values,
                     colors=[pal['oppose'], pal['support'], pal['neutral']], linewidth=0)
        total_now = int(cum.iloc[-1].sum())
        style_axes(ax, pal, total_now, size=9 * sc)
        ax.set_xlim(0, end_day + 0.4)
        ax.set_ylim(0, total_now * 1.12)
        span_idx = pd.date_range(start, start + pd.Timedelta(days=int(end_day)), freq='D')
        day_ticks(ax, span_idx, n=5 if sc == 1 else 9)
        asof_raw = args.as_of or cfg.get('as_of')
        asof = parse_day(asof_raw, '--as-of') if asof_raw else pd.Timestamp.today().normalize()
        if deadline is not None:
            dd = (deadline - start).days
            ax.axvline(dd, color=pal['ink'], lw=2, ls=(0, (4, 3)), zorder=4)
            pad = 0.45 if not label_pad_px else label_pad_px / (rect[2] * cv.W / (end_day + 0.4))   # days
            ax.text(dd - pad, total_now * 1.09, f'{deadline:%b} {deadline.day}', ha='right', va='top', fontsize=10.5 * sc,
                    fontweight='bold', color=pal['ink'])
            left = (deadline - asof).days
            if left > 0:
                mid = (max(days[-1], (asof - start).days) + dd) / 2
                if days_left_right:
                    mid = dd - pad
                ax.text(mid, total_now * 0.5, f'{left} day{"s" if left != 1 else ""} left', ha='right' if days_left_right else 'center', va='center',
                        fontsize=10.5 * sc, color=muted)
            out['deadline'] = f'{deadline:%Y-%m-%d}'
            out['days_left'] = int(left)
        out['cumulative'] = [dict(date=f'{d:%Y-%m-%d}', oppose=int(r['Oppose']), support=int(r['Support']),
                                  neither=int(r['Unclear'])) for d, r in cum.iterrows()]


HEAT_GEOM = dict(label_x=0.03, text_x=0.048, head_y=0.845, sub_y=0.815, label_w=0.34, x_cells=0.37, cell_w=0.115,
                 top=0.765, bottom=0.445, key_rect_y=0.787, key_text_y=0.795, s=1.0)
OVERLAP_GEOM = dict(head_x=0.745, head_y=0.845, sub_y=0.815, h=0.355, right=0.97, y=0.445, s=1.0)


def draw_heatmap(cv, names, heat, pal, basis_note, g=None):
    """Topics x position: the share of each side's comments that raise each topic. `g` overrides HEAT_GEOM."""
    g = {**HEAT_GEOM, **(g or {})}
    fig, T = cv.fig, cv.T
    k, s_ = len(names), g['s']
    if not k:
        return
    text_on = lambda fill: pal['ink'] if contrast(pal['ink'], fill) >= contrast(pal['bg'], fill) else pal['bg']
    head = dict(fontsize=12 * s_, fontweight='bold', va='center', ha='left', color=cv.muted, family=cv.disp)
    sub = dict(fontsize=9.5 * s_, va='center', ha='left', color=cv.muted)
    T(g['label_x'], g['head_y'], 'TOPICS × POSITION', **head)
    T(g['label_x'], g['sub_y'], f'share of each side’s comments that raise the topic  ·  {basis_note}', **sub)
    cell_w, label_w, x_cells = g['cell_w'], g['label_w'], g['x_cells']
    top, bottom = g['top'], g['bottom']
    row_h = (bottom - top) / k           # negative: rows run downward
    for j, (label, col) in enumerate((('OPPOSE', pal['oppose']), ('SUPPORT', pal['support']),
                                      ('NEITHER', pal['neutral']))):
        cx = x_cells + cell_w * (j + 0.5)
        fig.add_artist(Rectangle((cx - 0.05 * s_, g['key_rect_y']), 0.009 * s_, 0.016 * s_, transform=fig.transFigure,
                                 fc=col, ec='none'))
        T(cx - 0.036 * s_, g['key_text_y'], label, fontsize=10 * s_, fontweight='bold', va='center', ha='left',
          family=cv.disp)
    hax = fig.add_axes([x_cells, bottom, cell_w * 3, top - bottom])
    hax.set_xlim(0, 3)
    hax.set_ylim(k, 0)
    hax.axis('off')
    for i in range(k):
        yc = top + row_h * (i + 0.5)
        T(g['label_x'], yc, str(i + 1), fontsize=11 * s_, fontweight='bold', va='center', ha='left', family=cv.disp)
        lines = cv.wrap(names[i], 9.5 * s_, label_w - 0.03)[:2]
        T(g['text_x'], yc, '\n'.join(lines), fontsize=9.5 * s_, va='center', ha='left', linespacing=1.1)
        for j, col in enumerate((pal['oppose'], pal['support'], pal['neutral'])):
            v = heat[i, j]
            fill = mix(pal['bg'], col, 0.06 + 0.94 * v)
            hax.add_patch(Rectangle((j, i), 1, 1, fc=fill, ec=pal['bg'], lw=2.5))
            hax.text(j + 0.5, i + 0.5, f'{round(v * 100)}%', ha='center', va='center', fontsize=10.5 * s_,
                     fontweight='bold', color=text_on(fill))


def draw_overlap(cv, jac, pal, g=None):
    """Topic co-occurrence: lower triangle, the diagonal carries the topic number. `g` overrides OVERLAP_GEOM."""
    g = {**OVERLAP_GEOM, **(g or {})}
    fig, T = cv.fig, cv.T
    k, s_ = len(jac), g['s']
    if k < 2:
        return
    ink_ramp = lambda v: mix(pal['bg'], pal['ink'], 0.08 + 0.80 * float(v))
    text_on = lambda fill: pal['ink'] if contrast(pal['ink'], fill) >= contrast(pal['bg'], fill) else pal['bg']
    head = dict(fontsize=12 * s_, fontweight='bold', va='center', ha='left', color=cv.muted, family=cv.disp)
    sub = dict(fontsize=9.5 * s_, va='center', ha='left', color=cv.muted)
    T(g['head_x'], g['head_y'], 'TOPIC OVERLAP', **head)
    T(g['head_x'], g['sub_y'], 'comments sharing both topics, %', **sub)
    h = g['h']
    w = h * cv.H / cv.W
    oax = fig.add_axes([g['right'] - w, g['y'], w, h])
    oax.set_xlim(0, k)
    oax.set_ylim(k, 0)
    oax.axis('off')
    for i in range(k):
        for j in range(i + 1):
            if i == j:
                oax.add_patch(Rectangle((j, i), 1, 1, fc=pal['bg'], ec=pal['bg'], lw=2))
                oax.text(j + 0.5, i + 0.5, str(i + 1), ha='center', va='center', fontsize=11 * s_, fontweight='bold',
                         color=pal['ink'], family=cv.disp)
            else:
                fill = ink_ramp(jac[i, j])
                oax.add_patch(Rectangle((j, i), 1, 1, fc=fill, ec=pal['bg'], lw=2))
                oax.text(j + 0.5, i + 0.5, f'{round(jac[i, j] * 100)}', ha='center', va='center', fontsize=9 * s_,
                         fontweight='bold', color=text_on(fill))


def build_detail(df, meta, cfg, pal, out_dir, args):
    cv = Canvas(df, meta, cfg, pal, args)
    fig, W, H, T = cv.fig, cv.W, cv.H, cv.T
    muted, disp = cv.muted, cv.disp
    prefix = cfg.get('topic_prefix', args.topic_prefix)
    k = args.heatmap_topics
    basis = args.basis or cfg.get('basis', 'comments')
    names, heat, jac, n_basis = topic_matrices(df, prefix, k, basis)
    k = len(names)
    out = dict(basis=basis, comments_in_basis=n_basis, topics=names, share_by_position=heat.round(4).tolist(),
               overlap=jac.round(4).tolist())
    basis_note = 'each form-letter campaign counted once' if basis == 'voices' else 'all comments'

    # ---- row 1: topics x position heatmap, and topic co-occurrence
    draw_heatmap(cv, names, heat, pal, basis_note)
    draw_overlap(cv, jac, pal)

    # ---- row 2: dates
    draw_backlog(cv, df, args, cfg, pal, (0.03, 0.40), 0.372, [0.07, 0.095, 0.40, 0.245], out)
    draw_cumulative(cv, df, args, cfg, pal, (0.53, 0.40), [0.58, 0.095, 0.39, 0.255], out)

    paths = cv.save(out_dir, f'{args.name}_2', args.formats)
    jp = out_dir / f'{args.name}_2.json'
    jp.write_text(json.dumps(out, indent=2, default=str), encoding='utf-8')
    return paths + [jp]



# ------------------- any figure as a slide of its own (--slides <figure>) -------------------
# The figure fills the area under the header. SOLO_SCALE enlarges its text and chart parts to suit the larger area;
# a figure not listed there is drawn at its normal size.
SOLO_FIGURES = ('meter',) + tuple(n for n in PANEL_NAMES if n != 'none') + ('heatmap', 'overlap')
SOLO_SLOT = dict(x=0.03, w=0.94, wide=True, solo=True, head_y=0.835, cloud=(0.05, 0.72), chart_top=0.795,
                 chart_bottom=0.05, budget=0.72, ax_y=0.085, chart_h=0.67, map_bottom=0.045, hscale=1.25)
_CLOUD_SOLO = dict(scale=2.0, words_x=1.7)
SOLO_SCALE = {'cloud_oppose': _CLOUD_SOLO, 'cloud_support': _CLOUD_SOLO, 'cloud_both': _CLOUD_SOLO,
              'topics': dict(scale=1.7), 'timeline': dict(scale=1.5), 'entities': dict(scale=1.6),
              'state_map': dict(scale=2.2), 'backlog': dict(scale=1.5), 'cumulative': dict(scale=1.5)}


def build_solo(figure, df, meta, cfg, pal, out_dir, args):
    """One figure on a slide of its own: same frame and header as every other slide. Written as <name>_<figure>."""
    cv = Canvas(df, meta, cfg, pal, args)
    total = len(df)
    n_op = int((df['_pos'] == 'Oppose').sum())
    n_su = int((df['_pos'] == 'Support').sum())
    n_un = total - n_op - n_su
    ctx = Ctx(cv=cv, df=df, meta=meta, cfg=cfg, pal=pal, args=args, total=total,
              v=voices(df) if figure.startswith('cloud') else None, rule_id=cv.rule_id)
    if figure == 'meter':
        meter = args.meter or cfg.get('meter', 'oppose')
        if meter not in ('oppose', 'support'):
            sys.exit(f"meter must be 'oppose' or 'support', got {meter!r}")
        draw_meter(cv, pal, total, n_op, n_su, n_un, meter, dx=0.5 - 0.185)    # centred on the slide
        ctx.data.update(meter=meter, pct_oppose=round(100 * n_op / total, 1) if total else 0,
                        pct_support=round(100 * n_su / total, 1) if total else 0)
    elif figure in ('heatmap', 'overlap'):
        basis = args.basis or cfg.get('basis', 'comments')
        names, heat, jac, n_basis = topic_matrices(df, cfg.get('topic_prefix', args.topic_prefix), args.heatmap_topics, basis)
        ctx.data.update(basis=basis, comments_in_basis=n_basis, topics=names, share_by_position=heat.round(4).tolist(),
                        overlap=jac.round(4).tolist())
        if figure == 'heatmap':
            note = 'each form-letter campaign counted once' if basis == 'voices' else 'all comments'
            draw_heatmap(cv, names, heat, pal, note,
                         dict(label_x=0.03, text_x=0.055, head_y=0.835, sub_y=0.803, label_w=0.43, x_cells=0.47,
                              cell_w=0.165, top=0.735, bottom=max(0.07, 0.735 - 0.09 * len(names)), key_rect_y=0.763,
                              key_text_y=0.772, s=1.45))
        else:
            h = 0.66
            draw_overlap(cv, jac, pal, dict(head_x=0.03, head_y=0.835, sub_y=0.803, h=h, y=0.07,
                                            right=0.5 + h * cv.H / cv.W / 2, s=1.45))
    else:
        PANELS[figure](ctx, {**SOLO_SLOT, **SOLO_SCALE.get(figure, {})})
    name = f'{args.name}_{figure}'
    paths = cv.save(out_dir, name, args.formats)
    data = dict(comments_received=cv.received, figure=figure, rule_id=cv.rule_id, date_range=cv.dates, total=total, oppose=n_op, support=n_su,
                neither=n_un, **ctx.data)
    jp = out_dir / f'{name}.json'
    jp.write_text(json.dumps(data, indent=2, default=str), encoding='utf-8')
    return paths + [jp]


# ----------------------------------- CLI -----------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--regulation', required=True, help='slug, i.e. the folder name under regulations/')
    ap.add_argument('--regulation-dir', help='override regulations/<slug> (config, metadata, default input '
                    'and default output all live here)')
    ap.add_argument('--parquet', help='analysed comments; default: <regulation dir>/full_run.parquet '
                    '(where sync_state.py pulls it and generate_report.py reads it)')
    ap.add_argument('--out-dir', help='where to write the files; default: <regulation dir>, alongside index.html')
    ap.add_argument('--name', default='infographic')
    ap.add_argument('--formats', type=lambda s: s.split(','), default=['png'], help='png,svg,pdf')
    ap.add_argument('--size', type=lambda s: tuple(int(x) for x in s.lower().split('x')), default=(1920, 1080))
    ap.add_argument('--title', default='Public Comments')
    ap.add_argument('--rule-id', default='', help='fallback if regulation_metadata.json has no docket_id')
    ap.add_argument('--date-field', default='date', help="'date' (posted, as in the report) or 'received_date'")
    ap.add_argument('--palette', choices=sorted(PALETTES), help=f'default: {DEFAULT_PALETTE}')
    ap.add_argument('--color', action='append', default=[], metavar='ROLE=#HEX', help=f'roles: {", ".join(ROLES)}')
    ap.add_argument('--meter', choices=['oppose', 'support'], default=None,
                    help='which share the gauge shows (default: oppose, or infographic.meter in the config)')
    ap.add_argument('--slides', default=None,
                    help="comma list of slides to build: 'summary' (default), 'detail' (topics heatmap + overlap, "
                         "received vs posted, cumulative vs deadline; written as <name>_2.png), and/or any single "
                         "figure as a slide of its own, written as <name>_<figure>.png: " + ', '.join(SOLO_FIGURES))
    ap.add_argument('--deadline', default=None, help='comment deadline for the detail slide, e.g. 2026-10-13. '
                    'Never inferred; without it the cumulative curve has no deadline marker')
    ap.add_argument('--as-of', default=None, help='date used for "days left" (default: today; pass it for a reproducible rebuild)')
    ap.add_argument('--heatmap-topics', type=int, default=8, help='topics shown in the heatmap and overlap matrix')
    ap.add_argument('--basis', choices=['comments', 'voices'], default=None,
                    help="detail-slide topic stats: every comment (default) or each form-letter campaign counted once")
    ap.add_argument('--difference', choices=['cumulative', 'daily'], default=None,
                    help="detail slide: 'cumulative' = received but not yet posted as of each date (default); "
                         "'daily' = received minus posted on each date")
    ap.add_argument('--received-field', default=None, help='detail slide: when submitted (default: received_date)')
    ap.add_argument('--posted-field', default=None, help='detail slide: when posted (default: date)')
    ap.add_argument('--logo', default=None, help='PNG/JPG shown in the header after the title (SVG not supported)')
    ap.add_argument('--website', default=None, help='text shown in the header next to the logo, e.g. example.org')
    ap.add_argument('--logo-height', type=float, default=0.06, help='logo height as a fraction of slide height')
    ap.add_argument('--panel', choices=['timeline', 'entities'], default=None,
                    help='bottom-right panel: comments over time (default) or the entity-type summary')
    ap.add_argument('--timeline-date-field', default=None,
                    help="date column for the timeline (default: received_date, i.e. when people submitted; "
                         "'date' is when the agency posted, which happens in batches)")
    ap.add_argument('--layout', default=None,
                    help="figures for the four quadrants right of the count and meter: 'tl=..,tr=..,bl=..,br=..' "
                         f"(partial is fine); 'top' / 'bottom' merge a row into one wide slot. Figures: {', '.join(PANEL_NAMES)}. Default: "
                         + ','.join(f'{k}={v}' for k, v in DEFAULT_LAYOUT.items()))
    ap.add_argument('--clouds', choices=['separate', 'combined'], default=None,
                    help="'combined' = one word cloud for both sides (sizes stay relative within each side, colour "
                         "marks the side), which frees a quadrant for --extra-figure")
    ap.add_argument('--extra-figure', default=None,
                    help='figure for the quadrant freed by --clouds combined (default: state_map)')
    ap.add_argument('--combined-balance', choices=['equal', 'relative'], default=None,
                    help="combined cloud: 'equal' (default) gives both sides the same size at each rank, so heavy "
                         "words are split evenly; 'relative' sizes each word against its own side's biggest")
    ap.add_argument('--combined-share', type=float, default=0.6,
                    help='words per side in the combined cloud, as a share of --words (default 0.6)')
    ap.add_argument('--map-counts', choices=['auto', 'on', 'off'], default=None,
                    help="state map: print each side's count in its tile. 'auto' (default) = when they fit")
    ap.add_argument('--map-tile', choices=['auto', 'split', 'stack'], default=None,
                    help="state map tile: 'split' = oppose | support side by side (the appendix's look), "
                         "'stack' = oppose over support (narrower); 'auto' (default) picks by the room there is")
    ap.add_argument('--font', help="body text: an installed font name or a .ttf/.otf path (default: DejaVu Sans)")
    ap.add_argument('--display-font', help='the big labels: comment total, meter %% and label, title, section '
                    'headers, entity %%. Default: same as --font')
    ap.add_argument('--cloud-font', help='word clouds. Default: same as --font')
    ap.add_argument('--mono-font', help='rule ID and date in the header (default: DejaVu Sans Mono)')
    ap.add_argument('--topics', type=int, default=6)
    ap.add_argument('--topic-prefix', default='Concern:')
    ap.add_argument('--oppose-tag', action='append', default=[], metavar='TEXT',
                    help="stance tags containing this text count as opposing (repeatable; default 'Position: Oppose')")
    ap.add_argument('--support-tag', action='append', default=[], metavar='TEXT',
                    help="stance tags containing this text count as supporting (repeatable; default 'Position: Support')")
    ap.add_argument('--entities', type=int, default=5, help='organisation types listed before "+N more"')
    ap.add_argument('--words', type=int, default=40, help='words per cloud')
    ap.add_argument('--contrast', type=float, default=1.0, help='0 = raw word frequency, higher = favour words '
                    'that distinguish one side from the other')
    ap.add_argument('--boilerplate', type=float, default=0.03, help='strip 4-word runs shared by more than this '
                    'share of comments (form templates); 0 disables')
    ap.add_argument('--min-df-frac', type=float, default=0.004)
    ap.add_argument('--stopwords', type=lambda s: [w for w in s.split(',') if w], default=[],
                    help="comma-separated extra stop words; a trailing * matches any word starting with it (oppos*)")
    ap.add_argument('--stopwords-file', action='append', default=[], metavar='FILE',
                    help='plain-text stop-word list (words separated by newlines/spaces/commas, # comments); '
                    'repeatable, and added to the built-in list and any --stopwords')
    ap.add_argument('--seed', type=int, default=7)
    ap.add_argument('--strict', action='store_true', help='fail instead of warn on palette accessibility issues')
    args = ap.parse_args(argv)

    here = Path(__file__).resolve().parent
    reg = Path(args.regulation_dir) if args.regulation_dir else here / 'regulations' / args.regulation
    if not reg.is_dir():
        sys.exit(f'Regulation directory not found: {reg}')
    cfg_all = yaml.safe_load((reg / 'analyzer_config.yaml').read_text(encoding='utf-8')) if (reg / 'analyzer_config.yaml').exists() else {}
    cfg = (cfg_all or {}).get('infographic') or {}
    meta = json.loads((reg / 'regulation_metadata.json').read_text(encoding='utf-8')) if (reg / 'regulation_metadata.json').exists() else {}
    meta.setdefault('docket_id', args.rule_id or args.regulation)

    pal = dict(PALETTES[args.palette or cfg.get('palette', DEFAULT_PALETTE)])
    pal.update({k: v for k, v in (cfg.get('colors') or {}).items() if k in ROLES})
    for kv in args.color:
        role, _, val = kv.partition('=')
        if role not in ROLES or not re.fullmatch(r'#?[0-9a-fA-F]{6}', val):
            sys.exit(f'--color expects ROLE=#RRGGBB with ROLE in {ROLES}; got {kv!r}')
        pal[role] = '#' + val.lstrip('#').upper()
    sw_files = list(args.stopwords_file)
    if cfg.get('stopwords_file'):
        sw_files.append(reg / cfg['stopwords_file'])  # relative to the regulation dir
    args.stopwords_from_file = set()
    for f in sw_files:
        if not Path(f).is_file():
            sys.exit(f'Stop-word file not found: {f}')
        args.stopwords_from_file |= load_stopwords_file(f)
    def font_spec(key, default):
        v = getattr(args, key) or cfg.get(key)
        if not v:
            return default
        return str(reg / v) if (reg / str(v)).is_file() else str(v)  # config paths are relative to the regulation dir
    body_spec = font_spec('font', 'DejaVu Sans')
    args.fonts = dict(body=resolve_font(body_spec, 'body'),
                      display=resolve_font(font_spec('display_font', body_spec), 'display'),
                      cloud=resolve_font(font_spec('cloud_font', body_spec), 'cloud'),
                      mono=resolve_font(font_spec('mono_font', 'DejaVu Sans Mono'), 'mono'))
    logo = args.logo or cfg.get('logo')
    if logo:
        logo_path = reg / logo if (reg / str(logo)).is_file() else Path(logo)
        if not logo_path.is_file():
            sys.exit(f'logo file not found: {logo}')
        if logo_path.suffix.lower() == '.svg':
            sys.exit('SVG logos are not supported; export a PNG (transparent background works)')
        args.logo = str(logo_path)
    args.website = args.website or cfg.get('website')
    slides = args.slides or cfg.get('slides', 'summary')
    slides = [x.strip().lower().replace('-', '_') for x in (slides.split(',') if isinstance(slides, str) else slides) if x.strip()]
    if not slides or set(slides) - ({'summary', 'detail'} | set(SOLO_FIGURES)):
        sys.exit("--slides takes 'summary', 'detail' and/or a figure name: " + ', '.join(SOLO_FIGURES))
    args.layout_resolved = resolve_layout(args, cfg)
    warns = check_palette(pal)
    for w in warns:
        print(f'palette warning: {w}', file=sys.stderr)
    if warns and args.strict:
        sys.exit(1)

    parquet = Path(args.parquet) if args.parquet else reg / 'full_run.parquet'
    if not parquet.is_file():
        sys.exit(f'Parquet not found: {parquet}  (pass --parquet, or run `python sync_state.py pull` first)')
    for side, key, flag in (('Oppose', 'oppose_tags', args.oppose_tag), ('Support', 'support_tags', args.support_tag)):
        wanted = list(flag) or list(cfg.get(key) or [])
        if wanted:
            POSITION_TAGS[side] = wanted
    df = load_comments(parquet)
    out_dir = Path(args.out_dir) if args.out_dir else reg
    files, data = [], None
    for sl in dict.fromkeys(slides):                      # in the order given, each once
        if sl == 'summary':
            f, data = build(df, meta, cfg, pal, out_dir, args)
            files += f
        elif sl == 'detail':
            files += build_detail(df, meta, cfg, pal, out_dir, args)
        else:
            files += build_solo(sl, df, meta, cfg, pal, out_dir, args)
    for f in files:
        print(f)
    return data


if __name__ == '__main__':
    main()
