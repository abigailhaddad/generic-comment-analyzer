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

    python make_infographic.py --regulation USBC-2026-0628
    python make_infographic.py --regulation USBC-2026-0628 --palette plum-gold \
           --color oppose=#8E2C72 --topics 5 --words 50 --formats png,svg

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
from matplotlib.patches import Circle, Polygon, Rectangle, Wedge  # noqa: E402
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
    return pd.read_parquet(io.BytesIO(raw))


def _as_list(x):
    if x is None:
        return []
    if hasattr(x, 'tolist'):
        x = x.tolist()
    return x if isinstance(x, list) else []


def position(analysis):
    """Oppose / Support / Unclear. Mirrors generate_report.comment_position()."""
    a = analysis if isinstance(analysis, dict) else {}
    if a.get('verified_stance') in ('Oppose', 'Support', 'Unclear'):
        return a['verified_stance']
    s = _as_list(a.get('stances'))
    if any('Position: Oppose' in x for x in s):
        return 'Oppose'
    if any('Position: Support' in x for x in s):
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
            if s.startswith('Position:'):
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
notice rin cfr http https www com org gov html pdf attached attachment see name names commenting writing nprm quot rdquo ldquo rsquo lsquo amp nbsp
""".split())
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
    campaign, so a 6,500-copy template counts once instead of drowning out everyone else."""
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
    the opposite side (so shared words like 'census' fade out)."""
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


def find_font(family, weight='bold'):
    return font_manager.findfont(font_manager.FontProperties(family=family, weight=weight))


def render_cloud(weights, w, h, color, font_path, min_px, max_px, seed=7, margin=4):
    """Greedy spiral packing (largest word first) into a w x h RGBA image."""
    rng = np.random.default_rng(seed)
    occ = np.zeros((h, w), bool)
    fill = tuple(int(c * 255) for c in hex_rgb(color)) + (255,)
    # transparent but same RGB as the ink, so resampling never blends in black fringes
    img = Image.new('RGBA', (w, h), fill[:3] + (0,))
    draw = ImageDraw.Draw(img)
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
                draw.text((x + margin - l, y + margin - t), word, font=font, fill=fill)
                break
            size = int(size * 0.9)
    return img


# --------------------------------- drawing ---------------------------------
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


def build(df, meta, cfg, pal, out_dir, args):
    W, H = args.size
    dpi = 144
    fig = plt.figure(figsize=(W / dpi, H / dpi), dpi=dpi, facecolor=pal['bg'])
    muted = mix(pal['ink'], pal['bg'], 0.38)
    font = cfg.get('font', args.font)
    plt.rcParams.update({'font.family': font, 'text.parse_math': False, 'svg.fonttype': 'path'})
    T = lambda x, y, s, **k: fig.text(x, y, s, **{'color': pal['ink'], **k})
    renderer = fig.canvas.get_renderer()

    def fit(x, y, s, max_w, **k):
        """Left-aligned text, ellipsised until it fits max_w (figure-width fraction)."""
        t = T(x, y, s, ha='left', **k)
        while len(s) > 4 and t.get_window_extent(renderer).width / (W) > max_w:
            s = s[:-2].rstrip() + '…' if not s.endswith('…') else s[:-2].rstrip() + '…'
            t.set_text(s)
        return t

    def wrap(s, fontsize, max_w):
        """Greedy word-wrap to max_w (figure-width fraction). Never truncates."""
        probe = fig.text(0, 0, '', fontsize=fontsize)
        lines, cur = [], ''
        for word in s.split():
            trial = f'{cur} {word}'.strip()
            probe.set_text(trial)
            if cur and probe.get_window_extent(renderer).width / W > max_w:
                lines.append(cur)
                cur = word
            else:
                cur = trial
        probe.remove()
        return lines + ([cur] if cur else [])

    total = len(df)
    n_op = int((df['_pos'] == 'Oppose').sum())
    n_su = int((df['_pos'] == 'Support').sum())
    n_un = total - n_op - n_su
    meter = args.meter or cfg.get('meter', 'oppose')
    if meter not in ('oppose', 'support'):
        sys.exit(f"meter must be 'oppose' or 'support', got {meter!r}")
    n_meter = n_op if meter == 'oppose' else n_su
    frac = n_meter / total if total else 0
    rule_id = meta.get('docket_id') or args.rule_id
    dates = date_range_label(df, args.date_field)
    title = cfg.get('title', args.title)

    # frame + header
    fig.add_artist(Rectangle((0.008, 0.014), 0.984, 0.972, transform=fig.transFigure, fill=False,
                             ec=pal['ink'], lw=2.4))
    T(0.03, 0.925, title.upper(), fontsize=19, fontweight='bold', va='center', ha='left')
    T(0.97, 0.925, f'{rule_id}   ·   {dates}' if dates else rule_id, fontsize=15, ha='right',
      va='center', family='DejaVu Sans Mono', fontweight='bold', color=pal['ink'])
    fig.add_artist(plt.Line2D([0.03, 0.97], [0.875, 0.875], transform=fig.transFigure, color=pal['ink'], lw=1.6))

    # ---- left: total + gauge
    T(0.03, 0.785, f'{total:,}', fontsize=62, fontweight='bold', va='center', ha='left')
    T(0.033, 0.685, 'comments', fontsize=19, va='center', ha='left', color=muted)
    gax = fig.add_axes([0.03, 0.235, 0.31, 0.40])
    draw_gauge(gax, frac, pal, pal[meter])
    T(0.185, 0.185, pct_str(n_meter, total).replace('<', '').replace('>', ''), fontsize=54, fontweight='bold',
      color=pal[meter], ha='center', va='center')
    T(0.185, 0.105, meter.upper(), fontsize=20, fontweight='bold', ha='center', va='center')
    T(0.185, 0.055, f'{n_op:,} oppose  ·  {n_su:,} support  ·  {n_un:,} neither', fontsize=10.5,
      ha='center', va='center', color=muted)

    # ---- middle: word clouds
    v = voices(df)
    text_col = 'text' if 'text' in df else 'comment_text'
    stop = set(STOP) | {w.lower() for w in cfg.get('stopwords', [])} | {w.lower() for w in args.stopwords}
    stop |= args.stopwords_from_file
    stop |= {t for t in re.split(r'[^a-z]+', rule_id.lower()) if len(t) >= 3}
    min_df = max(3, math.ceil(args.min_df_frac * len(v)))
    fp = find_font(font)
    boxes = {'Oppose': (0.375, 0.485, 0.30, 0.335), 'Support': (0.375, 0.075, 0.30, 0.335)}
    other = {'Oppose': 'Support', 'Support': 'Oppose'}
    cloud_words = {}
    for grp, (x, y, w, h) in boxes.items():
        col = pal[grp.lower()]
        wts = cloud_weights(v, grp, other[grp], text_col, stop, args.words, args.contrast, min_df, args.boilerplate)
        if len(wts) < 5 and args.boilerplate:  # everything was template text: fall back to raw words
            wts = cloud_weights(v, grp, other[grp], text_col, stop, args.words, args.contrast, min_df, 0)
        cloud_words[grp] = list(wts)
        ss = 2
        pw, ph = int(w * W) * ss, int(h * H) * ss
        img = render_cloud(wts, pw, ph, col, fp, min_px=int(14 * ss * W / 1920 * 1.6),
                           max_px=int(64 * ss * W / 1920 * 1.6), seed=args.seed)
        ax = fig.add_axes([x, y, w, h])
        img = img.resize((pw // ss, ph // ss), Image.LANCZOS)  # constant RGB, so no halo
        ax.imshow(img, interpolation='none', aspect='auto')
        ax.axis('off')
        fig.add_artist(Rectangle((x, y + h + 0.012), 0.014, 0.024, transform=fig.transFigure, fc=col, ec='none'))
        T(x + 0.022, y + h + 0.024, grp.upper(), fontsize=12, fontweight='bold', va='center', ha='left')

    # ---- right top: topics
    rx, rw = 0.715, 0.255
    T(rx, 0.848, 'TOPICS', fontsize=12, fontweight='bold', va='center', ha='left', color=muted)
    topics = topic_counts(df, cfg.get('topic_prefix', args.topic_prefix), args.topics)
    tmax = max((t['n'] for t in topics), default=1)
    bh, line_h, gap = 0.017, 0.0235, 0.012
    fs = 10.5
    while True:  # shrink the label font a little if wrapping would crowd the entity block
        wrapped = [wrap(t['name'], fs, rw) for t in topics]
        line_h_fs = line_h * fs / 10.5
        need = sum(0.0185 + (len(w) - 1) * 0.0205 + 0.006 + bh + gap for w in wrapped)
        if need <= 0.40 or fs <= 8.5:
            break
        fs -= 0.5
    cursor = 0.825
    for t, lines in zip(topics, wrapped):
        label_h = (0.0185 + (len(lines) - 1) * 0.0205) * fs / 10.5
        T(rx, cursor, chr(10).join(lines), fontsize=fs, va='top', ha='left', linespacing=1.15)
        bar_y = cursor - label_h - 0.006 - bh
        T(rx + rw, bar_y + bh / 2, pct_str(t['n'], total), fontsize=10.5, fontweight='bold', va='center', ha='right')
        x0 = rx
        for key, col in (('oppose', pal['oppose']), ('support', pal['support']), ('other', pal['neutral'])):
            wseg = rw * 0.85 * t[key] / tmax
            fig.add_artist(Rectangle((x0, bar_y), wseg, bh, transform=fig.transFigure, fc=col, ec='none'))
            x0 += wseg
        cursor = bar_y - gap

    # ---- right bottom: entities
    ent = entity_summary(df, args.entities)
    T(rx, 0.40, 'WHO IS COMMENTING', fontsize=12, fontweight='bold', va='center', ha='left', color=muted)
    T(rx, 0.325, pct_str(ent['primary_n'], ent['total']).replace('<', '').replace('>', ''),
      fontsize=40, fontweight='bold', va='center', ha='left')
    T(rx + 0.105, 0.325, shorten(cfg.get('entity_primary_label', ent['primary']), 26), fontsize=13,
      va='center', ha='left', color=muted)
    y0 = 0.245
    rows = list(ent['shown']) + ([(f"+{ent['other_types']} more types", ent['other_n'])] if ent['other_n'] else [])
    rmax = max((n for _, n in rows), default=1)
    step = min(0.05, 0.19 / max(len(rows), 1))
    for i, (name, n) in enumerate(rows):
        y = y0 - i * step
        fig.add_artist(Rectangle((rx, y - 0.016), rw * 0.62 * n / rmax + 0.002, 0.032, transform=fig.transFigure,
                                 fc=pal['neutral'], ec='none'))
        fit(rx + 0.004, y, name, rw - 0.05, fontsize=9.5, va='center')
        T(rx + rw, y, f'{n:,}', fontsize=10, fontweight='bold', va='center', ha='right')

    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for fmt in args.formats:
        p = out_dir / f'{args.name}.{fmt}'
        fig.savefig(p, dpi=dpi, facecolor=pal['bg'])
        written.append(p)
    plt.close(fig)

    data = dict(rule_id=rule_id, date_range=dates, total=total, oppose=n_op, support=n_su, neither=n_un,
                meter=meter, pct_oppose=round(100 * n_op / total, 1) if total else 0,
                pct_support=round(100 * n_su / total, 1) if total else 0, topics=topics, entities=ent, cloud_words=cloud_words,
                voices_used_for_clouds=int(len(v)), palette=pal)
    jp = out_dir / f'{args.name}.json'
    jp.write_text(json.dumps(data, indent=2, default=str), encoding='utf-8')
    return written + [jp], data


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
    ap.add_argument('--font', default='DejaVu Sans')
    ap.add_argument('--topics', type=int, default=6)
    ap.add_argument('--topic-prefix', default='Concern:')
    ap.add_argument('--entities', type=int, default=5, help='organisation types listed before "+N more"')
    ap.add_argument('--words', type=int, default=40, help='words per cloud')
    ap.add_argument('--contrast', type=float, default=1.0, help='0 = raw word frequency, higher = favour words '
                    'that distinguish one side from the other')
    ap.add_argument('--boilerplate', type=float, default=0.03, help='strip 4-word runs shared by more than this '
                    'share of comments (form templates); 0 disables')
    ap.add_argument('--min-df-frac', type=float, default=0.004)
    ap.add_argument('--stopwords', type=lambda s: [w for w in s.split(',') if w], default=[],
                    help='comma-separated extra stop words')
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
    warns = check_palette(pal)
    for w in warns:
        print(f'palette warning: {w}', file=sys.stderr)
    if warns and args.strict:
        sys.exit(1)

    parquet = Path(args.parquet) if args.parquet else reg / 'full_run.parquet'
    if not parquet.is_file():
        sys.exit(f'Parquet not found: {parquet}  (pass --parquet, or run `python sync_state.py pull` first)')
    df = load_comments(parquet)
    out_dir = Path(args.out_dir) if args.out_dir else reg
    files, data = build(df, meta, cfg, pal, out_dir, args)
    for f in files:
        print(f)
    return data


if __name__ == '__main__':
    main()
