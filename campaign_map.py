#!/usr/bin/env python3
"""One map of EVERY form-letter campaign in a regulation, by text similarity.

Reads the same full_run.parquet that generate_report.py reads and draws a single
figure in the style of a packed-bubble "who is behind these comments" slide:

  * each campaign is a shaded disc (orange = opposes the rule, blue = supports it),
    its radius growing with the cube root of its members so a 5-member campaign
    stays visible next to one with thousands of members;
  * each DISTINCT text inside it is a dot whose area is its number of exact
    copies, and the farther a dot sits from its disc's centre the more that text
    deviates from the campaign's template (the "edited copies");
  * every distinct text that is NOT in a campaign is a small grey dot;
  * nearby = similar wording. Letter families (cluster_families) are labelled.

The similarity is latent-semantic (lsa_similarity.py: TF-IDF -> truncated SVD),
an alternative to the 5-gram MinHash campaign_similarity.py. This script only
READS campaign_id / family_id; it never changes how campaigns are detected.

Layout is UMAP (cosine) by default. UMAP is an optional dependency
(requirements-optional.txt); --layout pca needs nothing extra. Only campaign
CENTRES and non-campaign texts go through the 2-D layout. Campaign members do
not, because members of a form letter are ~identical and UMAP rescales local
distances, so it inflates their tiny differences into dozens of fake islands.
Instead they fan out around their campaign by their own deviation from it.

    python campaign_map.py --regulation <slug>
    python campaign_map.py --regulation <slug> --k 200 --layout pca --formats png,svg

Writes campaign_map.png (and .svg if asked) plus campaign_map_campaigns.csv, a
per-campaign summary with the terms that most define it, into the regulation's
folder (or --out-dir).
"""
import argparse
import collections
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional

import matplotlib

matplotlib.use('Agg')
import matplotlib.patheffects as pe  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Circle  # noqa: E402

import lsa_similarity as lsa  # noqa: E402

# Okabe-Ito, the same default as make_infographic.py, so the two figures agree.
COLORS = {'Oppose': '#D55E00', 'Support': '#0072B2', 'Other': '#8A8984'}
LABELS = {'Oppose': 'Opposes the rule', 'Support': 'Supports the rule', 'Other': 'Other / unclear'}
BG, INK, INK2, UNASSIGNED = '#FAF7F0', '#1B1B1B', '#5A5A55', '#C3C2BD'
DOT_AREA = 7.0   # marker area (pt^2) per exact copy


# ------------------------------- data in -----------------------------------
def position(analysis: Any) -> str:
    """Oppose / Support / Other. Mirrors generate_report.comment_position(): the
    second-pass verified_stance wins, else the Position tag(s), Oppose beating
    Support when a comment carries both."""
    a = analysis if isinstance(analysis, dict) else {}
    verified = a.get('verified_stance')
    if isinstance(verified, str) and verified:
        return verified if verified in ('Oppose', 'Support') else 'Other'
    stances = a.get('stances')
    stances = stances.tolist() if hasattr(stances, 'tolist') else stances
    stances = stances if isinstance(stances, list) else []
    if any('Position: Oppose' in s for s in stances):
        return 'Oppose'
    if any('Position: Support' in s for s in stances):
        return 'Support'
    return 'Other'


def load_frame(parquet: Path) -> pd.DataFrame:
    """The columns the map needs, with campaign_id / family_id as int-or-None."""
    df = pd.read_parquet(parquet)
    need = ['text', 'campaign_id']
    missing = [c for c in need if c not in df.columns]
    if missing:
        sys.exit(f'{parquet} has no {missing} column(s); run pipeline.py through campaign detection first.')
    n = len(df)

    def column(name, default):
        return df[name].tolist() if name in df.columns else [default] * n

    def int_or_none(v):
        return None if v is None or pd.isna(v) else int(v)

    return pd.DataFrame({
        'text': [t if isinstance(t, str) else '' for t in df['text'].tolist()],
        # dtype=object: a plain list of ints and None would be coerced back to float NaN
        'campaign_id': pd.Series([int_or_none(v) for v in df['campaign_id'].tolist()], dtype=object),
        'family_id': pd.Series([int_or_none(v) for v in column('family_id', None)], dtype=object),
        'canonical': [c if isinstance(c, str) else '' for c in column('campaign_canonical', '')],
        'position': [position(a) for a in column('analysis', None)],
    })


# ------------------------------ computation --------------------------------
class MapData(NamedTuple):
    n_comments: int
    n_distinct: int
    k: int
    layout: str
    campaigns: List[Dict[str, Any]]   # one per campaign: id, family, members, position, xy, radius, top_terms, tightness
    members: pd.DataFrame             # one row per distinct text in a campaign: x, y, copies, position, campaign_id
    others: pd.DataFrame              # one row per distinct text not in a campaign: x, y, copies
    n_unplaced: int                   # non-campaign texts the subspace kept too little of to place


def relax_discs(pos: np.ndarray, radii: np.ndarray, pad: float = 0.0, spring: float = 0.02,
                spring_iters: int = 300, max_iter: int = 2000) -> np.ndarray:
    """Push overlapping discs apart (heavier discs move less), pulled gently back
    toward their original positions for `spring_iters` rounds, then a spring-free
    finish so the result is overlap-free (by `pad`) rather than merely close to it."""
    anchor = pos.astype(float).copy()
    pos = anchor.copy()
    n = len(pos)
    if n < 2:
        return pos
    rng = np.random.default_rng(0)
    pos += rng.normal(0, 1e-6 * (np.ptp(anchor) or 1.0), pos.shape)   # break exact ties
    mass = radii ** 2 + 1e-12
    need = radii[:, None] + radii[None, :] + pad
    np.fill_diagonal(need, 0.0)

    def step():
        d = pos[None, :, :] - pos[:, None, :]                 # d[i, j] = pos[j] - pos[i]
        dist = np.sqrt((d ** 2).sum(-1)) + 1e-12
        overlap = np.clip(need - dist, 0.0, None)
        if not overlap.any():
            return False
        unit = d / dist[..., None]
        share_i = mass[None, :] / (mass[:, None] + mass[None, :])   # how much i moves relative to j
        move = -(unit * (overlap * share_i)[..., None]).sum(1)      # i is pushed away from every j
        pos[:] += move
        return True

    for _ in range(spring_iters):
        step()
        pos += spring * (anchor - pos)
    for _ in range(max_iter):          # spring-free finish
        if not step():
            break
    return pos


def layout_2d(U: np.ndarray, method: str, neighbors: int, min_dist: float, seed: int) -> np.ndarray:
    """2-D coordinates for unit-length rows U, by cosine similarity."""
    if len(U) < 3:
        raise ValueError(f'need at least 3 points to lay out, got {len(U)}')
    if method == 'umap':
        try:
            import umap
        except ImportError:
            sys.exit('--layout umap needs umap-learn: pip install -r requirements-optional.txt '
                     '(or use --layout pca, which needs nothing extra)')
        return umap.UMAP(n_neighbors=max(2, min(neighbors, len(U) - 1)), min_dist=min_dist, spread=1.5,
                         metric='cosine', random_state=seed).fit_transform(U)
    centred = U - U.mean(axis=0)
    _, _, vt = np.linalg.svd(centred, full_matrices=False)
    xy = centred @ vt[:2].T
    return xy if xy.shape[1] == 2 else np.pad(xy, ((0, 0), (0, 2 - xy.shape[1])))


def _id_array(series: pd.Series) -> np.ndarray:
    """Object array of int ids with None for missing (NaN, None or pd.NA)."""
    return np.array([None if pd.isna(v) else int(v) for v in series.tolist()], dtype=object)


def _majority(values) -> Any:
    return collections.Counter(values).most_common(1)[0][0]


def build_map(df: pd.DataFrame, k: int = 100, layout: str = 'umap', neighbors: int = 15,
              min_dist: float = 0.3, min_retained: float = 0.05, spread: float = 0.2,
              top_n_terms: int = 8, max_df_frac: float = 1.0, seed: int = 0) -> MapData:
    """Everything the figure needs, from a frame with text / campaign_id / family_id /
    position columns (see load_frame). No plotting, no file I/O."""
    # pandas turns an int-or-None column into float with NaN, and NaN != NaN would make
    # every non-campaign text its own "campaign", so rebuild as real ints / None.
    cid = _id_array(df['campaign_id'])
    fam = _id_array(df['family_id'])
    pos_lab = df['position'].to_numpy(dtype=object)

    # A distinct text is (normalised wording, campaign): the same wording filed under two
    # campaigns stays two units, so every campaign's member count equals the pipeline's.
    norm = [lsa.normalize_text(t) for t in df['text']]
    unit_key = [f'{t}\x00{"" if c is None else c}' for t, c in zip(norm, cid)]
    keys, inverse, copies = np.unique(np.array(unit_key, dtype=object), return_inverse=True, return_counts=True)
    inverse = inverse.ravel()
    nU = len(keys)
    tfidf = lsa.build_tfidf([str(k_).split('\x00')[0] for k_ in keys], max_df_frac=max_df_frac)
    sub = lsa.fit_subspace(tfidf.matrix, k=k, weights=np.sqrt(copies), seed=seed)
    Z = lsa.project(tfidf.matrix, sub)
    q = lsa.retained_norm(Z)
    U = lsa.unit_rows(Z)
    by_text = collections.defaultdict(list)
    for row, u in enumerate(inverse):
        by_text[u].append(row)

    # per distinct text: its campaign (majority of its copies), family, position
    u_cid = np.empty(nU, dtype=object)
    u_fam = np.empty(nU, dtype=object)
    u_pos = np.empty(nU, dtype=object)
    for u in range(nU):
        rows = by_text[u]
        u_cid[u] = _majority([cid[r] for r in rows])
        u_fam[u] = _majority([fam[r] for r in rows])
        u_pos[u] = _majority([pos_lab[r] for r in rows])
    in_camp = np.array([c is not None for c in u_cid])
    camp_ids = sorted({c for c in u_cid if c is not None})
    if not camp_ids:
        sys.exit('No campaigns in this data (campaign_id is empty everywhere); nothing to map.')

    # campaign centres: copies-weighted mean direction of its texts
    centers = lsa.group_centroids(U[in_camp], u_cid[in_camp], weights=copies[in_camp])
    C = np.stack([centers[c] for c in camp_ids])

    placeable = np.where(~in_camp & (q >= min_retained))[0]
    pts = np.vstack([C, U[placeable]])
    xy = layout_2d(pts, layout, neighbors, min_dist, seed)
    xy_c, xy_o = xy[:len(camp_ids)], xy[len(camp_ids):]
    width = float(np.ptp(xy[:, 0])) or 1.0

    sizes = np.array([int(copies[u_cid == c].sum()) for c in camp_ids], dtype=float)
    radii = 0.075 * width * (sizes / sizes.max()) ** (1 / 3)
    disc_xy = relax_discs(xy_c, radii, pad=0.004 * width)

    campaigns, member_rows = [], []
    for i, c in enumerate(camp_ids):
        idx = np.where(u_cid == c)[0]
        w = copies[idx].astype(float)
        resid = U[idx] - np.outer(U[idx] @ centers[c], centers[c])
        if len(idx) > 2:
            centred = resid - np.average(resid, axis=0, weights=w)
            _, _, vt = np.linalg.svd(centred, full_matrices=False)
            off = centred @ vt[:2].T
            if off.shape[1] < 2:
                off = np.pad(off, ((0, 0), (0, 2 - off.shape[1])))
        else:
            off = np.zeros((len(idx), 2))
        off = off * (spread * width)
        good = q[idx] >= min_retained
        tight = float(np.average(U[idx][good] @ centers[c], weights=w[good])) if good.any() else float('nan')
        fam_ids = [f for f in u_fam[idx] if f is not None]
        canonical = next((df['canonical'].iloc[by_text[u][0]] for u in idx if df['canonical'].iloc[by_text[u][0]]), '')
        campaigns.append({
            'campaign_id': c, 'family_id': _majority(fam_ids) if fam_ids else None,
            'members': int(sizes[i]), 'distinct_texts': int(len(idx)),
            'position': _majority(list(u_pos[idx])), 'x': float(disc_xy[i, 0]), 'y': float(disc_xy[i, 1]),
            'radius': float(radii[i]), 'tightness': tight, 'canonical': canonical,
            'top_terms': lsa.top_terms(sub, tfidf.vocab, centers[c], top_n_terms),
        })
        for j, u in enumerate(idx):
            member_rows.append((disc_xy[i, 0] + off[j, 0], disc_xy[i, 1] + off[j, 1],
                                int(copies[u]), u_pos[u], c))
    members = pd.DataFrame(member_rows, columns=['x', 'y', 'copies', 'position', 'campaign_id'])
    others = pd.DataFrame({'x': xy_o[:, 0], 'y': xy_o[:, 1], 'copies': copies[placeable]})
    n_unplaced = int((~in_camp).sum() - len(placeable))
    return MapData(len(df), nU, sub.components.shape[0], layout, campaigns, members, others, n_unplaced)


# --------------------------------- figure ----------------------------------
def _short(text: str, words: int = 4) -> str:
    w = lsa.normalize_text(text).split()
    return ' '.join(w[:words]) + ('…' if len(w) > words else '')


def draw(md: MapData, out_base: Path, formats: List[str], label_min: int = 40) -> List[Path]:
    fig, ax = plt.subplots(figsize=(14, 10), layout='constrained', facecolor=BG)
    ax.set_facecolor(BG)
    ax.scatter(md.others['x'], md.others['y'], s=np.maximum(DOT_AREA * md.others['copies'], 4),
               c=UNASSIGNED, alpha=.55, linewidths=0, zorder=2)
    for c in sorted(md.campaigns, key=lambda c: -c['radius']):
        col = COLORS[c['position']]
        ax.add_patch(Circle((c['x'], c['y']), c['radius'], facecolor=col, alpha=.16,
                            edgecolor=col, linewidth=.8, zorder=3))
    m = md.members.sort_values('copies')
    ax.scatter(m['x'], m['y'], s=np.maximum(DOT_AREA * m['copies'], 5), c=[COLORS[p] for p in m['position']],
               alpha=.75, edgecolors=BG, linewidths=.3, zorder=4)

    # labels: one per letter family big enough to matter, at its largest campaign
    allx = np.concatenate([md.members['x'], md.others['x']])
    width = float(np.ptp(allx)) or 1.0
    fam_total = collections.Counter()
    for c in md.campaigns:
        if c['family_id'] is not None:
            fam_total[c['family_id']] += c['members']
    by_id = {c['campaign_id']: c for c in md.campaigns}
    placed = []

    def box(cx, cy, w, h):
        return (cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2)

    def hit(a, b):
        return not (a[2] < b[0] or b[2] < a[0] or a[3] < b[1] or b[3] < a[1])

    for fam_id, total in fam_total.most_common():
        if total < label_min:
            continue
        group = [c for c in md.campaigns if c['family_id'] == fam_id]
        big = max(group, key=lambda c: c['members'])
        rep = by_id.get(fam_id, big)
        txt = f"“{_short(rep['canonical'] or big['canonical'])}”  {total:,}"
        w, h = 0.0065 * width * len(txt), 0.024 * width
        spot = None
        for rad in (1.0, 1.5, 2.2, 3.0):
            for ang in (90, 270, 30, 150, 330, 210, 0, 180):
                tx = big['x'] + math.cos(math.radians(ang)) * (big['radius'] + 0.05 * width) * rad
                ty = big['y'] + math.sin(math.radians(ang)) * (big['radius'] + 0.03 * width) * rad
                b = box(tx, ty, w, h)
                if not any(hit(b, p) for p in placed):
                    spot = (tx, ty, b)
                    break
            if spot:
                break
        if spot is None:
            continue
        placed.append(spot[2])
        ax.plot([big['x'], spot[0]], [big['y'], spot[1]], color=INK2, lw=.6, zorder=5)
        ax.text(spot[0], spot[1], txt, ha='center', va='center', fontsize=8, color=INK, zorder=7,
                path_effects=[pe.withStroke(linewidth=2.5, foreground=BG)])

    present = [p for p in ('Oppose', 'Support', 'Other') if p in set(md.members['position'])]
    handles = [Line2D([], [], marker='o', ls='', color=COLORS[p], markersize=9, label=LABELS[p]) for p in present]
    handles.append(Line2D([], [], marker='o', ls='', color=UNASSIGNED, markersize=5, label='text not in any campaign'))
    leg = ax.legend(handles=handles, frameon=False, fontsize=8.5, loc='upper left',
                    title='Campaign members, by stance', title_fontsize=8.5, labelcolor=INK)
    leg.get_title().set_color(INK)
    ax.add_artist(leg)
    top = int(md.members['copies'].max())
    steps = sorted({1, min(10, top), top})
    keys = [Line2D([], [], marker='o', ls='', color='#9A9993', markersize=math.sqrt(DOT_AREA * c),
                   label=f"{c} cop{'y' if c == 1 else 'ies'}") for c in steps]
    leg2 = ax.legend(handles=keys, frameon=False, fontsize=8.5, loc='lower left', labelspacing=1.3,
                     title='Dot area = exact copies of one text', title_fontsize=8.5, labelcolor=INK)
    leg2.get_title().set_color(INK)

    ax.set_aspect('equal', adjustable='datalim')
    ax.set_xticks([])
    ax.set_yticks([])
    for s in ax.spines.values():
        s.set_visible(False)
    in_camp = sum(c['members'] for c in md.campaigns)
    note = (f"\n{md.n_unplaced:,} non-campaign texts the subspace kept too little of are not drawn."
            if md.n_unplaced else '')
    ax.set_title(
        f"{len(md.campaigns)} campaigns hold {in_camp:,} of {md.n_comments:,} comments ({in_camp / md.n_comments:.0%}). "
        f"Disc = one campaign (radius ~ members^(1/3)); dots = its distinct texts.\n"
        f"Farther from a disc's centre = more edited. Nearby = similar wording "
        f"(TF-IDF → {md.k}-dim SVD → {'UMAP (cosine)' if md.layout == 'umap' else 'PCA'} "
        f"of campaign centres and non-campaign texts).{note}",
        loc='left', fontsize=9.5, color=INK)
    written = []
    for fmt in formats:
        path = out_base.with_suffix('.' + fmt)
        fig.savefig(path, dpi=140 if fmt == 'png' else None, facecolor=BG)
        written.append(path)
    plt.close(fig)
    return written


def write_summary(md: MapData, path: Path) -> None:
    rows = [{'campaign_id': c['campaign_id'], 'family_id': c['family_id'], 'comments': c['members'],
             'distinct_texts': c['distinct_texts'], 'position': c['position'],
             'tightness': None if math.isnan(c['tightness']) else round(c['tightness'], 3),
             'top_terms': ' | '.join(t.replace('_', ' ') for t in c['top_terms']),
             'canonical_start': ' '.join(c['canonical'].split())[:120]}
            for c in sorted(md.campaigns, key=lambda c: -c['members'])]
    pd.DataFrame(rows).to_csv(path, index=False)


# ----------------------------------- CLI -----------------------------------
def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--regulation', required=True, help='slug, i.e. the folder name under regulations/')
    ap.add_argument('--regulation-dir', help='override regulations/<slug>')
    ap.add_argument('--parquet', help='analysed comments; default: <regulation dir>/full_run.parquet')
    ap.add_argument('--out-dir', help='where to write the files; default: the regulation dir')
    ap.add_argument('--k', type=int, default=100,
                    help='SVD dimensions (default 100). Too few and short comments are mostly noise; '
                         'the stderr summary reports how much of each text the subspace retained')
    ap.add_argument('--layout', choices=('umap', 'pca'), default='umap',
                    help='2-D layout (default umap; needs umap-learn from requirements-optional.txt)')
    ap.add_argument('--neighbors', type=int, default=15, help='UMAP n_neighbors (default 15)')
    ap.add_argument('--min-dist', type=float, default=0.3, help='UMAP min_dist (default 0.3)')
    ap.add_argument('--min-retained', type=float, default=0.05,
                    help='non-campaign texts whose retained norm is below this are not placed (default 0.05)')
    ap.add_argument('--spread', type=float, default=0.2,
                    help='how far members fan out from their campaign, as a fraction of map width (default 0.2)')
    ap.add_argument('--label-min', type=int, default=40, help='label letter families of at least this many comments')
    ap.add_argument('--top-terms', type=int, default=8, help='terms listed per campaign in the CSV (default 8)')
    ap.add_argument('--max-df', type=float, default=1.0,
                    help='drop terms in more than this share of distinct texts (default 1.0 = keep all; a '
                         'dominant campaign\'s own template words would otherwise be discarded)')
    ap.add_argument('--formats', default='png', help='comma list of png,svg,pdf (default png)')
    ap.add_argument('--seed', type=int, default=0)
    args = ap.parse_args(argv)

    root = Path(__file__).resolve().parent
    reg_dir = Path(args.regulation_dir) if args.regulation_dir else root / 'regulations' / args.regulation
    parquet = Path(args.parquet) if args.parquet else reg_dir / 'full_run.parquet'
    if not parquet.exists():
        sys.exit(f'{parquet} not found. Run pipeline.py for this regulation first, or pass --parquet.')
    out_dir = Path(args.out_dir) if args.out_dir else reg_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    formats = [f.strip().lower() for f in args.formats.split(',') if f.strip()]
    bad = [f for f in formats if f not in ('png', 'svg', 'pdf')]
    if bad:
        sys.exit(f'unsupported --formats {bad}; use png, svg or pdf')

    df = load_frame(parquet)
    md = build_map(df, k=args.k, layout=args.layout, neighbors=args.neighbors, min_dist=args.min_dist,
                   min_retained=args.min_retained, spread=args.spread, top_n_terms=args.top_terms,
                   max_df_frac=args.max_df, seed=args.seed)
    written = draw(md, out_dir / 'campaign_map', formats, label_min=args.label_min)
    csv_path = out_dir / 'campaign_map_campaigns.csv'
    write_summary(md, csv_path)
    in_camp = sum(c['members'] for c in md.campaigns)
    print(f'{md.n_comments:,} comments -> {md.n_distinct:,} distinct texts; {len(md.campaigns)} campaigns '
          f'hold {in_camp:,}; {md.n_unplaced:,} non-campaign texts not placed (retained norm < {args.min_retained}).')
    for p in written + [csv_path]:
        print('wrote', p)


if __name__ == '__main__':
    main()
