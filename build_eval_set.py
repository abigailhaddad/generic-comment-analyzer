#!/usr/bin/env python3
"""Build a stratified evaluation set for position labels — any regulation.

Proportional sampling is useless on a docket with a lopsided split. This one is
~96% Oppose, so 200 random comments give ~192 Oppose and the metric that actually
broke (Support precision) would rest on six rows. Allocation defaults to BALANCED
— as near equal per position as availability allows — because per-class
precision/recall is the point of this set. The docket-wide rate stays recoverable
by weighting back with the `weight` column.

Nothing about the positions is hardcoded — they are read from whatever
`Position:` stances the regulation's config defines, plus a bucket for comments
carrying none. A new docket with different positions works unchanged.

    python build_eval_set.py --regulation <slug>
    python build_eval_set.py --regulation <slug> --size 300 --floor 25
"""
import argparse
import math
import os
import random

import pandas as pd
import yaml

NO_POSITION = '(none)'


def positions_from_config(reg):
    """Position labels this regulation defines, in config order."""
    cfg = yaml.safe_load(open(os.path.join(reg, 'analyzer_config.yaml'))) or {}
    out = []
    for s in cfg.get('stances') or []:
        name = s.get('name', '')
        if name.startswith('Position:'):
            out.append(name)
    return out, cfg


def position_of(analysis, known):
    s = (analysis or {}).get('stances')
    s = list(s) if s is not None and not isinstance(s, str) else (s or [])
    hit = [x for x in s if str(x) in known]
    return str(hit[0]) if hit else NO_POSITION


def body(r):
    """Exactly what the pipeline analysed — see `full_text` in pipeline.py.

    It ALWAYS appends the attachment, so this must too. An earlier version only
    fell back to the attachment when the comment body was under 60 chars, which
    handed the labeller a 110-char letterhead for a comment whose whole argument
    was in the attached letter. The tool had read the letter and was right; the
    eval scored it as an error, and that single row (weight 2,430) moved the
    estimated docket split by more than a point.
    """
    t = str(r['comment_text'] or '').strip()
    att = str(r['attachment_text'] or '').strip()
    if att:
        t = f'{t}\n\n--- ATTACHMENT CONTENT ---\n{att}' if t else att
    return ' '.join(t.split())


def allocate(counts, size, floor, strategy):
    """How many to sample per position.

    'balanced' (default) — as close to equal per position as availability allows.
    Per-class precision/recall is the point of this set, and a class with 29 rows
    cannot support a precision estimate. The `weight` column carries the sampling
    rate so docket-wide rates are still recoverable.

    'sqrt' — proportional to sqrt(n). Closer to the real mix, but on a lopsided
    docket it starves the rare classes.
    """
    keys = [k for k, v in counts.items() if v > 0]
    if not keys:
        return {}
    if strategy == 'sqrt':
        w = {k: math.sqrt(counts[k]) for k in keys}
    else:
        w = {k: 1.0 for k in keys}
    tot = sum(w.values()) or 1
    out = {k: min(counts[k], max(min(floor, counts[k]), round(size * w[k] / tot))) for k in keys}
    # redistribute whatever small classes could not absorb
    for _ in range(1000):
        cur = sum(out.values())
        if cur == size:
            break
        if cur > size:
            k = max(out, key=lambda x: out[x] - min(floor, counts[x]))
            if out[k] <= min(floor, counts[k]):
                break
            out[k] -= 1
        else:
            room = {k: counts[k] - out[k] for k in keys if counts[k] > out[k]}
            if not room:
                break
            out[max(room, key=room.get)] += 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--regulation', required=True)
    ap.add_argument('--size', type=int, default=200)
    ap.add_argument('--floor', type=int, default=20, help='minimum per position, if available')
    ap.add_argument('--out-dir', default='eval')
    ap.add_argument('--strategy', choices=['balanced','sqrt'], default='balanced')
    ap.add_argument('--max-chars', type=int, default=0,
                    help='0 = no limit. Never set this below pipeline.py --truncate.')
    ap.add_argument('--seed', type=int, default=20260815)
    a = ap.parse_args()

    reg = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations', a.regulation)
    out = os.path.join(reg, a.out_dir)
    os.makedirs(out, exist_ok=True)
    known, _ = positions_from_config(reg)
    print(f'positions defined by this regulation: {len(known)}')
    for k in known:
        print(f'   {k}')

    cols = ['id', 'analysis', 'comment_text', 'attachment_text', 'organization', 'campaign_id']
    d = pd.read_parquet(os.path.join(reg, 'full_run.parquet'), columns=cols)
    d['pos'] = d['analysis'].apply(lambda x: position_of(x, set(known)))
    d['text'] = d.apply(body, axis=1)
    d = d[d['text'].str.len() > 40].copy()

    # Sample RANDOMLY within each position. An earlier version stratified within
    # the class across short/long, org/individual and campaign/unique to guarantee
    # coverage — but that over-samples short comments, which is where errors
    # concentrate, and the per-row weight only captures BETWEEN-class sampling.
    # The result inflated the Oppose error rate ~17x against a properly sampled
    # audit. Random within class keeps the weights honest; the cell flags are still
    # recorded so coverage can be checked after the fact.
    cutoff = int(d['text'].str.len().median() / 2) or 180
    d['is_short'] = d['text'].str.len() < cutoff
    d['is_org'] = d['organization'].fillna('').str.strip().ne('')
    d['in_campaign'] = d['campaign_id'].notna()

    counts = d['pos'].value_counts().to_dict()
    plan = allocate(counts, a.size, a.floor, a.strategy)
    print(f'\nallocation [{a.strategy}] (random within each position):')
    picked, weights = [], {}
    for pos, n in plan.items():
        pool = d[d.pos == pos]
        weights[pos] = counts[pos] / n if n else 0
        picked += list(pool.sample(n, random_state=a.seed)['id'])
        print(f'   {pos:42} {len(pool.sample(n, random_state=a.seed)):4} of {counts[pos]:7,}   weight {weights[pos]:.1f}')

    sub = d[d.id.isin(picked)].copy()
    sub['weight'] = sub['pos'].map(weights).round(2)
    sub['url'] = 'https://www.regulations.gov/comment/' + sub['id']
    sub['label'] = ''
    sub['note'] = ''
    sub = sub.rename(columns={'pos': 'pipeline_label'})
    # Do NOT cut the text. The reference labeller must never see less than the
    # classifier did (pipeline.py analyses up to --truncate, default 50,000 chars).
    # At an earlier 1,200 a "See attached file(s)" comment showed only its
    # letterhead; the labeller correctly called it position-less and that was
    # scored as a classifier error. --max-chars 0 means no limit.
    if a.max_chars:
        sub['text'] = sub['text'].str.slice(0, a.max_chars)
    keep = ['id', 'pipeline_label', 'label', 'note', 'weight', 'url',
            'is_short', 'is_org', 'in_campaign', 'organization', 'text']
    path = os.path.join(out, 'candidates.csv')
    sub[keep].sort_values(['pipeline_label', 'id']).to_csv(path, index=False)
    print(f'\n{len(sub)} comments -> {path}')
    print('next: label_eval_set.py to draft labels, then review the `label` column')


if __name__ == '__main__':
    main()
