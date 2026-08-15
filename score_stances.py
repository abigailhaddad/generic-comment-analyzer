#!/usr/bin/env python3
"""Score the classifier's positions against the reviewed evaluation set.

Reports per-class precision and recall, plus a confusion matrix, and writes the data
behind the public accuracy page. Per-class is the point: the defect that prompted all this
was Support precision, which a single overall accuracy number would have hidden
behind a 96%-Oppose docket.

Two things the arithmetic has to respect:

  * The eval set separates "took no position" from "unreadable junk". The
    pipeline cannot — both surface as Unclear — so they are folded together
    before scoring. Scoring the classifier on a distinction it was never asked
    to make would invent errors.

  * The sample is stratified, not proportional, so raw counts are not docket
    rates. Each row carries the sampling `weight` (population / sampled), and
    weighted figures are what get published.

    python score_stances.py --regulation <slug>
    python score_stances.py --regulation <slug> --json eval/scores.json
"""
import argparse
import html
import os
from collections import defaultdict

import pandas as pd
import yaml

# What the pipeline can actually express. Anything the eval calls junk is, from
# the classifier's point of view, the same answer as "no position".
FOLD = {'unclear_junk': 'no_position'}
PRETTY = {'oppose': 'Oppose', 'support': 'Support', 'no_position': 'No position / unclear'}


def load(reg, out_dir):
    p = os.path.join(reg, out_dir, 'candidates.csv')
    d = pd.read_csv(p)
    if 'label' not in d.columns:
        raise SystemExit(f'{p} has no `label` column — run label_eval_set.py first')
    d = d[d['label'].astype(str).str.strip().ne('')].copy()
    if d.empty:
        raise SystemExit('no labelled rows yet')
    d['truth'] = d['label'].astype(str).str.strip().map(lambda x: FOLD.get(x, x))
    d['pred'] = d['pipeline_norm'].astype(str).str.strip().map(lambda x: FOLD.get(x, x))
    d['weight'] = pd.to_numeric(d.get('weight', 1), errors='coerce').fillna(1.0)
    return d, p


def metrics(d, weighted=True):
    w = d['weight'] if weighted else pd.Series(1.0, index=d.index)
    classes = sorted(set(d['truth']) | set(d['pred']))
    tp, fp, fn = defaultdict(float), defaultdict(float), defaultdict(float)
    for c in classes:
        tp[c] = float(w[(d.pred == c) & (d.truth == c)].sum())
        fp[c] = float(w[(d.pred == c) & (d.truth != c)].sum())
        fn[c] = float(w[(d.pred != c) & (d.truth == c)].sum())
    rows = []
    for c in classes:
        prec = tp[c] / (tp[c] + fp[c]) if tp[c] + fp[c] else float('nan')
        rec = tp[c] / (tp[c] + fn[c]) if tp[c] + fn[c] else float('nan')
        f1 = 2 * prec * rec / (prec + rec) if prec == prec and rec == rec and prec + rec else float('nan')
        rows.append({'class': c, 'precision': prec, 'recall': rec, 'f1': f1,
                     'n_labelled': int(((d.truth == c)).sum())})
    return pd.DataFrame(rows)


def prevalence(d, pop_counts):
    """Estimate the docket's TRUE position mix from the labelled sample.

    The classifier's own output is a biased estimate — it is exactly what the
    labels are measuring. Because the sample is drawn at random WITHIN each
    predicted class, each stratum gives an unbiased read of what those comments
    really are, and the strata reweight to the population:

        est(true = c) = SUM over predicted classes k of  N_k * P(truth = c | pred = k)

    with the usual stratified variance. This is why within-class sampling has to
    stay random: stratify inside a stratum and P(truth|pred) is no longer what
    the population looks like.
    """
    classes = sorted(set(d['truth']) | set(d['pred']))
    total = sum(pop_counts.values())
    est, var = {c: 0.0 for c in classes}, {c: 0.0 for c in classes}
    for k in sorted(set(d['pred'])):
        cell = d[d.pred == k]
        n_k = len(cell)
        N_k = pop_counts.get(k, 0)
        if not n_k or not N_k:
            continue
        for c in classes:
            p = float((cell.truth == c).mean())
            est[c] += N_k * p
            if n_k > 1:
                var[c] += (N_k ** 2) * p * (1 - p) / n_k
    rows = []
    for c in classes:
        se = var[c] ** 0.5
        rows.append({'class': c,
                     'tool_says': pop_counts.get(c, 0),
                     'tool_pct': pop_counts.get(c, 0) / total * 100 if total else 0,
                     'estimated_true': est[c],
                     'est_pct': est[c] / total * 100 if total else 0,
                     'ci_lo_pct': max(0.0, (est[c] - 1.96 * se)) / total * 100 if total else 0,
                     'ci_hi_pct': min(total, (est[c] + 1.96 * se)) / total * 100 if total else 0})
    return pd.DataFrame(rows)


def prompt_and_notes(reg):
    """The verbatim labelling prompt, from the code that builds it.

    Imported from label_eval_set rather than re-assembled here — a page that
    publishes a prompt slightly different from the one that ran is worse than
    publishing none. The position definitions are inside this prompt, which is
    why the page shows the prompt itself rather than a paraphrase of it.
    """
    import label_eval_set
    prompt, _slugs, cfg = label_eval_set.build_prompt(reg)
    return prompt, (cfg.get('eval') or {}).get('labeling_notes', '').strip()


def reviewed_mask(d):
    """Rows a person actually checked.

    `label` is pre-filled with the model's draft, so a row matching the draft
    proves nothing — an explicit `reviewed` column is the only honest signal.
    """
    if 'reviewed' not in d.columns:
        return pd.Series(False, index=d.index)
    seen = d['reviewed'].astype(str).str.strip().str.lower()
    return seen.isin(['1', 'x', 'y', 'yes', 'true', 't', 'done'])


def pipeline_stages(reg, eval_model, eval_prompt, n_labelled, n_reviewed):
    """The stages a comment passes through, with each stage's real prompt.

    Every value is read from the regulation's own config or built by the same
    code the pipeline runs, so a docket with different prompts, models or
    triggers publishes its own — nothing here is written for one rule.
    """
    import comment_analyzer
    cfg = yaml.safe_load(open(os.path.join(reg, 'analyzer_config.yaml'))) or {}
    sp = cfg.get('second_pass') or {}

    # First pass: ask the analyzer itself for the prompt, rather than
    # reconstructing it — a published prompt that differs from the one that ran
    # is worse than no prompt at all.
    cwd = os.getcwd()
    try:
        os.chdir(reg)
        first_prompt = comment_analyzer.CommentAnalyzer().get_system_prompt()
    finally:
        os.chdir(cwd)

    try:
        used = pd.read_parquet(os.path.join(reg, 'full_run.parquet'), columns=['model_used'])
        first_model = used['model_used'].dropna().mode()
        first_model = str(first_model.iloc[0]) if len(first_model) else ''
    except Exception:
        first_model = ''

    trig = (sp.get('stance') or {}).get('trigger_stances') or []
    if trig:
        when2 = 'Comments the first pass called ' + ' or '.join(str(t) for t in trig) + '.'
    else:
        when2 = 'Comments flagged by the first pass.'

    stages = [{
        'name': 'First pass',
        'model': first_model,
        'when': 'Every comment on the docket.',
        'what': 'Reads the comment and its attachments, then assigns a position and the concerns it raises.',
        'prompt': first_prompt,
    }]
    if (sp.get('prompts') or {}).get('stance'):
        stages.append({
            'name': 'Second pass',
            'model': sp.get('model', ''),
            'when': when2,
            'what': 'A stronger model re-reads them and can overturn the first pass.',
            'prompt': sp['prompts']['stance'],
        })
    stages.append({
        'name': 'Reference labels',
        'model': eval_model,
        'when': f'{n_labelled} sampled comments.',
        'what': 'Labels the sample from scratch, without seeing what the pipeline said.',
        'prompt': eval_prompt,
    })
    # Prompt changes, matched to the stage they hit. The prompts published above are
    # the current ones; this is what says they were not always current.
    for st in stages:
        st['history'] = [{'date': h.get('date', ''), 'note': ' '.join(str(h.get('note', '')).split())}
                         for h in (cfg.get('prompt_history') or [])
                         if str(h.get('stage', '')).strip().lower() == st['name'].strip().lower()]

    stages.append({
        'name': 'Us',
        'model': '',
        'when': (f'{n_reviewed} of {n_labelled} — every comment where the two disagreed.'
                 if n_reviewed else f'None of the {n_labelled} yet.'),
        'history': [],
        'what': ('We read those and made the call. The scores on this page use our call, not '
                 'the model\'s.'
                 if n_reviewed else
                 'Not started. The reference labels stand as the model drafted them.'),
        'prompt': '',
    })
    return stages


def unreadable_count(reg):
    """Comments that point at an attachment we could not read.

    They reach the classifier as little more than "See attached file(s)", so
    there is nothing in them to take a position on and they land in the
    no-position bucket. Worth saying out loud rather than letting a reader
    assume the label means the commenter had no view.
    """
    f = pd.read_parquet(os.path.join(reg, 'full_run.parquet'),
                        columns=['comment_text', 'attachment_text'])
    body = f['comment_text'].astype(str)
    points = body.str.contains(r'attach|enclosed', case=False, na=False) & (body.str.len() < 400)
    empty = f['attachment_text'].astype(str).str.strip().isin(['', 'None', 'nan'])
    return int((points & empty).sum())


def dump(d, m_w, m_u, prev, pop_counts, model, prompt='', notes='', stages=None, unreadable=0):
    """Everything the accuracy page needs, as data. generate_report.py owns the
    rendering, the same way rule_sections.json feeds read-the-rule.html — so the
    page picks up the site's palette and never drifts from the report's styling.
    """
    conf = pd.crosstab(d['truth'], d['pred'])
    classes = sorted(set(d['truth']) | set(d['pred']))

    # Reviewed-by-hand count. `label` is pre-filled with the model's draft, so a
    # row that merely matches the draft proves nothing; an explicit `reviewed`
    # column is the only honest signal. No column = nothing reviewed yet.
    reviewed = reviewed_mask(d)

    def txt(v):
        # pandas NaN stringifies to the literal "nan", which would render as text
        return '' if v is None or v != v else str(v).strip()

    rows = []
    for _, r in d.sort_values(['pred', 'truth']).iterrows():
        rows.append({
            'id': str(r['id']),
            'url': str(r.get('url', '')),
            'tool': str(r['pred']),
            'eval': str(r['truth']),
            'eval_raw': txt(r.get('label')),
            'match': bool(r['truth'] == r['pred']),
            'reviewed': bool(reviewed.get(r.name, False)),
            'weight': float(r['weight']),
            'organization': txt(r.get('organization')),
            'evidence': txt(r.get('evidence')),
            'case_for': txt(r.get('case_for')),
            'runner_up': txt(r.get('runner_up')),
            'case_against': txt(r.get('case_against')),
            'what_makes_it_hard': txt(r.get('what_makes_it_hard')),
            'reviewer_should_check': txt(r.get('reviewer_should_check')),
            'text': txt(r.get('text'))[:1500],
        })

    def recs(m):
        return {r['class']: {k: (None if r[k] != r[k] else round(float(r[k]), 4))
                             for k in ('precision', 'recall', 'f1')}
                | {'n_labelled': int(r['n_labelled'])}
                for _, r in m.iterrows()}

    return {
        'unreadable': unreadable,
        'stages': stages or [],
        'prompt': prompt,
        'labeling_notes': notes,
        'n_labelled': int(len(d)),
        'n_reviewed': int(reviewed.sum()),
        'model': model,
        'agreement': round(float((d.truth == d.pred).mean()), 4),
        'classes': classes,
        'pretty': PRETTY,
        'weighted': recs(m_w),
        'unweighted': recs(m_u),
        'prevalence': [{k: (round(float(v), 4) if isinstance(v, float) else v)
                        for k, v in r.items()} for r in prev.to_dict('records')],
        'confusion': {str(t): {str(c): int(conf.loc[t, c]) for c in conf.columns}
                      for t in conf.index},
        'population_total': int(sum(pop_counts.values())),
        'rows': rows,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--regulation', required=True)
    ap.add_argument('--out-dir', default='eval')
    ap.add_argument('--json', default='eval/scores.json',
                    help='where to write the data the accuracy page renders from')
    a = ap.parse_args()

    reg = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations', a.regulation)
    d, src = load(reg, a.out_dir)
    m_w, m_u = metrics(d, True), metrics(d, False)

    # population counts per predicted class, straight from the parquet
    import numpy as np
    full = pd.read_parquet(os.path.join(reg, 'full_run.parquet'), columns=['analysis'])
    def pos_of(x):
        st = (x or {}).get('stances')
        st = list(st) if st is not None and not isinstance(st, str) else (st or [])
        p = [y for y in st if str(y).startswith('Position:')]
        if not p:
            return 'no_position'
        return 'support' if 'Support' in str(p[0]) else 'oppose'
    pop_counts = full['analysis'].apply(pos_of).value_counts().to_dict()
    prev = prevalence(d, pop_counts)

    print(f'scored {len(d)} labelled comments from {src}\n')
    print('WEIGHTED (reflects the docket):')
    print(m_w.to_string(index=False, float_format=lambda v: f'{v:.3f}'))
    print('\nUNWEIGHTED (the sample as drawn):')
    print(m_u.to_string(index=False, float_format=lambda v: f'{v:.3f}'))
    print('\nESTIMATED TRUE SPLIT (tool output corrected by the reviewed sample):')
    print(prev.to_string(index=False, float_format=lambda v: f'{v:,.2f}'))
    print('\nconfusion (rows = reviewed truth, cols = tool):')
    print(pd.crosstab(d['truth'], d['pred']).to_string())
    agree = (d.truth == d.pred).mean()
    print(f'\noverall agreement on the sample: {agree:.1%}')

    if a.json:
        import json
        import yaml
        cfg = yaml.safe_load(open(os.path.join(reg, 'analyzer_config.yaml'))) or {}
        model = (cfg.get('eval') or {}).get('model', '')
        out = a.json if os.path.isabs(a.json) else os.path.join(reg, a.json)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        prompt, notes = prompt_and_notes(reg)
        stages = pipeline_stages(reg, model, prompt, len(d), int(reviewed_mask(d).sum()))
        with open(out, 'w') as f:
            json.dump(dump(d, m_w, m_u, prev, pop_counts, model, prompt, notes, stages,
                           unreadable_count(reg)),
                      f, indent=1)
        print(f'\naccuracy data -> {out}')
        print('   render it: generate_report.py (picks it up automatically)')


if __name__ == '__main__':
    main()
