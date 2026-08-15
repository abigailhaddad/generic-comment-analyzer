#!/usr/bin/env python3
"""Draft CANDIDATE labels for an evaluation set, with enough reasoning to review by hand.

These are model output, not ground truth. The point is to make human review cheap
and *checkable*: for every comment the model must quote the words it relied on,
argue its own call, argue the strongest case against it, and say what a reviewer
should look at. A label with no visible reasoning is not reviewable — you would
just be agreeing with a black box, which is how the original mislabelling survived.

A deliberately stronger model than the pipeline uses (`eval.model` in the config,
e.g. gpt-5.4 against the pipeline's nano/mini), so the reference standard is not
the thing being measured.

Everything docket-specific — model, sample size, extra guidance, and the position
definitions themselves — comes from the regulation's analyzer_config.yaml. This
file contains no rule-specific text.

    python label_eval_set.py --regulation <slug>
    python label_eval_set.py --regulation <slug> --model o3        # override
"""
import argparse
import concurrent.futures as cf
import json
import os
import threading

import pandas as pd
import yaml
from pydantic import BaseModel, Field, create_model
from litellm import completion

NO_POSITION = 'no_position'
JUNK = 'unclear_junk'

# Used when the regulation's config has no `stance_audit.no_position`. Named so
# score_stances can display exactly the words the prompt used.
FALLBACK_NO_POSITION = (
    'engages the subject but takes no side: procedural (e.g. asking to extend the '
    'comment period), only describes the proposal, asks a question, or argues for '
    'something adjacent without endorsing or objecting to it.')


def build_prompt(reg):
    """Assemble the labelling prompt from the regulation's own config."""
    cfg = yaml.safe_load(open(os.path.join(reg, 'analyzer_config.yaml'))) or {}
    desc = (cfg.get('regulation_description') or cfg.get('regulation_name') or '').strip()
    audit = cfg.get('stance_audit') or {}
    notes = ((cfg.get('eval') or {}).get('labeling_notes') or '').strip()

    lines, slugs = [], []
    for s in cfg.get('stances') or []:
        name = s.get('name', '')
        if not name.startswith('Position:'):
            continue
        slug = name.split(':', 1)[1].strip().split()[0].lower()
        slugs.append(slug)
        # prefer the audit definition — written to be stricter than the classifier's
        text = (audit.get(slug) or s.get('indicator') or '').strip()
        lines.append(f'- "{slug}" — {" ".join(text.split())}')

    # The regulation's own wording wins; these are only a fallback for a config
    # that has not written one.
    np_def = (audit.get(NO_POSITION) or FALLBACK_NO_POSITION).strip()
    lines.append(f'- "{NO_POSITION}" — {" ".join(np_def.split())}')
    lines.append(f'- "{JUNK}" — off-topic, incoherent, a single word, or no substantive content.')
    slugs += [NO_POSITION, JUNK]

    prompt = (
        "You are building a gold-standard evaluation set. A human will review every one of your\n"
        "labels, so show your work: quote the words you relied on and argue both sides.\n\n"
        f"THE RULE: {' '.join(desc.split())}\n\n"
        "Choose exactly one label:\n\n" + "\n".join(lines) +
        (f"\n\nADDITIONAL GUIDANCE:\n{' '.join(notes.split())}" if notes else "") +
        "\n\nFor every comment give:\n"
        "  label            — your call\n"
        "  evidence         — the VERBATIM words you relied on (or \"\" if there are none)\n"
        "  case_for         — why that label, in 1-2 sentences\n"
        "  runner_up        — the next most defensible label\n"
        "  case_against     — the strongest argument for the runner_up, stated fairly\n"
        "  what_makes_it_hard — what a careful reader could trip on, or \"\" if it is clear-cut\n"
        "  reviewer_should_check — the specific question a human should answer to settle it,\n"
        "                     or \"\" if it is clear-cut\n\n"
        "COMMENT:\n{TEXT}")
    return prompt, slugs, cfg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--regulation', required=True)
    ap.add_argument('--out-dir', default='eval')
    ap.add_argument('--model', default=None, help='overrides eval.model in the config')
    ap.add_argument('--workers', type=int, default=12)
    a = ap.parse_args()

    reg = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations', a.regulation)
    ev = os.path.join(reg, a.out_dir)
    src, out = os.path.join(ev, 'candidates.csv'), os.path.join(ev, 'candidates_labeled.jsonl')

    prompt, slugs, cfg = build_prompt(reg)
    model = a.model or (cfg.get('eval') or {}).get('model') or 'gpt-5.4'
    print(f'model: {model}   labels: {", ".join(slugs)}')

    Label = create_model(
        'Label',
        label=(str, Field(description='Exactly one of: ' + ', '.join(slugs))),
        evidence=(str, Field(description='Verbatim words from the comment, or empty string.')),
        case_for=(str, Field(description='Why this label. 1-2 sentences.')),
        runner_up=(str, Field(description='Next most defensible label, from the same list.')),
        case_against=(str, Field(description='Strongest fair argument for runner_up.')),
        what_makes_it_hard=(str, Field(description='What a careful reader could trip on, or empty.')),
        reviewer_should_check=(str, Field(description='Specific question for a human, or empty.')),
        __base__=BaseModel,
    )

    d = pd.read_csv(src)
    seen = set()
    if os.path.exists(out):
        for line in open(out):
            try:
                seen.add(json.loads(line)['id'])
            except Exception:
                pass
    # No truncation: the reference labeller must see everything the classifier saw.
    rows = [(r['id'], str(r['text'])) for _, r in d.iterrows() if r['id'] not in seen]
    print(f'drafting {len(rows)} of {len(d)}')

    lock, n = threading.Lock(), [0]

    def go(row):
        cid, text = row
        try:
            r = completion(model=model, timeout=180, response_format=Label,
                           messages=[{'role': 'user', 'content': prompt.replace('{TEXT}', text)}])
            rec = {'id': cid, **json.loads(r.choices[0].message.content)}
        except Exception as e:
            rec = {'id': cid, 'error': f'{type(e).__name__}: {str(e)[:80]}'}
        with lock:
            n[0] += 1
            if n[0] % 25 == 0:
                print(f'  {n[0]} drafted', flush=True)
            with open(out, 'a') as f:
                f.write(json.dumps(rec) + '\n')

    with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
        list(ex.map(go, rows))

    drafts = {}
    for line in open(out):
        try:
            r = json.loads(line)
            if 'error' not in r:
                drafts[r['id']] = r
        except Exception:
            pass

    fields = ['label', 'evidence', 'case_for', 'runner_up', 'case_against',
              'what_makes_it_hard', 'reviewer_should_check']
    for f in fields:
        d[f if f != 'label' else 'draft_label'] = d['id'].map(lambda i: drafts.get(i, {}).get(f, ''))

    def norm(p):
        return NO_POSITION if p == '(none)' else str(p).split(':', 1)[-1].strip().split()[0].lower()

    d['pipeline_norm'] = d['pipeline_label'].map(norm)
    d['agrees'] = d['pipeline_norm'] == d['draft_label']
    # Sort the ones worth a human's attention to the top: where the draft and the
    # pipeline disagree, or where the model itself flagged something to check.
    d['review_first'] = (~d['agrees']) \
        | d['what_makes_it_hard'].astype(str).str.len().gt(0) \
        | d['reviewer_should_check'].astype(str).str.len().gt(0)
    # `label` stays the human's column: pre-filled with the draft, theirs to change
    d['label'] = d['draft_label']
    front = ['id', 'label', 'draft_label', 'pipeline_norm', 'agrees',
             'evidence', 'case_for', 'runner_up', 'case_against', 'what_makes_it_hard',
             'reviewer_should_check', 'note', 'weight', 'url', 'text']
    d = d[front + [c for c in d.columns if c not in front]]
    d = d.sort_values(['review_first', 'agrees', 'pipeline_norm'], ascending=[False, True, True])
    d.to_csv(src, index=False)

    print(f'\n  drafted {len(drafts)} of {len(d)}   -> {src}')
    print(f'  disagrees with pipeline : {int((~d.agrees).sum())}')
    print(f'  model flagged as tricky : {int(d.what_makes_it_hard.astype(str).str.len().gt(0).sum())}')
    print(f'  -> REVIEW THESE FIRST   : {int(d.review_first.sum())} (sorted to the top)')
    print('\n  edit the `label` column, then: score_stances.py --regulation ' + a.regulation)


if __name__ == '__main__':
    main()
