#!/usr/bin/env python3
"""Check whether comments labelled with a given position actually hold it.

The pipeline's second pass decides a stance and writes a rationale, and nothing
compares the two. That gap is how a comment asking OMB for a deadline extension
ended up labelled "Support the proposed rule" while its own rationale said the
commenter took no position on the substance. A reader found it before we did.

This audits a position in bulk, cheaply, and it is deliberately NOT the pipeline:
a separate prompt, judging the comment text against a written definition, so it
can disagree with the classifier rather than reproduce it. It only ever reports —
correcting is a second step (see --emit-ids and METHOD-stance-audit.md).

    python audit_stances.py --regulation omb-financial-assistance --position support
    python audit_stances.py --regulation omb-financial-assistance --position oppose --sample 2500

Definitions live in the regulation's analyzer_config.yaml under `stance_audit:`,
so a different docket audits against its own wording, not this file's.
"""
import argparse
import concurrent.futures as cf
import json
import os
import sys
import threading

import pandas as pd
import yaml
from pydantic import BaseModel, Field
from litellm import completion

# Fallback wording. A regulation that cares should put its own under
# `stance_audit:` in analyzer_config.yaml rather than rely on these.
DEFAULT_DEFINITIONS = {
    'support': (
        "SUPPORT means the comment endorses THIS RULE or specific provisions of it.\n"
        "It is NOT support when the comment merely wants less waste/fraud, more oversight or\n"
        "accountability without tying that to backing this rule; complains about how money has\n"
        "been misspent; asks to revise, soften or reconsider any part of the rule; defends peer\n"
        "review or scientific merit; or would read the same way had the rule never been proposed."
    ),
    'oppose': (
        "OPPOSE means the comment objects to this rule, and should be read generously: objecting\n"
        "to any provision, defending peer review or scientific independence, warning of harm to\n"
        "research or public health from this proposal, or general outrage at it.\n"
        "It is NOT oppose when the comment states no position, is procedural, only describes the\n"
        "rule, is off-topic, or in fact supports it."
    ),
}


class Verdict(BaseModel):
    holds_position: bool = Field(description="True if the comment genuinely holds the position "
                                             "it was labelled with, per the definition given.")
    reads_as: str = Field(description="The position the comment actually takes. Exactly one of: "
                                      "oppose, support, no_position, cannot_tell")


PROMPT = """A pipeline labelled this public comment as {POSITION} on a proposed rule.

Decide whether that label is right, using this definition:

{DEFINITION}

Answer holds_position=true only if the comment genuinely holds that position.

COMMENT:
{TEXT}"""


def build(reg_dir, position, sample, seed):
    cfg = yaml.safe_load(open(os.path.join(reg_dir, 'analyzer_config.yaml'))) or {}
    definition = (cfg.get('stance_audit') or {}).get(position) or DEFAULT_DEFINITIONS[position]

    df = pd.read_parquet(os.path.join(reg_dir, 'full_run.parquet'),
                         columns=['id', 'analysis', 'comment_text', 'attachment_text'])
    want = f'Position: {position.capitalize()}'
    rows = []
    for _, r in df.iterrows():
        a = r['analysis'] or {}
        s = a.get('stances')
        s = list(s) if s is not None and not isinstance(s, str) else (s or [])
        if not any(want in str(x) for x in s):
            continue
        # A body of "See attached file(s)" carries no signal; fall back to the extraction.
        t = str(r['comment_text'] or '').strip()
        if len(t) < 60:
            t = (t + ' || ATTACHMENT: ' + str(r['attachment_text'] or '')).strip()
        if len(t) > 40:
            rows.append((r['id'], ' '.join(t.split())[:1500]))

    print(f'{position}-labelled with usable text: {len(rows):,}')
    if sample and sample < len(rows):
        import random
        random.seed(seed)
        rows = random.sample(rows, sample)
        print(f'sampling {len(rows):,} (seed {seed})')
    return rows, definition


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--regulation', required=True)
    ap.add_argument('--position', required=True, choices=['support', 'oppose'])
    ap.add_argument('--sample', type=int, default=0, help='0 = audit all')
    ap.add_argument('--seed', type=int, default=7)
    ap.add_argument('--model', default='gpt-5.4-mini')
    ap.add_argument('--workers', type=int, default=16)
    ap.add_argument('--out', default=None)
    ap.add_argument('--emit-ids', default=None,
                    help='write the disputed ids as JSON, for a targeted re-verify')
    a = ap.parse_args()

    reg = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations', a.regulation)
    out = a.out or os.path.join(reg, f'stance_audit_{a.position}.jsonl')
    rows, definition = build(reg, a.position, a.sample, a.seed)

    # Resume: judging is the only cost, so never pay for the same comment twice.
    seen = set()
    if os.path.exists(out):
        for line in open(out):
            try:
                seen.add(json.loads(line)['id'])
            except Exception:
                pass
    rows = [r for r in rows if r[0] not in seen]
    print(f'to judge now: {len(rows):,}')

    lock, done = threading.Lock(), [0]

    def judge(row):
        cid, text = row
        try:
            resp = completion(model=a.model, timeout=90, response_format=Verdict,
                              messages=[{"role": "user", "content": PROMPT.format(
                                  POSITION=a.position.upper(), DEFINITION=definition, TEXT=text)}])
            v = json.loads(resp.choices[0].message.content)
            rec = {'id': cid, 'holds_position': bool(v.get('holds_position')),
                   'reads_as': v.get('reads_as', '?')}
        except Exception as e:
            rec = {'id': cid, 'error': type(e).__name__}
        with lock:
            done[0] += 1
            if done[0] % 500 == 0:
                print(f'  {done[0]:,} judged', flush=True)
            with open(out, 'a') as f:
                f.write(json.dumps(rec) + '\n')

    with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
        list(ex.map(judge, rows))

    verdicts = {}
    for line in open(out):
        try:
            r = json.loads(line)
            if 'error' not in r:
                verdicts[r['id']] = r
        except Exception:
            pass
    vals = list(verdicts.values())
    disputed = [r['id'] for r in vals if not r['holds_position']]
    rate = len(disputed) / len(vals) if vals else 0
    se = (rate * (1 - rate) / len(vals)) ** 0.5 if vals else 0
    print(f'\naudited {len(vals):,}')
    print(f'  disputed: {len(disputed):,} ({rate*100:.2f}%)  95% CI '
          f'{(rate-1.96*se)*100:.2f}–{(rate+1.96*se)*100:.2f}%')
    print(f'  -> {out}')
    if a.emit_ids:
        json.dump(sorted(disputed), open(a.emit_ids, 'w'))
        print(f'  disputed ids -> {a.emit_ids}')


if __name__ == '__main__':
    main()
