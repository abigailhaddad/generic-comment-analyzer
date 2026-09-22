#!/usr/bin/env python3
"""Check a named_campaigns roster against the corpus: what's a close call?

A roster (analyzer_config.yaml: named_campaigns) says a comment is IN a
campaign or it isn't — binary, no matter how close the text is to the
boundary. This measures every comment in the corpus against each named
campaign's own text, using the exact same 5-gram MinHash similarity
pipeline.py's organic campaign detector uses (campaign_similarity.py), and
surfaces the two things worth a human's eyes:

  - non-members whose text is close to the campaign's text (candidates the
    roster might be MISSING — a false negative)
  - members whose text is LEAST similar to the rest of the roster
    (candidates that might not actually belong — a false positive)

Nothing here is an LLM call — it's the same cheap text-similarity computation
pipeline.py already runs for organic campaign detection, just measured against
a roster instead of building clusters from scratch.

Writes campaign_audit.json, which generate_report.py renders as a discrete
campaign-audit.html page (not part of the main report; linked quietly from
the Known Campaigns section) whenever that file is present.

    python audit_named_campaigns.py --regulation <slug>
    python audit_named_campaigns.py --regulation <slug> --near-floor 0.2 --top-n 25
"""
import argparse
import json
import os
from collections import Counter

import pandas as pd
import yaml

from campaign_similarity import make_campaign_minhash


def load_roster(reg_dir):
    cfg = yaml.safe_load(open(os.path.join(reg_dir, 'analyzer_config.yaml'))) or {}
    entries = cfg.get('named_campaigns') or []
    roster = {}
    for e in entries:
        if isinstance(e, dict) and e.get('name'):
            roster[e['name']] = [str(i) for i in (e.get('ids') or [])]
    return roster


def full_text(row):
    """Same "what got clustered" text as pipeline.py: body, then attachment."""
    body = (row.get('comment_text') or '').strip()
    att = (row.get('attachment_text') or '').strip()
    return (body + '\n' + att).strip() if att else body


def audit_campaign(name, ids, comments_by_id, all_rows, near_floor, top_n):
    member_ids = set(ids)
    matched = [comments_by_id[i] for i in ids if i in comments_by_id]
    missing_ids = [i for i in ids if i not in comments_by_id]

    if not matched:
        return {'name': name, 'roster_size': len(ids), 'matched': 0,
                'missing_ids': missing_ids, 'error': 'no roster ids found in this corpus'}

    # The roster's own most-common text stands in for "what this campaign says"
    # — same idea as the organic campaign's canonical text in generate_report.py.
    text_counts = Counter(full_text(c) for c in matched)
    canonical = text_counts.most_common(1)[0][0]
    canon_mh = make_campaign_minhash(canonical)
    if canon_mh is None:
        return {'name': name, 'roster_size': len(ids), 'matched': len(matched),
                'missing_ids': missing_ids,
                'error': 'canonical text too short to compare (fewer than 5 words)'}

    sims = {}
    for row in all_rows:
        mh = make_campaign_minhash(full_text(row))
        sims[row['id']] = canon_mh.jaccard(mh) if mh else 0.0

    lowest_members = sorted(
        ({'id': c['id'], 'sim': round(sims.get(c['id'], 0.0), 3), 'snippet': full_text(c)[:220]}
         for c in matched),
        key=lambda r: r['sim']
    )
    closest_non_members = sorted(
        ({'id': r['id'], 'sim': round(sims.get(r['id'], 0.0), 3), 'snippet': full_text(r)[:220]}
         for r in all_rows if r['id'] not in member_ids and sims.get(r['id'], 0.0) >= near_floor),
        key=lambda r: -r['sim']
    )

    sim_values = sorted(m['sim'] for m in lowest_members)
    return {
        'name': name,
        'roster_size': len(ids),
        'matched': len(matched),
        'missing_ids': missing_ids,
        'canonical': canonical[:500],
        'member_sim_min': sim_values[0] if sim_values else None,
        'member_sim_median': sim_values[len(sim_values) // 2] if sim_values else None,
        'lowest_similarity_members': lowest_members[:top_n],
        'closest_non_members': closest_non_members[:top_n],
        'closest_non_members_total': len(closest_non_members),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--regulation', required=True)
    ap.add_argument('--near-floor', type=float, default=0.10,
                     help='minimum similarity for a non-member to be listed as a close call (default 0.10)')
    ap.add_argument('--top-n', type=int, default=40,
                     help='rows kept on each side per campaign (default 40)')
    args = ap.parse_args()

    reg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations', args.regulation)
    roster = load_roster(reg_dir)
    if not roster:
        print("No named_campaigns in this regulation's config — nothing to audit.")
        return

    cols = ['id', 'comment_text', 'attachment_text']
    df = pd.read_parquet(os.path.join(reg_dir, 'full_run.parquet'), columns=cols)
    all_rows = df.to_dict('records')
    comments_by_id = {r['id']: r for r in all_rows}

    print(f'Auditing {len(roster)} named campaign(s) against {len(all_rows):,} comments...')
    results = []
    for name, ids in roster.items():
        print(f'  {name}: {len(ids)} listed id(s)')
        result = audit_campaign(name, ids, comments_by_id, all_rows, args.near_floor, args.top_n)
        results.append(result)
        if 'error' in result:
            print(f'    WARNING: {result["error"]}')
        else:
            print(f'    matched {result["matched"]}/{result["roster_size"]} — '
                  f'{result["closest_non_members_total"]} close-call candidate(s) to add, '
                  f'member similarity min={result["member_sim_min"]}')

    out_path = os.path.join(reg_dir, 'campaign_audit.json')
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f'Wrote {out_path}')


if __name__ == '__main__':
    main()
