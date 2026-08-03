#!/usr/bin/env python3
"""Pull every figure used in the OMB write-up straight from the parquet.

Reuses generate_report.py's own aggregation functions, so the numbers here are
guaranteed identical to the dashboard. Re-run after the pipeline updates to get
fresh figures for the doc.

    python report_numbers.py --regulation omb-financial-assistance
"""
import argparse
import os

import generate_report as gr


def rnd(n, sig=2):
    """Round a count to `sig` significant figures for prose (e.g. 44047 -> 44000)."""
    if n < 100:
        return n
    import math
    d = sig - int(math.floor(math.log10(abs(n)))) - 1
    return int(round(n, d))


def pct(n, total):
    return round(n / total * 100) if total else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--regulation', default='omb-financial-assistance')
    ap.add_argument('--parquet', default=None)
    ap.add_argument('--quotes', type=int, default=40, help='how many high-profile quote candidates to list')
    args = ap.parse_args()

    reg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations', args.regulation)
    os.chdir(reg_dir)
    parquet = args.parquet or 'full_run.parquet'

    comments = gr.load_results_parquet(parquet)
    b = gr.compute_briefing(comments)
    total = len(comments)

    def line(label, n, denom=total):
        print(f"  {label:52} {n:>8,}  (~{rnd(n):,}, {pct(n, denom)}%)")

    print(f"\n{'='*78}\nTOTALS  (parquet: {parquet})\n{'='*78}")
    print(f"  Total comments analyzed: {total:,}")

    print(f"\n--- STANCE ---")
    line("Oppose", b['oppose_count'])
    line("Support", b['support_count'])
    line("Unclear", b['unclear_count'])

    print(f"\n--- CAMPAIGNS / FORM LETTERS ---")
    cc = b['campaign_comments_count']
    print(f"  Comments in a form-letter campaign: {cc:,}  (~{rnd(cc):,}, {pct(cc, total)}%)")
    print(f"  Distinct campaigns: {b['campaign_count']:,}")
    print(f"  Unique (non-campaign) comments: {total-cc:,}  (~{rnd(total-cc):,}, {pct(total-cc, total)}%)")

    # form-letter rate WITHIN each stance. A comment is a form-letter member iff
    # campaign_size > 1 (singletons have campaign_id = NaN).
    from collections import Counter
    def in_campaign(c):
        sz = c.get('campaign_size')
        return sz is not None and sz == sz and sz > 1  # NaN-safe
    stance_of = {c.get('id'): gr.comment_position(c) for c in comments}
    by = {'Oppose': [0, 0], 'Support': [0, 0], 'Unclear': [0, 0]}  # [in_campaign, total]
    for c in comments:
        s = stance_of[c.get('id')]
        by[s][1] += 1
        if in_campaign(c):
            by[s][0] += 1
    for s, (inc, tot) in by.items():
        if tot:
            print(f"    {s}: {inc:,}/{tot:,} in a campaign  ({pct(inc, tot)}%)")

    print(f"\n--- ATTACHMENTS ---")
    n_att = sum(1 for c in comments if (c.get('attachment_text') or '').strip())
    print(f"  Comments with attachment text: {n_att:,}  (~{rnd(n_att):,}, {pct(n_att, total)}%)")

    print(f"\n--- SUBMITTER TYPES (entity_type) ---")
    for e in b['entity_counts']:
        line(e['name'], e['count'])

    print(f"\n--- CONCERNS (top 15) ---")
    for c in b['concern_counts'][:15]:
        line(c['name'], c['count'])

    # supporter justifications
    print(f"\n--- CONCERNS AMONG SUPPORTERS (top 10) ---")
    supp_concern = Counter()
    for c in comments:
        if stance_of[c.get('id')] != 'Support':
            continue
        st = (c.get('analysis') or {}).get('stances', [])
        st = st.tolist() if hasattr(st, 'tolist') else (st if isinstance(st, list) else [])
        for s in st:
            if s.startswith('Concern:'):
                supp_concern[s.replace('Concern: ', '')] += 1
    for name, cnt in supp_concern.most_common(10):
        print(f"  {name:52} {cnt:>8,}")

    print(f"\n--- CFR SECTION CITATIONS (top 12) ---")
    fields = gr.load_fields()
    value_sections, patterns = gr.compute_value_sections(comments, fields)
    for vs in value_sections:
        for item in vs['entries'][:12]:
            print(f"  {item['name']:52} {item['count']:>8,}  (~{rnd(item['count']):,})")

    # ---- high-profile quote candidates ----
    print(f"\n{'='*78}\nHIGH-PROFILE QUOTE CANDIDATES (non-form-letter, organizational)\n{'='*78}")
    org_types = [t for t in b['entity_counts'] if t not in ('Individual/Other',)]
    cands = []
    for c in comments:
        a = c.get('analysis') or {}
        et = a.get('entity_type', '')
        if et == 'Individual/Other':
            continue
        if in_campaign(c):  # skip form-letter members
            continue
        q = (a.get('key_quote') or '').strip()
        if not q:
            continue
        cands.append({
            'et': et,
            'org': (c.get('organization') or a.get('entity_name') or c.get('submitter') or '').strip(),
            'stance': stance_of[c.get('id')],
            'score': a.get('key_quote_match_score') or 0,
            'quote': q,
            'id': c.get('id'),
        })
    # prefer named orgs + high quote-match score
    cands.sort(key=lambda x: (x['org'] == '', -x['score']))
    for c in cands[:args.quotes]:
        print(f"\n  [{c['et']} | {c['stance']} | {c['id']}]  {c['org']}")
        print(f"    \"{c['quote'][:400]}\"")


if __name__ == '__main__':
    main()
