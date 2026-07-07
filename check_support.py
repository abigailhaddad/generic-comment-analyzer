#!/usr/bin/env python3
"""Check all comments classified as 'Support' to find false positives."""

import pandas as pd
import sys


def check_support(parquet_file='full_run.parquet'):
    df = pd.read_parquet(parquet_file)

    support_comments = []
    oppose_count = 0
    support_count = 0

    for _, row in df.iterrows():
        analysis = row.get('analysis')
        if not analysis or not isinstance(analysis, dict):
            continue
        stances = analysis.get('stances', [])
        if hasattr(stances, 'tolist'):
            stances = stances.tolist()
        if not isinstance(stances, list):
            stances = []

        has_support = any('Support' in s for s in stances)
        has_oppose = any('Oppose' in s for s in stances)

        if has_oppose:
            oppose_count += 1
        if has_support:
            support_count += 1
            support_comments.append({
                'id': row['id'],
                'text': (row.get('comment_text', '') or '')[:300],
                'key_quote': analysis.get('key_quote', ''),
                'rationale': analysis.get('rationale', ''),
            })

    print(f'Total comments: {len(df)}')
    print(f'Oppose: {oppose_count}')
    print(f'Support: {support_count}')
    print()

    if support_comments:
        print(f'=== ALL {len(support_comments)} SUPPORT COMMENTS ===')
        print()
        for c in support_comments:
            print(f'--- {c["id"]} ---')
            print(f'Text: {c["text"]}')
            print(f'Quote: {c["key_quote"]}')
            print(f'Rationale: {c["rationale"]}')
            print()


if __name__ == '__main__':
    f = sys.argv[1] if len(sys.argv) > 1 else 'full_run.parquet'
    check_support(f)
