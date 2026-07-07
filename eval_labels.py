#!/usr/bin/env python3
"""Evaluate LLM entity type classifications against gold labels."""

import pandas as pd
import sys


def evaluate(parquet_file='test_run.parquet', gold_file='gold_labels.csv'):
    df = pd.read_parquet(parquet_file)
    gold = pd.read_csv(gold_file)

    results = []
    for _, g in gold.iterrows():
        match = df[df['id'] == g['id']]
        if match.empty:
            continue
        row = match.iloc[0]
        a = row.get('analysis') or {}
        llm_type = a.get('entity_type', '')
        human_type = g['human_entity_type']
        correct = llm_type == human_type
        results.append({
            'id': g['id'],
            'human': human_type,
            'llm': llm_type,
            'correct': correct,
            'notes': g.get('notes', ''),
        })

    results_df = pd.DataFrame(results)
    total = len(results_df)
    correct = results_df['correct'].sum()
    print(f'Accuracy: {correct}/{total} ({correct/total*100:.0f}%)')
    print()

    errors = results_df[~results_df['correct']]
    if len(errors) > 0:
        print(f'ERRORS ({len(errors)}):')
        for _, e in errors.iterrows():
            print(f'  {e["id"]}: LLM={e["llm"]} | should be={e["human"]}')
            print(f'    {e["notes"]}')
            print()
    else:
        print('No errors!')


if __name__ == '__main__':
    parquet = sys.argv[1] if len(sys.argv) > 1 else 'test_run.parquet'
    evaluate(parquet)
