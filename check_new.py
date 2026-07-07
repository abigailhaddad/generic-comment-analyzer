#!/usr/bin/env python3
"""Quick check: how many comments are posted on a docket vs. what the local CSV has.

Reads the docket id from the regulation's regulation_metadata.json.

Usage:
    python check_new.py --regulation omb-financial-assistance
"""
import argparse
import glob
import json
import os
import urllib.parse
import urllib.request

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description='Compare regulations.gov comment counts to the local CSV')
    parser.add_argument('--regulation', type=str, help='Regulation slug under regulations/<slug>/')
    parser.add_argument('--csv', type=str, default='source.csv', help='Local CSV to compare against (default: source.csv)')
    args = parser.parse_args()

    if args.regulation:
        reg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations', args.regulation)
        if not os.path.isdir(reg_dir):
            raise SystemExit(f"Regulation directory not found: {reg_dir}")
        os.chdir(reg_dir)

    with open('regulation_metadata.json', 'r', encoding='utf-8') as f:
        docket_id = json.load(f)['docket_id']

    params = {
        'filter[docketId]': docket_id,
        'sort': '-postedDate',
        'page[size]': '5',
    }
    url = 'https://api.regulations.gov/v4/comments?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={'X-Api-Key': os.environ.get('REGULATIONS_API_KEY', 'DEMO_KEY')})
    data = json.loads(urllib.request.urlopen(req).read())

    aggs = {a['label']: a['docCount'] for a in data['meta']['aggregations']['postedDate']}
    print(f'Posted on regulations.gov (docket {docket_id}):')
    for k in ('Today', 'Last 3 Days', 'Last 7 Days', 'Last 15 Days', 'Last 30 Days'):
        if k in aggs:
            print(f'  {k}: {aggs[k]:,}')

    # compare to the local CSV
    csv_file = args.csv if os.path.exists(args.csv) else next(iter(sorted(glob.glob('*.csv'))), None)
    if csv_file:
        df = pd.read_csv(csv_file, low_memory=False)
        df['pd'] = pd.to_datetime(df['Posted Date']).dt.date
        today = pd.Timestamp.today().date()
        local_today = (df['pd'] == today).sum()
        print(f'\nLocal CSV: {csv_file} ({len(df):,} total, {local_today} posted today)')
        if 'Today' in aggs:
            print(f'New since local pull: {aggs["Today"] - local_today}')


if __name__ == '__main__':
    main()
