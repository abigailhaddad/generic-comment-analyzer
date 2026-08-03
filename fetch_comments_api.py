#!/usr/bin/env python3
"""Fetch new comments from the regulations.gov API and append them to source.csv.

A workaround for the bulk-download form (regulations.gov/bulkdownload), whose
instructions ask for a date range that has no input field, so the request cannot
be submitted. This pulls the same data through the documented v4 API instead and
writes rows in the identical CSV schema, so everything downstream is unchanged.

It works incrementally: existing Document IDs are read from source.csv and only
comments not already present are fetched. A full docket costs one API call per
comment, which is impractical at the standard 1,000/hour key limit; a daily
delta of ~1,000 is comfortable.

Requires REGULATIONS_API_KEY (free: https://open.gsa.gov/api/regulationsgov/).
DEMO_KEY works but rate-limits almost immediately.

Usage:
    python fetch_comments_api.py --regulation omb-financial-assistance
    python fetch_comments_api.py --regulation omb-financial-assistance --dry-run
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API = 'https://api.regulations.gov/v4'

# CSV header, in the order the bulk export produces. Two headers contain commas
# and are quoted in the real file; csv.DictWriter re-quotes them identically.
COLUMNS = [
    'Document ID', 'Agency ID', 'Docket ID', 'Tracking Number', 'Document Type',
    'Posted Date', 'Is Withdrawn?', 'Federal Register Number', 'FR Citation', 'Title',
    'Comment Start Date', 'Comment Due Date', 'Allow Late Comments',
    'Comment on Document ID', 'Effective Date', 'Implementation Date', 'Postmark Date',
    'Received Date', 'Author Date', 'Related RIN(s)', 'Authors', 'CFR', 'Abstract',
    'Legacy ID', 'Media', 'Document Subtype', 'Exhibit Location', 'Exhibit Type',
    'Additional Field 1', 'Additional Field 2', 'Topics', 'Duplicate Comments',
    'OMB/PRA Approval Number', 'Page Count', 'Page Length', 'Paper Width',
    'Special Instructions', 'Source Citation', 'Start End Page', 'Subject',
    'First Name', 'Last Name', 'City', 'State/Province', 'Zip/Postal Code', 'Country',
    'Organization Name', 'Submitter Representative', "Representative's Address",
    "Representative's City, State & Zip", 'Government Agency', 'Government Agency Type',
    'Comment', 'Category', 'Restrict Reason Type', 'Restrict Reason', 'Reason Withdrawn',
    'Content Files', 'Attachment Files',
    'Display Properties (Name, Label, Tooltip)',
]

# API attribute -> CSV column. Anything not listed is left blank, matching the
# bulk export, which also leaves most of these empty for public submissions.
FIELD_MAP = {
    'agencyId': 'Agency ID', 'docketId': 'Docket ID', 'trackingNbr': 'Tracking Number',
    'documentType': 'Document Type', 'title': 'Title', 'legacyId': 'Legacy ID',
    'commentOnDocumentId': 'Comment on Document ID', 'postmarkDate': 'Postmark Date',
    'subtype': 'Document Subtype', 'field1': 'Additional Field 1',
    'field2': 'Additional Field 2', 'duplicateComments': 'Duplicate Comments',
    'pageCount': 'Page Count', 'firstName': 'First Name', 'lastName': 'Last Name',
    'city': 'City', 'stateProvinceRegion': 'State/Province', 'zip': 'Zip/Postal Code',
    'country': 'Country', 'organization': 'Organization Name',
    'submitterRep': 'Submitter Representative', 'submitterRepAddress': "Representative's Address",
    'submitterRepCityState': "Representative's City, State & Zip",
    'govAgency': 'Government Agency', 'govAgencyType': 'Government Agency Type',
    'comment': 'Comment', 'category': 'Category',
    'restrictReasonType': 'Restrict Reason Type', 'restrictReason': 'Restrict Reason',
    'reasonWithdrawn': 'Reason Withdrawn', 'docAbstract': 'Abstract',
}


def get(path, params, key, tries=6):
    """GET with backoff. 429 is the normal signal that the hourly budget is spent."""
    url = f'{API}/{path}?' + urllib.parse.urlencode(params) if params else f'{API}/{path}'
    for attempt in range(tries):
        req = urllib.request.Request(url, headers={'X-Api-Key': key})
        try:
            return json.loads(urllib.request.urlopen(req, timeout=60).read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = min(300, 20 * 2 ** attempt)
                print(f'  rate limited, waiting {wait}s', flush=True)
                time.sleep(wait)
                continue
            if e.code >= 500 and attempt < tries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            raise
    raise SystemExit('gave up after repeated rate limiting — try again later '
                     'or request a higher API rate limit')


def list_comment_ids(docket, key):
    """Every comment id on the docket, paging by lastModifiedDate.

    The API caps a result set at 5,000 (20 pages x 250), so once a window fills
    we restart from the last timestamp seen. That is the documented way to walk
    a set larger than the cap.
    """
    seen, cursor = {}, None
    while True:
        page, drained = 1, False
        while page <= 20:
            params = {'filter[docketId]': docket, 'page[size]': 250,
                      'page[number]': page, 'sort': 'lastModifiedDate'}
            if cursor:
                params['filter[lastModifiedDate][ge]'] = cursor
            data = get('comments', params, key)
            rows = data.get('data', [])
            if not rows:
                drained = True
                break
            for r in rows:
                seen[r['id']] = r['attributes'].get('lastModifiedDate')
            print(f'  listed {len(seen):,}', end='\r', flush=True)
            if not data['meta'].get('hasNextPage'):
                drained = True
                break
            page += 1
        last = max(seen.values()) if seen else None
        if drained or last == cursor:
            break
        cursor = last
    print()
    return seen


def row_for(docid, key):
    """One CSV row from the detail endpoint, attachments included."""
    d = get(f'comments/{docid}', {'include': 'attachments'}, key)
    a = d['data']['attributes']
    row = {c: '' for c in COLUMNS}
    row['Document ID'] = docid
    for src, dst in FIELD_MAP.items():
        v = a.get(src)
        if v is not None and v != '':
            row[dst] = v
    # The bulk export writes these as lowercase JSON-ish literals, and omits a
    # duplicate count of zero rather than writing 0. Match it exactly so a row
    # fetched here is indistinguishable from a downloaded one.
    row['Is Withdrawn?'] = 'true' if a.get('withdrawn') else 'false'
    row['Allow Late Comments'] = 'true' if a.get('openForComment') else 'false'
    row['Duplicate Comments'] = a.get('duplicateComments') or ''

    props = []
    for p in a.get('displayProperties') or []:
        props.append(', '.join(str(p.get(k, '')) for k in ('name', 'label', 'tooltip')))
    row['Display Properties (Name, Label, Tooltip)'] = '\n'.join(props)

    for src, dst in (('postedDate', 'Posted Date'), ('receiveDate', 'Received Date')):
        v = a.get(src)
        if v:
            # Bulk export style: 2026-07-13T04:00Z
            row[dst] = v.replace(':00Z', 'Z') if v.endswith(':00:00Z') else v

    urls = []
    for inc in d.get('included', []) or []:
        for f in inc.get('attributes', {}).get('fileFormats') or []:
            if f.get('fileUrl'):
                urls.append(f['fileUrl'])
    row['Attachment Files'] = ','.join(urls)
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--regulation', required=True)
    ap.add_argument('--csv', default='source.csv')
    ap.add_argument('--limit', type=int, default=0, help='stop after N new comments')
    ap.add_argument('--dry-run', action='store_true', help='list what is missing, fetch nothing')
    args = ap.parse_args()

    key = os.environ.get('REGULATIONS_API_KEY', 'DEMO_KEY')
    if key == 'DEMO_KEY':
        print('WARNING: using DEMO_KEY, which rate-limits after a handful of calls.\n'
              '         Get a free key at https://open.gsa.gov/api/regulationsgov/ '
              'and set REGULATIONS_API_KEY.\n', file=sys.stderr)

    reg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations',
                           args.regulation)
    os.chdir(reg_dir)
    docket = json.load(open('regulation_metadata.json'))['docket_id']

    known = set()
    if os.path.exists(args.csv):
        with open(args.csv, newline='', encoding='utf-8') as f:
            for r in csv.DictReader(f):
                known.add(r['Document ID'])
    print(f'{docket}: {len(known):,} comments already in {args.csv}')

    print('listing docket comments...')
    ids = list_comment_ids(docket, key)
    missing = [i for i in ids if i not in known]
    print(f'docket has {len(ids):,}; {len(missing):,} not in the CSV')
    if args.dry_run or not missing:
        for m in missing[:20]:
            print('   ', m)
        return

    total_missing = len(missing)
    if args.limit:
        missing = missing[:args.limit]
    rows = []
    for n, docid in enumerate(missing, 1):
        try:
            rows.append(row_for(docid, key))
        except Exception as e:
            print(f'\n  FAILED {docid}: {e}')
            continue
        print(f'  fetched {n:,}/{len(missing):,}', end='\r', flush=True)
    print()

    # Append rather than rewrite, so a partial run is never destructive.
    with open(args.csv, 'a', newline='', encoding='utf-8') as f:
        csv.DictWriter(f, fieldnames=COLUMNS).writerows(rows)
    print(f'appended {len(rows):,} rows to {args.csv} (now {len(known) + len(rows):,})')

    # A capped run stopping early is expected, not a failure — say so plainly
    # (exit 0 either way) so a scheduled job can pick up the rest next time.
    remaining = total_missing - len(rows)
    if args.limit and remaining > 0:
        print(f'CAPPED at --limit {args.limit}: {remaining:,} more comment(s) still missing '
              f'from {args.csv} — next run will continue.')
    else:
        print(f'Caught up: 0 comments missing from {args.csv}.')


if __name__ == '__main__':
    main()
