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
import datetime as dt
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


class RateLimited(Exception):
    """The hourly comment-call budget is spent. Not an error — stop and resume later."""


def get(path, params, key, tries=3):
    """GET with a SHORT backoff.

    429 means the hourly budget is spent, and it does not come back for the best
    part of an hour. Waiting that out inside the process would mean a scheduled
    run sleeping for hours, so we retry only briefly (in case the 429 is a
    momentary burst limit) and then raise RateLimited. The caller saves what it
    has and exits cleanly; the next scheduled run continues from there.
    """
    url = f'{API}/{path}?' + urllib.parse.urlencode(params) if params else f'{API}/{path}'
    for attempt in range(tries):
        req = urllib.request.Request(url, headers={'X-Api-Key': key})
        try:
            return json.loads(urllib.request.urlopen(req, timeout=60).read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                if attempt == tries - 1:
                    raise RateLimited()
                wait = 5 * (attempt + 1)
                print(f'  rate limited, brief retry in {wait}s', flush=True)
                time.sleep(wait)
                continue
            if e.code >= 500 and attempt < tries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            raise
    raise RateLimited()


def list_comment_ids(docket, key, since=None):
    """Comment ids on the docket, paging by lastModifiedDate.

    The API caps a result set at 5,000 (20 pages x 250), so once a window fills
    we restart from the last timestamp seen. That is the documented way to walk
    a set larger than the cap.

    `since` (YYYY-MM-DD) starts the walk from that date instead of the beginning
    of the docket. This matters more than it looks: listing costs one call per
    250 comments, so walking a 61,000-comment docket burns ~245 of the ~500 calls
    the key allows per hour — half the budget spent before a single new comment
    is fetched. Starting from the newest row already held costs a handful.

    `since` seeds the SAME lastModifiedDate cursor the paging already uses,
    rather than adding a second postedDate filter: the API returns 400 when both
    date filters are sent at once, which only shows up once a result set exceeds
    the 5,000 cap and the cursor kicks in. Filtering on lastModifiedDate is a
    superset of what we need (anything newly posted was also newly modified);
    the extra rows are already-known ids and cost nothing.
    """
    seen = {}
    cursor = f'{since} 00:00:00' if since else None
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
        # The filter wants 'YYYY-MM-DD HH:mm:ss'; the API hands back
        # '2026-07-30T18:59:54Z'. Feeding its own value straight back is a 400 —
        # a latent bug that only surfaces once a result set passes the 5,000 cap
        # and this cursor path is first used.
        last_raw = max(seen.values()) if seen else None
        last = last_raw.replace('T', ' ').rstrip('Z') if last_raw else None
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
    ap.add_argument('--full-list', action='store_true',
                    help='walk the whole docket instead of only what was posted since the '
                         'newest row already held. Needed to bootstrap an empty CSV, or to '
                         'pick up comments backfilled with an older posted date.')
    args = ap.parse_args()

    # Read .env next to this script, as pipeline.py does, so a local run picks up
    # REGULATIONS_API_KEY instead of silently falling back to DEMO_KEY. In CI the
    # variable is already exported and there is no .env, which is fine.
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
    except ImportError:
        pass

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
    newest_posted = ''
    if os.path.exists(args.csv):
        with open(args.csv, newline='', encoding='utf-8') as f:
            for r in csv.DictReader(f):
                known.add(r['Document ID'])
                posted = (r.get('Posted Date') or '')[:10]
                if posted > newest_posted:
                    newest_posted = posted
    print(f'{docket}: {len(known):,} comments already in {args.csv}')

    # Only list what could possibly be new. Back up a day from the newest row we
    # already hold, so anything posted the same day can't slip through a
    # timezone or same-day-ordering edge. --full-list forces the whole walk, for
    # bootstrapping an empty CSV or picking up comments backfilled with an older
    # posted date.
    since = None
    if newest_posted and not args.full_list:
        since = (dt.date.fromisoformat(newest_posted) - dt.timedelta(days=1)).isoformat()
        print(f'listing comments posted since {since} (newest already held: {newest_posted})')
    else:
        print('listing every comment on the docket (this costs ~1 call per 250)...')

    try:
        ids = list_comment_ids(docket, key, since=since)
    except RateLimited:
        print('Rate limited while listing — nothing fetched. The next run will retry.')
        return
    missing = [i for i in ids if i not in known]
    print(f'listing returned {len(ids):,}; {len(missing):,} not in the CSV')
    if args.dry_run or not missing:
        for m in missing[:20]:
            print('   ', m)
        if not args.dry_run:
            print(f'Caught up: 0 comments missing from {args.csv}.')
        return

    total_missing = len(missing)
    if args.limit:
        missing = missing[:args.limit]

    # Append as we go rather than accumulating and writing at the end: when the
    # hourly budget runs out mid-run we keep every comment fetched so far
    # instead of throwing the whole run away.
    written = 0
    stopped_early = False
    with open(args.csv, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        for n, docid in enumerate(missing, 1):
            try:
                writer.writerow(row_for(docid, key))
            except RateLimited:
                print(f'\nRate limit reached after {written:,} — stopping here and '
                      f'keeping what was fetched.')
                stopped_early = True
                break
            except Exception as e:
                print(f'\n  FAILED {docid}: {e}')
                continue
            written += 1
            if written % 50 == 0:
                f.flush()
            print(f'  fetched {n:,}/{len(missing):,}', end='\r', flush=True)
    print()
    print(f'appended {written:,} rows to {args.csv} (now {len(known) + written:,})')

    # Stopping early — capped or rate limited — is expected, not a failure. Exit
    # 0 either way and say how much is left, so the next scheduled run picks up.
    remaining = total_missing - written
    if remaining > 0:
        why = 'rate limited' if stopped_early else f'CAPPED at --limit {args.limit}'
        print(f'{why}: {remaining:,} more comment(s) still missing from {args.csv} '
              f'— next run will continue.')
    else:
        print(f'Caught up: 0 comments missing from {args.csv}.')


if __name__ == '__main__':
    main()
