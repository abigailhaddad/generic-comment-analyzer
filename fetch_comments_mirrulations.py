#!/usr/bin/env python3
"""Fetch missing comments for a docket from the public Mirrulations mirror.

Backup for fetch_comments_api.py: a regulations.gov key is capped at ~500
"comment" calls/hour, and a large backlog (or several dispatches sharing an
hour) can leave it rate-limited for a long time. Mirrulations
(https://github.com/mirrulations/mirrulations) maintains an open, public
mirror of regulations.gov on S3 -- s3://mirrulations, anonymous read, no API
key, no rate limit -- built from a pool of donated regulations.gov keys.
Confirmed 2026-09-23: for USBC-2026-0628 its snapshot (5,103 comments, newest
dated the same day) was AHEAD of what our own rate-limited key had pulled.
It is not guaranteed current for every docket, though -- OMB-2026-0034's
mirror trails by weeks, though that specific gap is at least partly because
OMB's own live fetch has been manual-only, not necessarily because the
mirror itself lags the live API. Treat this as a supplement to
fetch_comments_api.py, not a replacement: run the live fetch first, then
this script for whatever is still missing.

Reads and writes the identical source.csv schema as fetch_comments_api.py
(shares its COLUMNS and attrs_to_row so a row from either source is
indistinguishable) so everything downstream is unaffected by which one
fetched a given row. Attachments are pulled from the mirror's own binary
copies (comments_attachments/<comment id>_attachment_<n>.<ext>, plain HTTPS,
no signing) rather than a fileUrl, since the mirrored comment JSON doesn't
carry one.

No credentials, no dependencies beyond the stdlib (matches
fetch_comments_api.py's own no-dependency listing/fetch path).

Usage:
    python fetch_comments_mirrulations.py --regulation usbc-2026-0628
    python fetch_comments_mirrulations.py --regulation usbc-2026-0628 --dry-run
"""
import argparse
import csv
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from fetch_comments_api import COLUMNS, attrs_to_row

MIRROR = 'https://mirrulations.s3.amazonaws.com'
NS = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}


def _list_keys(prefix):
    """Every object key under a prefix, via anonymous S3 ListObjectsV2 (plain HTTPS)."""
    keys, token = [], None
    while True:
        params = {'list-type': '2', 'prefix': prefix, 'max-keys': '1000'}
        if token:
            params['continuation-token'] = token
        url = f'{MIRROR}/?' + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=60) as resp:
            root = ET.fromstring(resp.read())
        for c in root.findall('s3:Contents', NS):
            keys.append(c.find('s3:Key', NS).text)
        token_el = root.find('s3:NextContinuationToken', NS)
        if token_el is None:
            break
        token = token_el.text
    return keys


def list_mirrored_ids(agency, docket):
    """Comment ids Mirrulations has mirrored for this docket."""
    prefix = f'raw-data/{agency}/{docket}/text-{docket}/comments/'
    return [os.path.basename(k)[:-len('.json')] for k in _list_keys(prefix)]


def list_mirrored_attachments(agency, docket):
    """Document ID -> [attachment URL, ...], from the mirror's binary copies.

    Empty (not an error) if this docket has no binary-<docket> prefix at all --
    Mirrulations only creates it once a docket has at least one attachment.
    """
    prefix = f'raw-data/{agency}/{docket}/binary-{docket}/comments_attachments/'
    try:
        keys = _list_keys(prefix)
    except urllib.error.HTTPError:
        return {}
    out = {}
    for key in keys:
        m = re.match(r'(.+?)_attachment_\d+\.\w+$', os.path.basename(key))
        if m:
            out.setdefault(m.group(1), []).append(f'{MIRROR}/{key}')
    return out


def fetch_row(agency, docket, raw_id, attachments):
    """`raw_id` is the mirror's file-key id, which is NOT always the real one.

    Regulations.gov sometimes reuses the same Document ID across different
    comments (see the "Never assume Document IDs are unique" convention);
    Mirrulations disambiguates its own storage keys for that with a
    "<id>(1).json" suffix, but the comment resource inside still reports the
    true, non-suffixed id in data.id. Writing the suffixed key as the row's
    Document ID would create an id source.csv's own dedup logic has never
    seen, so the true id is read back out of the fetched JSON instead.
    """
    url = f'{MIRROR}/raw-data/{agency}/{docket}/text-{docket}/comments/{urllib.parse.quote(raw_id)}.json'
    with urllib.request.urlopen(url, timeout=60) as resp:
        d = json.loads(resp.read())
    true_id = d['data'].get('id') or raw_id
    row = attrs_to_row(true_id, d['data']['attributes'])
    row['Attachment Files'] = ','.join(attachments.get(true_id, []))
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--regulation', required=True)
    ap.add_argument('--csv', default='source.csv')
    ap.add_argument('--limit', type=int, default=0, help='stop after N new comments')
    ap.add_argument('--dry-run', action='store_true', help='list what is missing, fetch nothing')
    args = ap.parse_args()

    reg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations',
                            args.regulation)
    os.chdir(reg_dir)
    docket = json.load(open('regulation_metadata.json'))['docket_id']
    agency = docket.split('-')[0]

    known = set()
    if os.path.exists(args.csv):
        with open(args.csv, newline='', encoding='utf-8') as f:
            for r in csv.DictReader(f):
                known.add(r['Document ID'])
    print(f'{docket}: {len(known):,} comments already in {args.csv}')

    print(f'listing the Mirrulations mirror for {agency}/{docket}...')
    mirrored = list_mirrored_ids(agency, docket)
    missing = [i for i in mirrored if i not in known]
    print(f'mirror has {len(mirrored):,} comment(s); {len(missing):,} not in the CSV')

    if args.dry_run:
        for m in missing[:20]:
            print('   ', m)
        return
    if not missing:
        print(f'Caught up: 0 comments missing from {args.csv}.')
        return

    total_missing = len(missing)
    fetch_ids = missing[:args.limit] if args.limit else missing

    print('listing mirrored attachments...')
    attachments = list_mirrored_attachments(agency, docket)

    written = 0
    failed = 0
    with open(args.csv, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        for n, docid in enumerate(fetch_ids, 1):
            try:
                writer.writerow(fetch_row(agency, docket, docid, attachments))
            except Exception as e:
                print(f'\n  FAILED {docid}: {e}')
                failed += 1
                continue
            written += 1
            if written % 50 == 0:
                f.flush()
            print(f'  fetched {n:,}/{len(fetch_ids):,}', end='\r', flush=True)
    print()
    print(f'appended {written:,} rows to {args.csv} from Mirrulations (now {len(known) + written:,})')

    # Two distinct reasons this can be nonzero: a deliberate --limit cap, or a
    # handful of per-item failures (a transient connection reset, say) inside
    # a run that otherwise walked the whole list. Only the first is a cap.
    capped = total_missing - len(fetch_ids)
    if capped > 0:
        print(f'CAPPED at --limit {args.limit}: {capped:,} more comment(s) still missing '
              f'from {args.csv} -- next run will continue.')
    if failed > 0:
        print(f'{failed:,} comment(s) failed to fetch (see FAILED lines above) -- rerun to retry.')
    if capped == 0 and failed == 0:
        print(f'Caught up: 0 comments missing from {args.csv} against the Mirrulations mirror.')


if __name__ == '__main__':
    main()
