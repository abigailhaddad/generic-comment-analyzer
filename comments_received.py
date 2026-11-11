"""Comments *submitted* to a docket, as a small disclosure beside the published count.

regulations.gov publishes only some of what it receives. The comments endpoint
(and everything this repo analyzes) counts the published ones; the number
received comes from a separate endpoint, keyed by the document being commented on:

    GET /v4/document-comments-received-counts/<documentId>

Two places hold the value, and `load` returns whichever is newer:

  * comments_received.json next to the regulation's data: written by `refresh`
    (every pipeline run and every fetch run).
  * the parquet's file-level metadata, key `comments_received`: written by
    pipeline.save_results, so the number travels with the analysis it was
    reported beside. Columns and rows are untouched.

Rendering a page needs no key and no network. Nothing found means no disclosure.
"""
import csv
import datetime as dt
import json
import os
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Optional

CACHE = 'comments_received.json'
PARQUET_KEY = b'comments_received'
API = 'https://api.regulations.gov/v4'


def document_id(reg_dir: Path, csv_path: str = 'source.csv') -> str:
    """The document comments are filed against: regulation_metadata.json
    `document_id` if set, else the one most rows of the csv point at."""
    reg_dir = Path(reg_dir)
    try:
        meta = json.loads((reg_dir / 'regulation_metadata.json').read_text(encoding='utf-8'))
        if meta.get('document_id'):
            return meta['document_id']
    except (OSError, ValueError):
        pass
    try:
        with open(reg_dir / csv_path, newline='', encoding='utf-8') as f:
            seen = Counter(r.get('Comment on Document ID') or '' for r in csv.DictReader(f))
    except OSError:
        return ''
    seen.pop('', None)
    return seen.most_common(1)[0][0] if seen else ''


def fetch_count(doc: str, key: str) -> int:
    req = urllib.request.Request(f'{API}/document-comments-received-counts/{doc}', headers={'X-Api-Key': key})
    data = json.loads(urllib.request.urlopen(req, timeout=60).read())
    return int(data['data']['attributes']['count'])


def refresh(reg_dir: Path, key: Optional[str] = None, csv_path: str = 'source.csv') -> Optional[dict]:
    """Fetch the received count and write the cache. Never raises: the disclosure
    is optional, and a stale value beats failing a run over it."""
    reg_dir = Path(reg_dir)
    doc = document_id(reg_dir, csv_path)
    if not doc:
        return None
    try:
        count = fetch_count(doc, key or os.environ.get('REGULATIONS_API_KEY', 'DEMO_KEY'))
    except Exception as e:
        print(f'  could not fetch comments received for {doc}: {e}')
        return None
    info = {'document_id': doc, 'count': count, 'as_of': dt.date.today().isoformat()}
    (reg_dir / CACHE).write_text(json.dumps(info, indent=2), encoding='utf-8')
    print(f'comments received on {doc}: {count:,}')
    return info


def _clean(d) -> Optional[dict]:
    try:
        return {'document_id': d.get('document_id', ''), 'count': int(d['count']), 'as_of': d.get('as_of', '')}
    except (AttributeError, KeyError, TypeError, ValueError):
        return None


def from_cache(reg_dir: Path = Path('.')) -> Optional[dict]:
    try:
        return _clean(json.loads((Path(reg_dir) / CACHE).read_text(encoding='utf-8')))
    except (OSError, ValueError):
        return None


def from_schema_metadata(metadata) -> Optional[dict]:
    """The value out of a pyarrow schema's metadata mapping (bytes keys), or None."""
    try:
        return _clean(json.loads((metadata or {})[PARQUET_KEY]))
    except (KeyError, ValueError, TypeError):
        return None


def from_parquet(path) -> Optional[dict]:
    try:
        import pyarrow.parquet as pq
        return from_schema_metadata(pq.read_schema(str(path)).metadata)
    except Exception:
        return None


def load(reg_dir: Path = Path('.'), parquet=None) -> Optional[dict]:
    """The newest of the cache and the parquet's embedded value."""
    found = [x for x in (from_cache(reg_dir), from_parquet(parquet) if parquet else None) if x]
    return max(found, key=lambda x: x['as_of']) if found else None


def embed(table, info: Optional[dict]):
    """`table` (pyarrow) with `info` in its schema metadata; unchanged when info is None."""
    if not info:
        return table
    return table.replace_schema_metadata({**(table.schema.metadata or {}), PARQUET_KEY: json.dumps(info).encode()})
