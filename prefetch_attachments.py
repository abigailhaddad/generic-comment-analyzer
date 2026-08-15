#!/usr/bin/env python3
"""Warm the attachment extraction cache in parallel, before running the pipeline.

`pipeline.py --workers N` only parallelises the LLM analysis stage. The attachment
step is a plain sequential `for` inside read_comments_from_csv, so it downloads and
OCRs one file at a time while the rest of the machine idles. On a docket with
thousands of uncached attachments that alone runs for hours: measured at ~27/min
against ~250/min here, it turned a 3.8-hour phase into 10 minutes.

This calls the project's OWN process_attachments, so extraction is byte-for-byte
what the pipeline would have produced — same downloader, same PyMuPDF path, same
OCR, same `<file>.extracted.txt` cache the pipeline consults before downloading.
Nothing is reimplemented, so there is no text drift and therefore no spurious
re-analysis (a comment whose attachment text changes gets a new text key).

Run it to completion FIRST, then start the pipeline. Do not run the two at once:
they will race on the same files, and a comment being written by one process while
the other reads it produces "Package not found" style failures.

Usage (from the regulation directory):
    PREFETCH_WORKERS=12 python ../../prefetch_attachments.py
"""
import concurrent.futures as cf
import csv
import logging
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.WARNING)
for noisy in ('LiteLLM', 'httpx', 'urllib3', 'attachment_utils'):
    logging.getLogger(noisy).setLevel(logging.ERROR)

from attachment_utils import process_attachments  # noqa: E402

ATT_COL = 'Attachment Files'
ATT_DIR = 'attachments'
WORKERS = int(os.environ.get('PREFETCH_WORKERS', '12'))

lock = threading.Lock()
done = skipped = failed = 0


def already_cached(row):
    """True when this comment's attachments are all extracted.

    Counts distinct filename STEMS, not URLs. regulations.gov often stores one
    upload twice — the submitter's original plus a PDF rendition — and
    process_attachments deliberately reads only the first of each group that
    yields text. Requiring one extraction per URL therefore marks every such
    comment permanently incomplete and re-downloads it on every run; on this
    docket that was ~1,700 comments fetched again for nothing.
    """
    cid = row.get('Document ID', '')
    d = os.path.join(ATT_DIR, cid)
    if not os.path.isdir(d):
        return False
    urls = [u.strip() for u in (row.get(ATT_COL) or '').split(',') if u.strip()]
    if not urls:
        return True
    stems = {os.path.splitext(os.path.basename(u))[0].lower() for u in urls}
    hits = [f for f in os.listdir(d) if f.endswith('.extracted.txt') and
            os.path.getsize(os.path.join(d, f)) > 0]
    return len(hits) >= len(stems)


def work(row):
    global done, skipped, failed
    try:
        if already_cached(row):
            with lock:
                skipped += 1
            return
        process_attachments(row, ATT_DIR, ATT_COL, use_gemini=False)
        with lock:
            done += 1
    except Exception as e:                       # keep going; the pipeline retries
        with lock:
            failed += 1
        print(f'  FAIL {row.get("Document ID")}: {type(e).__name__}: {e}', flush=True)


def main():
    csv.field_size_limit(sys.maxsize)
    with open('source.csv', newline='', encoding='utf-8', errors='replace') as f:
        rows = [r for r in csv.DictReader(f)
                if (r.get(ATT_COL) or '').strip()
                and r.get('Document Type') == 'Public Submission']
    print(f'{len(rows):,} comments with attachments; {WORKERS} workers', flush=True)

    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(work, r) for r in rows]
        last = 0
        for i, _ in enumerate(cf.as_completed(futs), 1):
            if time.time() - last >= 30:
                last = time.time()
                el = time.time() - t0
                rate = i / el * 60 if el else 0
                left = (len(rows) - i) / rate if rate else 0
                print(f'  {i:,}/{len(rows):,}  fetched={done:,} cached={skipped:,} '
                      f'failed={failed:,}  {rate:.0f}/min  ~{left:.0f} min left', flush=True)

    print(f'DONE in {(time.time()-t0)/60:.1f} min — '
          f'fetched {done:,}, already cached {skipped:,}, failed {failed:,}', flush=True)


if __name__ == '__main__':
    main()
