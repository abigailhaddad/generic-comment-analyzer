#!/usr/bin/env python3
"""Pull/push a regulation's local state (source.csv, full_run.parquet) to Cloudflare R2.

source.csv and full_run.parquet are gitignored (too big for git) but are exactly
what a scheduled run needs to resume from: the last CSV the API fetcher appended
to, and the last parquet the pipeline analyzed into. This is how a clean GitHub
Actions checkout (which starts with neither file) picks up where the previous
run left off, and how a laptop run can pick up whatever CI last produced.

State lives at r2://<bucket>/state/<slug>/{source.csv.gz,full_run.parquet.gz}.
The bucket comes from the regulation's own `analyzer_config.yaml` (`state.bucket`,
falling back to the already-established `report.full_export.bucket`) — never
hardcoded, per the no-regulation-strings-in-code convention.

Uses the `aws` CLI against the R2 S3-compatible endpoint (the same tool
deploy_report.sh already uses) rather than boto3, so there's only one
AWS-talking dependency to keep working.

Requires CF_R2_ACCOUNT_ID / CF_R2_ACCESS_KEY_ID / CF_R2_SECRET_ACCESS_KEY
(from .env locally, from repo secrets in CI).

Idempotent: re-running pull or push is always safe. A 0-byte or corrupt
download is refused rather than used to overwrite good local state; an empty
local file is refused rather than pushed over good remote state.

Usage:
    python sync_state.py pull --regulation omb-financial-assistance
    python sync_state.py push --regulation omb-financial-assistance
    # Pull just source.csv (e.g. for a cheap "is there anything new" check
    # before paying for the bigger full_run.parquet download):
    python sync_state.py pull --regulation omb-financial-assistance --files source.csv
"""
import argparse
import gzip
import os
import shutil
import subprocess
import tarfile
import sys
import tempfile

import yaml

# `attachment_cache` is a directory of extracted text rather than one file, so it
# travels as a tar. It holds only the `.extracted.txt` files (~30 MB for 65k
# comments), never the downloaded PDFs/images themselves (~450 MB) — the text is
# the expensive part, because reproducing it means re-downloading, re-extracting,
# and for scanned pages paying for vision OCR.
#
# Syncing it matters for more than speed. `process_attachments` reads a cached
# extraction before it downloads anything, so a CI run that starts with the cache
# skips that work entirely. Without it every run re-OCRs from scratch, and OCR
# output varies run to run, which changes the comment's text, which misses the
# text-keyed analysis cache and re-analyses a comment nothing about which changed.
# It is also what stopped an API outage from silently dropping attachments: on
# 2026-08-07 an exhausted key failed ~174 OCR calls, and with no cache to fall
# back on those comments were published with their attached letter missing.
ATTACHMENT_CACHE = 'attachment_cache'
FILES = ['source.csv', 'full_run.parquet', ATTACHMENT_CACHE]
CREDS = ('CF_R2_ACCOUNT_ID', 'CF_R2_ACCESS_KEY_ID', 'CF_R2_SECRET_ACCESS_KEY')


def require_creds():
    missing = [v for v in CREDS if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"Missing R2 credentials: {', '.join(missing)} "
                          "(source .env locally, or set as repo secrets in CI)")


def r2_endpoint():
    return f"https://{os.environ['CF_R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"


def s3_env():
    env = dict(os.environ)
    env['AWS_ACCESS_KEY_ID'] = os.environ['CF_R2_ACCESS_KEY_ID']
    env['AWS_SECRET_ACCESS_KEY'] = os.environ['CF_R2_SECRET_ACCESS_KEY']
    env['AWS_DEFAULT_REGION'] = 'auto'
    return env


def reg_dir_for(regulation):
    reg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations', regulation)
    if not os.path.isdir(reg_dir):
        raise SystemExit(f'Regulation directory not found: {reg_dir}')
    return reg_dir


def bucket_for(reg_dir):
    cfg_path = os.path.join(reg_dir, 'analyzer_config.yaml')
    cfg = yaml.safe_load(open(cfg_path)) or {}
    bucket = (cfg.get('state') or {}).get('bucket')
    if not bucket:
        bucket = ((cfg.get('report') or {}).get('full_export') or {}).get('bucket')
    if not bucket:
        raise SystemExit(f"No state.bucket (or report.full_export.bucket) in {cfg_path}")
    return bucket


def s3_cp(src, dst, env):
    """Run `aws s3 cp`, returning (ok, stderr). Never raises on a plain missing key."""
    proc = subprocess.run(['aws', 's3', 'cp', src, dst, '--endpoint-url', r2_endpoint()],
                           env=env, capture_output=True, text=True)
    return proc.returncode == 0, proc.stderr


def remote_name(fname):
    """Object name under state/<slug>/ for a synced artifact."""
    return f'{fname}.tar.gz' if fname == ATTACHMENT_CACHE else f'{fname}.gz'


def build_attachment_cache_tar(reg_dir, dest_tar):
    """Tar every `.extracted.txt` under the regulation's attachments/ dir.

    Returns the number of files archived (0 means there is nothing to push).
    Paths are stored relative to reg_dir so a pull unpacks straight back into place.
    """
    attachments = os.path.join(reg_dir, 'attachments')
    if not os.path.isdir(attachments):
        return 0

    count = 0
    with tarfile.open(dest_tar, 'w') as tar:
        for root, _dirs, files in os.walk(attachments):
            for name in sorted(files):
                if not name.endswith('.extracted.txt'):
                    continue
                full = os.path.join(root, name)
                # An empty cache file is not an answer (see process_attachments):
                # shipping one would teach another machine to stop retrying.
                if os.path.getsize(full) == 0:
                    continue
                tar.add(full, arcname=os.path.relpath(full, reg_dir))
                count += 1
    return count


def extract_attachment_cache_tar(src_tar, reg_dir):
    """Unpack a pulled cache tar into the regulation dir.

    A non-empty local extraction already on disk wins: it may have been re-run
    against a newer extractor, and the remote copy could predate that fix.
    Returns (written, kept).
    """
    written = kept = 0
    with tarfile.open(src_tar, 'r') as tar:
        members = []
        for m in tar.getmembers():
            if not (m.isfile() and m.name.endswith('.extracted.txt')):
                continue
            local = os.path.join(reg_dir, m.name)
            if os.path.exists(local) and os.path.getsize(local) > 0:
                kept += 1
                continue
            members.append(m)
        # filter='data' refuses absolute paths and paths escaping the destination.
        tar.extractall(reg_dir, members=members, filter='data')
        written = len(members)
    return written, kept


def s3_size(uri, env):
    """Size in bytes of an existing S3 object, or None if it isn't there."""
    proc = subprocess.run(['aws', 's3', 'ls', uri], env=env,
                          capture_output=True, text=True)
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    try:
        return int(proc.stdout.split()[2])
    except (IndexError, ValueError):
        return None


def pull(args):
    require_creds()
    reg_dir = reg_dir_for(args.regulation)
    bucket = bucket_for(reg_dir)
    env = s3_env()
    prefix = f'state/{args.regulation}'

    with tempfile.TemporaryDirectory() as tmp:
        for fname in args.files:
            key = f'{prefix}/{remote_name(fname)}'
            local_gz = os.path.join(tmp, fname + '.gz')
            ok, err = s3_cp(f's3://{bucket}/{key}', local_gz, env)
            if not ok:
                if 'does not exist' in err or 'Not Found' in err or '404' in err:
                    # The attachment cache is an optimisation, not state: a run
                    # without it re-downloads and re-extracts, which is slower and
                    # costs OCR calls but is still correct. Never fail the run over
                    # a missing one (it won't exist at all until the first push).
                    if fname == ATTACHMENT_CACHE:
                        print(f'  no remote {fname} yet — continuing without it')
                        continue
                    # Carrying on without state is how a CI run silently destroys
                    # it: with no source.csv the fetcher thinks the docket is
                    # empty, re-fetches from scratch, and the push at the end
                    # writes that stub over the real thing. Only tolerate a
                    # missing remote when a local copy already exists (a local
                    # run that has simply never pushed), or with --allow-missing
                    # to bootstrap a brand-new regulation.
                    local = os.path.join(reg_dir, fname)
                    if os.path.exists(local) and os.path.getsize(local) > 0:
                        print(f'  no remote state for {fname} — keeping local '
                              f'({os.path.getsize(local):,} bytes)')
                        continue
                    if args.allow_missing:
                        print(f'  no remote state for {fname} and no local copy — '
                              f'continuing because --allow-missing was given')
                        continue
                    raise SystemExit(
                        f'No remote state at s3://{bucket}/{key} and no local {fname}.\n'
                        f'Refusing to continue: downstream steps would treat the docket as\n'
                        f'empty, re-fetch everything, and overwrite good state with a stub.\n'
                        f'Seed it once with:  python sync_state.py push --regulation '
                        f'{args.regulation}\n'
                        f'or pass --allow-missing if this really is a new regulation.')
                raise SystemExit(f'Failed downloading s3://{bucket}/{key}:\n{err}')
            if os.path.getsize(local_gz) == 0:
                print(f'  WARNING: downloaded {fname}.gz is 0 bytes — refusing to use it, '
                      f'leaving local {fname} untouched')
                continue

            # Decompress to a staging path and only replace the real file once we know
            # the download is a complete, valid gzip stream with actual content — never
            # clobber good local state with a partial/corrupt one.
            staged = os.path.join(tmp, fname)
            try:
                with gzip.open(local_gz, 'rb') as fin, open(staged, 'wb') as fout:
                    shutil.copyfileobj(fin, fout)
            except (OSError, EOFError) as e:
                print(f'  WARNING: {fname}.gz failed to decompress ({e}) — leaving local '
                      f'{fname} untouched')
                continue
            if os.path.getsize(staged) == 0:
                print(f'  WARNING: decompressed {fname} is 0 bytes — leaving local {fname} untouched')
                continue

            if fname == ATTACHMENT_CACHE:
                written, kept = extract_attachment_cache_tar(staged, reg_dir)
                print(f'  pulled {fname} ({written:,} cached extractions written, '
                      f'{kept:,} local kept)')
                continue

            dest = os.path.join(reg_dir, fname)
            shutil.move(staged, dest)
            print(f'  pulled {fname} ({os.path.getsize(dest):,} bytes)')


def push(args):
    require_creds()
    reg_dir = reg_dir_for(args.regulation)
    bucket = bucket_for(reg_dir)
    env = s3_env()
    prefix = f'state/{args.regulation}'

    with tempfile.TemporaryDirectory() as tmp:
        for fname in args.files:
            if fname == ATTACHMENT_CACHE:
                src = os.path.join(tmp, f'{fname}.tar')
                archived = build_attachment_cache_tar(reg_dir, src)
                if not archived:
                    print(f'  no cached extractions to push — skipping {fname}')
                    continue
                print(f'  archiving {archived:,} cached extractions')
            else:
                src = os.path.join(reg_dir, fname)
            if not os.path.exists(src):
                print(f'  no local {fname} — skipping')
                continue
            if os.path.getsize(src) == 0:
                raise SystemExit(f'Local {fname} is 0 bytes — refusing to push empty '
                                  'state over good remote state')
            local_gz = os.path.join(tmp, fname + '.gz')
            with open(src, 'rb') as fin, gzip.open(local_gz, 'wb') as fout:
                shutil.copyfileobj(fin, fout)
            key = f'{prefix}/{remote_name(fname)}'

            # State only ever grows. A push that would shrink it sharply means
            # something upstream went wrong — a failed pull, a truncated fetch,
            # a half-written parquet — and pushing it destroys the only backup.
            remote_size = s3_size(f's3://{bucket}/{key}', env)
            new_size = os.path.getsize(local_gz)
            if remote_size and new_size < remote_size * 0.9 and not args.force:
                raise SystemExit(
                    f'Refusing to shrink remote {fname}: local is {new_size:,} bytes '
                    f'gzipped vs {remote_size:,} remote ({new_size / remote_size:.0%}).\n'
                    f'State should only grow. Check that the pull succeeded and the '
                    f'local file is complete.\nPass --force if the shrink is intended.')
            ok, err = s3_cp(local_gz, f's3://{bucket}/{key}', env)
            if not ok:
                raise SystemExit(f'Failed uploading s3://{bucket}/{key}:\n{err}')
            print(f'  pushed {fname} ({os.path.getsize(src):,} bytes -> s3://{bucket}/{key})')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    for name, fn in (('pull', pull), ('push', push)):
        p = sub.add_parser(name, help=f'{name} source.csv + full_run.parquet {"from" if name == "pull" else "to"} R2')
        p.add_argument('--regulation', required=True, help='Regulation slug under regulations/<slug>/')
        if name == 'pull':
            p.add_argument('--allow-missing', action='store_true',
                           help='continue when there is no remote state AND no local copy '
                                '(bootstrapping a brand-new regulation). Without it, that '
                                'combination is fatal, because downstream steps would treat '
                                'the docket as empty and overwrite good state with a stub.')
        else:
            p.add_argument('--force', action='store_true',
                           help='push even if it would shrink the remote copy by more than 10%%')
        p.add_argument('--files', nargs='+', choices=FILES, default=FILES,
                        help='Subset of files to sync (default: both). Lets a cheap check pull just '
                             'source.csv without paying for full_run.parquet too.')
        p.set_defaults(func=fn)
    args = ap.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
