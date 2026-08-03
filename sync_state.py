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
import sys
import tempfile

import yaml

FILES = ['source.csv', 'full_run.parquet']
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
            key = f'{prefix}/{fname}.gz'
            local_gz = os.path.join(tmp, fname + '.gz')
            ok, err = s3_cp(f's3://{bucket}/{key}', local_gz, env)
            if not ok:
                if 'does not exist' in err or 'Not Found' in err or '404' in err:
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
            key = f'{prefix}/{fname}.gz'

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
