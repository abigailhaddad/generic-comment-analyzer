#!/usr/bin/env bash
#
# Deploy a regulation's report to its Netlify site.
#
# Publishes ONLY the runtime files the report actually serves:
#   index.html, comment_detail.json, and read-the-rule.html (if built).
# It never uploads source.csv, the parquet, the attachment cache, or any of
# the other regenerable/private files that live in the regulation directory.
#
# The Netlify site id normally comes from the regulation's own gitignored
# .netlify/state.json (linked once locally with `netlify link`, run inside the
# regulation dir). Where that file doesn't exist — a fresh CI checkout — it
# falls back to the non-secret report.netlify_site_id in analyzer_config.yaml.
#
# Credentials (CF_R2_*, NETLIFY_AUTH_TOKEN) come from .env when present
# (local use); otherwise they must already be in the environment (CI use) —
# missing ones fail loudly below, naming what's missing.
#
# Usage:
#   ./deploy_report.sh <regulation-slug>
#   ./deploy_report.sh omb-financial-assistance
#
set -euo pipefail

SLUG="${1:?usage: deploy_report.sh <regulation-slug>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REG="$ROOT/regulations/$SLUG"

# Credentials live in .env next to the code for local use; in CI they're
# already exported into the environment, so a missing .env is not an error.
if [ -f "$ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$ROOT/.env"
    set +a
fi

# Prefer the venv's python (has PyYAML from requirements.txt); fall back to
# system python3 for CI runs that install straight into the runner's python.
PYTHON="$ROOT/myenv/bin/python"
[ -x "$PYTHON" ] || PYTHON="python3"

[ -d "$REG" ] || { echo "No such regulation dir: $REG" >&2; exit 1; }
[ -f "$REG/index.html" ] || { echo "No index.html in $REG — generate the report first." >&2; exit 1; }
[ -f "$REG/comment_detail.json" ] || { echo "No comment_detail.json in $REG — regenerate the report first." >&2; exit 1; }

STATE="$REG/.netlify/state.json"
if [ -f "$STATE" ]; then
    SITE_ID="$("$PYTHON" -c "import json; print(json.load(open('$STATE'))['siteId'])")"
else
    SITE_ID="$("$PYTHON" - "$REG/analyzer_config.yaml" <<'PY'
import sys, yaml
cfg = yaml.safe_load(open(sys.argv[1])) or {}
site_id = (cfg.get('report') or {}).get('netlify_site_id')
if not site_id:
    sys.exit(1)
print(site_id)
PY
)" || { echo "No $STATE and no report.netlify_site_id in $REG/analyzer_config.yaml — " \
             "link the site once (cd '$REG' && netlify link) or add netlify_site_id to the config." >&2; exit 1; }
fi

if [ -z "${NETLIFY_AUTH_TOKEN:-}" ] && [ ! -f "$HOME/.netlify/config.json" ]; then
    echo "No Netlify auth found: set NETLIFY_AUTH_TOKEN (CI) or run 'netlify login' (local)." >&2
    exit 1
fi

# Full-dataset export → Cloudflare R2. Runs only when the regulation's config
# declares report.full_export.{bucket,key}; the report's "Download everything"
# link points at report.full_export.url. The file is far too large for Netlify,
# and it is rebuilt from the parquet here so it can never lag the report.
EXPORT_CFG="$("$PYTHON" - "$REG/analyzer_config.yaml" <<'PY'
import sys, yaml
cfg = (yaml.safe_load(open(sys.argv[1])) or {}).get('report', {}).get('full_export') or {}
print(f"{cfg.get('bucket','')}\t{cfg.get('key','')}")
PY
)"
EXPORT_BUCKET="$(printf '%s' "$EXPORT_CFG" | cut -f1)"
EXPORT_KEY="$(printf '%s' "$EXPORT_CFG" | cut -f2)"

if [ -n "$EXPORT_BUCKET" ] && [ -n "$EXPORT_KEY" ]; then
    : "${CF_R2_ACCOUNT_ID:?set CF_R2_ACCOUNT_ID (see .env) to upload the full export}"
    : "${CF_R2_ACCESS_KEY_ID:?set CF_R2_ACCESS_KEY_ID (see .env) to upload the full export}"
    : "${CF_R2_SECRET_ACCESS_KEY:?set CF_R2_SECRET_ACCESS_KEY (see .env) to upload the full export}"

    EXPORT_CSV="$REG/comments_export.csv"
    echo "Building full export: $EXPORT_CSV"
    (cd "$REG" && "$PYTHON" "$ROOT/generate_report.py" --parquet full_run.parquet --export-csv comments_export.csv)
    echo "Compressing..."
    gzip -f -k "$EXPORT_CSV"

    echo "Uploading to r2://$EXPORT_BUCKET/$EXPORT_KEY"
    AWS_ACCESS_KEY_ID="$CF_R2_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$CF_R2_SECRET_ACCESS_KEY" \
    AWS_DEFAULT_REGION=auto \
    aws s3 cp "$EXPORT_CSV.gz" "s3://$EXPORT_BUCKET/$EXPORT_KEY" \
        --endpoint-url "https://$CF_R2_ACCOUNT_ID.r2.cloudflarestorage.com" \
        --content-type application/gzip
else
    echo "No report.full_export in the config — skipping the R2 export upload."
fi

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
cp "$REG/index.html" "$STAGE/"
cp "$REG/comment_detail.json" "$STAGE/"
[ -f "$REG/read-the-rule.html" ] && cp "$REG/read-the-rule.html" "$STAGE/"

echo "Deploying '$SLUG' to Netlify site $SITE_ID"
echo "Publishing:"
ls -la "$STAGE"
netlify deploy --prod --dir="$STAGE" --site "$SITE_ID"
