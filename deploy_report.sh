#!/usr/bin/env bash
#
# Deploy a regulation's report to its Netlify site.
#
# Publishes ONLY the runtime files the report actually serves:
#   index.html, comment_detail.json, and read-the-rule.html (if built).
# It never uploads source.csv, the parquet, the attachment cache, or any of
# the other regenerable/private files that live in the regulation directory.
#
# The Netlify site is taken from the regulation's own .netlify/state.json,
# so this stays generic across regulations. Link a site once with
# `netlify link` (run inside the regulation dir) before the first deploy.
#
# Usage:
#   ./deploy_report.sh <regulation-slug>
#   ./deploy_report.sh omb-financial-assistance
#
set -euo pipefail

SLUG="${1:?usage: deploy_report.sh <regulation-slug>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REG="$ROOT/regulations/$SLUG"

# R2 credentials for the full-export upload live in .env next to the code.
if [ -f "$ROOT/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$ROOT/.env"
    set +a
fi

[ -d "$REG" ] || { echo "No such regulation dir: $REG" >&2; exit 1; }
[ -f "$REG/index.html" ] || { echo "No index.html in $REG — generate the report first." >&2; exit 1; }
[ -f "$REG/comment_detail.json" ] || { echo "No comment_detail.json in $REG — regenerate the report first." >&2; exit 1; }

STATE="$REG/.netlify/state.json"
[ -f "$STATE" ] || { echo "No $STATE — link the site once: cd '$REG' && netlify link" >&2; exit 1; }
SITE_ID="$(python3 -c "import json; print(json.load(open('$STATE'))['siteId'])")"

# Full-dataset export → Cloudflare R2. Runs only when the regulation's config
# declares report.full_export.{bucket,key}; the report's "Download everything"
# link points at report.full_export.url. The file is far too large for Netlify,
# and it is rebuilt from the parquet here so it can never lag the report.
EXPORT_CFG="$(python3 - "$REG/analyzer_config.yaml" <<'PY'
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
    (cd "$REG" && "$ROOT/myenv/bin/python" "$ROOT/generate_report.py" --parquet full_run.parquet --export-csv comments_export.csv)
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
