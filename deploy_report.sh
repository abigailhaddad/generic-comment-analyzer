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

[ -d "$REG" ] || { echo "No such regulation dir: $REG" >&2; exit 1; }
[ -f "$REG/index.html" ] || { echo "No index.html in $REG — generate the report first." >&2; exit 1; }
[ -f "$REG/comment_detail.json" ] || { echo "No comment_detail.json in $REG — regenerate the report first." >&2; exit 1; }

STATE="$REG/.netlify/state.json"
[ -f "$STATE" ] || { echo "No $STATE — link the site once: cd '$REG' && netlify link" >&2; exit 1; }
SITE_ID="$(python3 -c "import json; print(json.load(open('$STATE'))['siteId'])")"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
cp "$REG/index.html" "$STAGE/"
cp "$REG/comment_detail.json" "$STAGE/"
[ -f "$REG/read-the-rule.html" ] && cp "$REG/read-the-rule.html" "$STAGE/"

echo "Deploying '$SLUG' to Netlify site $SITE_ID"
echo "Publishing:"
ls -la "$STAGE"
netlify deploy --prod --dir="$STAGE" --site "$SITE_ID"
