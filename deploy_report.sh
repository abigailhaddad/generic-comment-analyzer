#!/usr/bin/env bash
#
# Deploy a regulation's report to its Cloudflare Pages project.
#
# Publishes ONLY the runtime files the report actually serves:
#   index.html, comment_rows/ + comment_detail/ (both sharded), and
#   read-the-rule.html / accuracy.html (if built).
# It never uploads source.csv, the parquet, the attachment cache, or any of
# the other regenerable/private files that live in the regulation directory.
#
# Pages, not Netlify: Netlify bills per deploy, and a report that publishes
# daily spends roughly half of a whole month's account-wide deploy budget on
# its own. Pages does not meter deploys at all. The old Netlify site keeps
# serving a static _redirects stub that points its netlify.app URL at the
# custom domain — it is never redeployed, so it costs nothing.
#
# The Pages project name comes from report.pages_project in the regulation's
# analyzer_config.yaml. It is not a secret and needs no local link step, so
# unlike the Netlify site id it works the same on a laptop and in a fresh CI
# checkout.
#
# Credentials (CF_R2_*, CLOUDFLARE_*) come from .env when present (local use);
# otherwise they must already be in the environment (CI use) — missing ones
# fail loudly below, naming what's missing.
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
[ -d "$REG/comment_detail" ] || { echo "No comment_detail/ dir in $REG — regenerate the report first." >&2; exit 1; }
[ -d "$REG/comment_rows" ]   || { echo "No comment_rows/ dir in $REG — regenerate the report first." >&2; exit 1; }

PAGES_PROJECT="$("$PYTHON" - "$REG/analyzer_config.yaml" <<'PY'
import sys, yaml
cfg = yaml.safe_load(open(sys.argv[1])) or {}
project = (cfg.get('report') or {}).get('pages_project')
if not project:
    sys.exit(1)
print(project)
PY
)" || { echo "No report.pages_project in $REG/analyzer_config.yaml — set it to the" \
             "Cloudflare Pages project name that serves this regulation." >&2; exit 1; }

# wrangler reads both of these from the environment. Checked here rather than
# left to wrangler because it fails late — after the R2 export has already been
# built and uploaded, which on this report is several minutes of work.
: "${CLOUDFLARE_API_TOKEN:?set CLOUDFLARE_API_TOKEN (account-scoped, Cloudflare Pages: Edit) to deploy}"
: "${CLOUDFLARE_ACCOUNT_ID:?set CLOUDFLARE_ACCOUNT_ID to deploy}"

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
cp -R "$REG/comment_detail" "$STAGE/comment_detail"
# The table rows. index.html is inert without these - it throws rather than
# rendering an empty table - so a deploy that omits them is a broken deploy.
cp -R "$REG/comment_rows" "$STAGE/comment_rows"
[ -f "$REG/read-the-rule.html" ] && cp "$REG/read-the-rule.html" "$STAGE/"
# The accuracy page. index.html links to it from a callout at the top whenever
# eval/scores.json exists, so omitting it here publishes a prominent 404 -
# exactly how read-the-rule broke on 2026-08-12.
[ -f "$REG/accuracy.html" ] && cp "$REG/accuracy.html" "$STAGE/"

# A real 404 page. Cloudflare Pages serves index.html for any unmatched path
# when no 404.html is present, so a typo'd URL returned the entire report with
# an HTTP 200 - a soft 404 that search engines index and that ships megabytes
# for a broken link. Netlify 404s by default; Pages does not.
cat > "$STAGE/404.html" <<'HTML'
<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Page not found</title>
<style>
 body{font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,sans-serif;
      margin:0;min-height:100vh;display:grid;place-items:center;padding:2rem;
      background:#fff;color:#111}
 @media (prefers-color-scheme:dark){body{background:#14161a;color:#e9e9ea}}
 .box{max-width:36rem;text-align:center}
 h1{font-size:1.3rem;margin:0 0 .5rem}
 p{margin:0 0 1.2rem;opacity:.75}
 a{color:inherit}
</style>
<div class="box">
  <h1>Page not found</h1>
  <p>That address does not match anything in this report.</p>
  <p><a href="/">Go to the report</a></p>
</div>
HTML

# No pages.dev -> custom domain redirect is written here, deliberately.
#
# The Netlify version of this script wrote a `_redirects` rule with a full URL
# as its source, so the platform hostname sent visitors to the custom domain.
# That does not port: Cloudflare Pages matches `_redirects` on the PATH only,
# and silently ignores a rule whose source is an absolute URL. Tested against
# this project — the rule uploads, and https://<project>.pages.dev/ still
# returns 200 with the report rather than a 302.
#
# The workaround is a Pages Function (functions/_middleware.js), which would run
# a Worker on every single request to the site just to redirect the small
# fraction that arrive on pages.dev. Not worth it here: unlike the netlify.app
# URL, this project's pages.dev hostname was created during the migration and
# has never been published or cited anywhere, so there is nothing to move off
# it. The legacy netlify.app URL is still redirected — by the static stub left
# behind on the old Netlify site, which is never redeployed.

echo "Deploying '$SLUG' to Cloudflare Pages project $PAGES_PROJECT"
echo "Publishing:"
ls -la "$STAGE"
npx --yes wrangler@4 pages deploy "$STAGE" \
    --project-name="$PAGES_PROJECT" \
    --branch=main \
    --commit-dirty=true
