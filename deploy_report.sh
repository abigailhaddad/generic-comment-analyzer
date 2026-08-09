#!/usr/bin/env bash
#
# Deploy a regulation's report to its Netlify site.
#
# Publishes ONLY the runtime files the report actually serves:
#   index.html, comment_rows/ + comment_detail/ (both sharded), and
#   read-the-rule.html (if built).
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
[ -d "$REG/comment_detail" ] || { echo "No comment_detail/ dir in $REG — regenerate the report first." >&2; exit 1; }
[ -d "$REG/comment_rows" ]   || { echo "No comment_rows/ dir in $REG — regenerate the report first." >&2; exit 1; }

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

# `netlify login` writes its config to a platform-specific path, so check all of
# them: macOS puts it under Library/Preferences, Linux under XDG config. Looking
# only at ~/.netlify/config.json rejected an authenticated macOS laptop and made
# every local deploy impossible.
netlify_logged_in() {
    [ -f "$HOME/.netlify/config.json" ] \
        || [ -f "$HOME/Library/Preferences/netlify/config.json" ] \
        || [ -f "${XDG_CONFIG_HOME:-$HOME/.config}/netlify/config.json" ]
}

if [ -z "${NETLIFY_AUTH_TOKEN:-}" ] && ! netlify_logged_in; then
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
cp -R "$REG/comment_detail" "$STAGE/comment_detail"
# The table rows. index.html is inert without these - it throws rather than
# rendering an empty table - so a deploy that omits them is a broken deploy.
cp -R "$REG/comment_rows" "$STAGE/comment_rows"
[ -f "$REG/read-the-rule.html" ] && cp "$REG/read-the-rule.html" "$STAGE/"

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

# Send the *.netlify.app hostname to the site's own custom domain, so the
# platform URL stops being the address people bookmark and cite. Netlify does
# NOT do this on its own: setting a primary custom domain only adds a
# `link: rel="canonical"` header — an SEO hint search engines may honour, which
# does nothing for a human who followed an old link. Only an explicit rule
# actually moves them.
#
# Both hostnames are read from the API rather than written here, because this
# script is regulation-agnostic and each regulation has its own site. No
# custom domain configured (the normal case for a site that hasn't been moved
# yet) means no rule is written and nothing changes.
#
# The source must be a full URL: a bare `/*` would match on the custom domain
# too and redirect it to itself forever. The trailing `!` forces the rule to
# win over index.html, which would otherwise be served first.
#
# Read into a file, not an interpolated string: the JSON contains backslash
# escapes a Python literal would re-interpret, corrupting it before json.loads.
#
# A failed lookup and a site with no custom domain both produce no rule, but
# only one of them is a problem — so they are reported differently rather than
# both printing "nothing to do". A publish is not worth aborting over a missing
# redirect, but it is worth saying loudly that the redirect is missing.
if netlify api getSite --data "{\"site_id\":\"$SITE_ID\"}" > "$STAGE/.site.json" 2>"$STAGE/.site.err"; then
    REDIRECT_RULES="$("$PYTHON" - "$STAGE/.site.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    site = json.load(f)
primary = site.get('custom_domain')
sub = site.get('name')
if primary and sub:
    for scheme in ('https', 'http'):
        print(f'{scheme}://{sub}.netlify.app/* https://{primary}/:splat 302!')
PY
)"
    if [ -n "$REDIRECT_RULES" ]; then
        printf '%s\n' "$REDIRECT_RULES" > "$STAGE/_redirects"
        echo "Adding _redirects (platform URL -> custom domain):"
        sed 's/^/  /' "$STAGE/_redirects"
    else
        echo "No custom domain on this site — skipping the _redirects rule."
    fi
else
    echo "WARNING: could not read site $SITE_ID from the Netlify API, so the" >&2
    echo "         netlify.app -> custom domain redirect was NOT written. The" >&2
    echo "         report still deploys; the old platform URL just keeps serving" >&2
    echo "         it instead of redirecting. API said:" >&2
    sed 's/^/           /' "$STAGE/.site.err" >&2
fi
rm -f "$STAGE/.site.json" "$STAGE/.site.err"

echo "Deploying '$SLUG' to Netlify site $SITE_ID"
echo "Publishing:"
ls -la "$STAGE"
netlify deploy --prod --dir="$STAGE" --site "$SITE_ID"
