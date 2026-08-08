# CLAUDE.md

## Project Overview

Regulation-agnostic analyzer for federal public comments. All regulation-specific
behavior — what the LLM extracts, how the report looks, which colors and fields
appear — is driven by a single per-regulation `analyzer_config.yaml`. The code is
generic; each regulation is a self-contained directory under `regulations/<slug>/`.

## Stack

- **LLM:** OpenAI via **LiteLLM** (`litellm.completion`) with Pydantic structured output
- **Models (defaults):** main `gpt-5.4-nano`; second-pass verify + attachment OCR `gpt-5.4-mini` (`FALLBACK_MODEL` also `gpt-5.4-mini`). Override main with `--model` (any LiteLLM string).
- **Python 3.14**, virtualenv in `myenv/`
- **Key deps:** litellm, openai, pydantic, pandas, datasketch, tqdm, jinja2, pyyaml

## Layout

```
generic-comment-analyzer/
├── pipeline.py, comment_analyzer.py, verify_stances.py, attachment_utils.py   # generic code
├── generate_report.py               # renders index.html (+ read-the-rule.html)
├── fetch_rule_text.py               # fetches proposed-rule text from Federal Register
├── report_template.html, rule_template.html   # Jinja templates (shared, code)
├── column_mapping.json              # regulations.gov column schema (shared)
└── regulations/
    └── <slug>/                      # one dir per regulation
        ├── analyzer_config.yaml         # committed: the single source of truth (see below)
        ├── regulation_metadata.json     # committed: name, docket id, agency
        ├── source.csv                   # local only (gitignored)
        ├── attachments/                 # local only (gitignored) — cached extractions
        ├── full_run.parquet             # local only (gitignored); back up to R2
        ├── rule_sections.json           # local only (gitignored) — parsed rule text
        ├── index.html                   # local only (gitignored) — the report
        └── read-the-rule.html           # local only (gitignored) — the rule page
```

Only `analyzer_config.yaml` + `regulation_metadata.json` are committed per regulation.
All large/regenerable data (CSV, attachments, parquet, `*.html`, `rule_sections.json`)
is gitignored. A private/sensitive regulation's directory can be kept entirely local (never committed) via `.git/info/exclude`.

## Key Files

- `comment_analyzer.py` — LiteLLM analyzer. Builds the Pydantic schema **and** the system prompt dynamically from the config `fields:` block (enum fields are constrained to config values). No hardcoded taxonomy.
- `pipeline.py` — CSV → attachments → dedup → LLM analysis → second-pass verification → campaign detection → parquet → report. `--regulation <slug>` chdirs into `regulations/<slug>/`. **Resume is text-keyed** (checkpoint every 50 comments + parquet snapshot every 250) so restarts don't lose work. **Duplicate Document IDs:** the regulations.gov bulk export sometimes assigns the *same Document ID to different comments* (OMB-2026-0034 had ~10). `read_comments_from_csv` keeps the first occurrence on the bare Document ID and disambiguates later ones as `<id>#<TrackingNumber>` (Tracking Number is unique per submission — verified 0 dupes — so the id is stable across re-runs; falls back to `#dupN` only if the Tracking Number is blank, which only happens on non-comment rows like the rule doc / empty submissions that get skipped anyway). It warns on any duplicates. Separately, the incremental-reuse loop drops any text-key that maps to *conflicting* stances rather than trust it. See `_stance_bucket` and the reuse block.
- **Pre-publish guards (`pipeline.py`, after state is saved).** Both bad publishes this project has had looked fine to the machine: totals stayed plausible, nothing raised, and a person caught it days later by reading a percentage. Two checks now stand between a run and the report. (1) *Credentials*: if any comment failed because the API rejected the request (no credits, bad key, rate limit), the run exits non-zero rather than publishing the hole — an exhausted key fails analysis *and* attachment OCR, so comments land with no stance or with their attached letter silently missing. (2) *Quality gate* (`check_batch_quality`): compares the comments that arrived this run against the corpus they join, the corpus against what the last run produced, and the share with no stance at all. Replayed against history it catches July's campaign flip (94.8% → 98.5% oppose) and August's credit outage (no-stance 0.4% → 2.4%). Both guards run **after** the parquet is written, so stopping costs only the publish; re-run with `--force` when a flagged change is real. Tune via `quality_gate:` in the config. The corpus-level checks measure against the **last published** figures, not the previous parquet: `record_data_changelog` stores the stance shares as `last_shares` in the committed `data_changelog.json`, and `published_baseline()` reads them back. That matters because CI pushes state to R2 even when a run fails, so a parquet baseline absorbs the very batch the gate just blocked and waves the same corruption through on the next run — `last_shares` only moves when something is genuinely published. Batch-level check 1 still uses the previous parquet, which is the right yardstick for "is this batch like the corpus it is joining". The changelog is only rewritten when the corpus grows (a dirty changelog is the workflow's unpublished signal), and only *after* the guards pass, so a stopped run never claims to have published.
- `verify_stances.py` — second-pass verification (stance/entity/state/political/cosigner). Prompts + triggers come from `second_pass` in the config; enum outputs are config-constrained. The `cosigner_span` task detects joint/coalition letters (phrase triggers + a structural repeated-short-line check), locates the signer-block span via verbatim quotes, and parses it into names/count in plain Python — no extra LLM call.
- `attachment_utils.py` — download/extract attachment text (PyMuPDF for PDFs — preserves visual reading order, unlike PyPDF2 which garbles multi-column layouts; docx via python-docx; caches to `.extracted.txt`). Image OCR uses OpenAI vision via LiteLLM (opt-in `--use-gemini`, a legacy flag name). `reextract_attachment_text()` re-runs extraction for one comment's cached PDF, refreshing the cache — used to pick up extractor fixes without a full re-run.
- `generate_report.py` — renders `index.html` from the parquet + config, and `read-the-rule.html` if `rule_sections.json` is present. Everything (columns, cards, filters, flag/section/campaign bars, colors) is derived from the config. `--export-csv <path>` instead writes a one-row-per-comment CSV: every original bulk-export column, then every derived covariate (analysis fields, one `<field>__<option>` TRUE/FALSE indicator per enum option, regex flags/values, dedup + campaign membership, attachment text). Columns come from the config and the data, never hardcoded. The join back to `source.csv` is by Document ID **narrowed by Tracking Number then exact comment text**, claiming each source row once — never a bare ID join (ids repeat; see below) — and raises rather than emitting an unmatched row.
- `fetch_rule_text.py` — fetches the proposed rule's XML from the Federal Register (per the config `rule_text` block) and parses it into `rule_sections.json` (per-section text).
- `check_new.py` — compares regulations.gov comment counts to the local CSV for a docket.
- `fetch_comments_api.py` — pulls comments not already in `source.csv` from the regulations.gov v4 API and appends them in the identical bulk-export CSV schema, so everything downstream (pipeline, report) is unchanged. **Never waits out a rate limit:** on 429 it retries briefly (in case of a burst limit), then stops, keeps everything already fetched, reports how many remain and exits 0 — the next scheduled run continues. Rows are appended **as they are fetched**, not accumulated and written at the end, so a run that stops mid-way loses nothing. **Listing is date-filtered by default:** it lists only comments posted since the newest row already in the CSV, because listing costs one call per 250 comments and walking a 61k-comment docket would burn ~245 of the key's ~500 calls/hour before fetching anything; filtered, it costs ~14. `--full-list` forces the whole walk (bootstrapping an empty CSV, or picking up comments backfilled with an older posted date). Replaces the manual bulk-download form, which cannot actually be submitted (its date-range instructions have no input field). Incremental (Document-ID-keyed against the existing CSV), append-only (a partial run is never destructive), and retries HTTP 429 with backoff. `--limit N` stops after N new comments — the API key is capped at roughly 500 "comment" calls/hour, so a large backlog is fetched over several runs; a capped run logs how many comments are still missing and exits 0 (not an error). `--dry-run` reports what's missing without fetching. See "Automated updates" below.
- `sync_state.py` — `pull`/`push` a regulation's gitignored state (`source.csv`, `full_run.parquet`, `attachment_cache`) to/from Cloudflare R2, gzipped, under `state/<slug>/`. This is what lets a clean CI checkout (which starts with none of them) resume from the last run. Bucket comes from the regulation's `state.bucket` (or `report.full_export.bucket`) config, never hardcoded. Refuses to let a 0-byte/corrupt download clobber good local state, or an empty local file overwrite good remote state. **`attachment_cache`** is a tar of every non-empty `attachments/**/*.extracted.txt` (~8 MB gzipped at 65k comments) and never the downloaded PDFs/images themselves (~450 MB). `process_attachments` reads a cached extraction *before* downloading, so a run that starts with the cache skips download, extraction and vision OCR entirely. Syncing it is also a correctness fix, not just a speed one: OCR output varies between runs, and a comment whose attachment text changes gets a new text-key and is re-analysed for no reason — and when the API is down, a run with no cache publishes those comments with their attached letter silently missing (2026-08-07). On pull, a non-empty local extraction always wins over the remote copy, since local may have been re-run against a newer extractor; a missing remote cache is never fatal (it is an optimisation, and does not exist until the first push).

## The config: `analyzer_config.yaml` (single source of truth)

- **`fields:`** — the analysis schema. Each field is declared once and drives the Pydantic model, the prompt, and the frontend:
  `name`, `type` (`multi_enum` | `single_enum` | `enum_or_empty` | `text` | `quote` | `short_text` | `multi_value`), `options`/`options_from` (`stances`|`entity_types`), `source` (default LLM, or `regex` with a `pattern` — extracted at report time, excluded from the LLM schema), `label`, `prompt`, `justifies` (for quotes), and `show: [cards, column, filter, section, modal]` (empty = extracted but not surfaced).
- **`stances:` / `entity_types:`** — the value lists referenced by `options_from`.
- **`regex_flags:`** — `name → {label, description, patterns}`; boolean per-comment flags → clickable stat cards + filters.
- **`quality_gate:`** — optional thresholds for the pre-publish check in `pipeline.py` (`min_batch`, `max_batch_shift_pp`, `max_corpus_shift_pp`, `max_no_stance_rise_pp`, `enabled`). Defaults live in `QUALITY_GATE_DEFAULTS` and suit a docket whose split has been stable; widen them for a docket that genuinely swings, rather than reaching for `--force` every run.
- **`second_pass:`** — `model`, `max_workers`, per-field triggers (`stance`, `entity_type`, `state`, `political_affiliation`), and required `prompts.stance` / `prompts.entity` (+ optional `prompts.state` / `.political` / `.cosigner`). Optional `cosigner_span.trigger_patterns` (regex list) opts a regulation into joint/coalition-letter detection (e.g. `omb-financial-assistance`); omitting the key disables it entirely.
- **`report:`** — display options: `full_export:` (`url` for the report's "Download everything" link, `bucket`/`key` for the R2 upload in `deploy_report.sh`), `netlify_site_id` (non-secret; lets `deploy_report.sh` deploy from CI where there's no local `.netlify/state.json`), `colors:` (full palette — `bg, surface, text, accent, oppose, support, unclear, mixed, highlight, border, …`; edit any color here, it flows everywhere), `show_state`, `show_political`.
- **`state:`** — `bucket` for `sync_state.py`'s R2 state backup (falls back to `report.full_export.bucket` if omitted).
- **`rule_text:`** — `federal_register_document` + `part` for `fetch_rule_text.py` / the Read-the-Rule page.

## Running

```bash
source myenv/bin/activate
# Smoke test:
python pipeline.py --regulation omb-financial-assistance --sample 5 --no-verify

# Full run (incremental + resumable; caffeinate for long runs):
caffeinate -i python pipeline.py --regulation omb-financial-assistance --workers 24

# Regenerate the report only (no re-analysis):
python generate_report.py --parquet regulations/omb-financial-assistance/full_run.parquet \
  --model gpt-5.4-nano --output regulations/omb-financial-assistance/index.html   # run from the reg dir

# Fetch the rule text (for the Read-the-Rule page):
python fetch_rule_text.py --regulation omb-financial-assistance

# Export one row per comment for analysis in R/Stata/Excel (run from the reg dir):
python ../../generate_report.py --parquet full_run.parquet --export-csv comments_export.csv
```

## Deploy

The report is a single self-contained HTML (~120 MB at ~47k comments — over GitHub's
100 MB limit), so host on **Netlify**, not GitHub Pages:
`netlify deploy --dir=<dir with index.html + read-the-rule.html> --prod --site <id>`.

`./deploy_report.sh <slug>` does the whole publish: when the config declares
`report.full_export.{bucket,key}` it rebuilds `comments_export.csv` from the parquet,
gzips it, uploads it to Cloudflare R2 (`aws s3 cp` against the R2 endpoint, credentials
`CF_R2_*` sourced from `.env`), then deploys the site to Netlify. The export is far too
big for Netlify, so the report links out to `report.full_export.url` — rebuilding it on
every deploy is what keeps that link from going stale. No `full_export` in the config
means the upload step is skipped. Credentials come from `.env` when it's present (local
use); when it's absent (CI), they must already be in the environment — anything missing
fails loudly, naming the variable. The Netlify site id comes from the regulation's
gitignored `.netlify/state.json` when present, falling back to the non-secret
`report.netlify_site_id` in `analyzer_config.yaml` (set once via `netlify link`, or by
reading the site id from the Netlify dashboard).

## Automated updates

`.github/workflows/update-regulation.yml` replaces the manual "download the bulk CSV,
run the pipeline, deploy" loop with scheduled runs against the regulations.gov v4 API.
It runs on two schedules, distinguished by `github.event.schedule`:

- **Catch-up ingest** (`23 13-21/2 * * *`, i.e. 9am/11am/1pm/3pm/5pm US Eastern): pull
  state from R2 → `fetch_comments_api.py --limit "$MAX_NEW"` → `pipeline.py` (incremental)
  → push state back to R2. No report, no deploy, no git commit — its only job is to keep
  chipping away at the regulations.gov rate limit (~500 "comment" calls/hour) so the
  backlog never falls far behind.
- **Daily publish** (`23 23 * * *`, 7pm ET, after that day's OMB posting is done):
  everything the catch-up run does, then commits `data_changelog.json`, renders the
  report (`generate_report.py`), and deploys (`deploy_report.sh` — Netlify + the full
  export to R2). Also runs on `workflow_dispatch` when `deploy` is left at its default
  `true`.

Both schedules land in the middle of the US day on purpose — nothing runs overnight
(8pm-8am ET).

**Making a drained run cheap.** At up to 6 runs/day, most runs should find nothing new.
The workflow orders steps so a drained run costs well under a minute: it installs only
PyYAML and pulls just `source.csv` (not the much bigger `full_run.parquet`) before
running `fetch_comments_api.py`, which is stdlib-only and itself doubles as the "is
there anything new" check — if it appends 0 rows, every later step (heavy `pip install
-r requirements.txt`, the `full_run.parquet` pull, the pipeline, the report, the
deploy) is skipped via `if: steps.fetch.outputs.added != '0'`. A run that legitimately
finds a big backlog is expected to sit through several 429 backoff windows rather than
give up after an hour (`fetch_comments_api.py` already retries), so the job
`timeout-minutes` is a generous 300; the rate limit and the schedule (not this timeout)
are what actually bound a run in practice.

**Concurrency.** A single `concurrency:` group keyed on the regulation means a catch-up
run and the publish run for the same regulation never overlap — important since both
pull/push the same R2 state, and the publish run also commits to the branch.

**The changelog commit.** `data_changelog.json` is a committed file (see Layout above),
so publishing it means the workflow pushes back to the branch it's running on
(`permissions: contents: write`, a `github-actions[bot]` commit identity). It only pushes
when the file actually changed, and rebases and retries once if `origin` moved in the
meantime (the concurrency group should make that rare, not impossible).

**Secrets** (repo Settings → Secrets and variables → Actions):

| Secret | Used for | How to get it |
|---|---|---|
| `REGULATIONS_API_KEY` | `fetch_comments_api.py` — the regulations.gov v4 API | Free, instant: https://open.gsa.gov/api/regulationsgov/ |
| `OPENAI_API_KEY` | `pipeline.py` — LLM analysis (LiteLLM) and attachment OCR | OpenAI platform dashboard |
| `CF_R2_ACCOUNT_ID` | `sync_state.py`, `deploy_report.sh` — Cloudflare R2 endpoint | Cloudflare dashboard → R2 → Overview (account id in the URL/sidebar) |
| `CF_R2_ACCESS_KEY_ID` | same | Cloudflare dashboard → R2 → Manage API tokens → create an R2 token with read+write on `regulations-comments` |
| `CF_R2_SECRET_ACCESS_KEY` | same | issued alongside the access key id above, shown once |
| `NETLIFY_AUTH_TOKEN` | `deploy_report.sh` → `netlify deploy` | Netlify dashboard → User settings → Applications → New access token |

No `NETLIFY_SITE_ID` secret is needed — the site id isn't a secret (it can't deploy
anything on its own without the auth token above), so it lives in
`report.netlify_site_id` in each regulation's `analyzer_config.yaml` instead, keeping
the code generic and the config the single source of truth.

**Per-run cap.** `max_new` (workflow input, default 5000) is a safety ceiling, not the
normal binding constraint — a normal day is on the order of ~1,000 new comments,
comfortably drained within the rate limit and the 300-minute timeout. It exists so a
genuine anomaly (e.g. a docket ID mixup) can't trigger an unbounded fetch.

## Environment

`.env` (next to the code): `OPENAI_API_KEY`, `REGULATIONS_API_KEY` (falls back to a
heavily-rate-limited `DEMO_KEY` if unset), `CF_R2_ACCOUNT_ID` / `CF_R2_ACCESS_KEY_ID` /
`CF_R2_SECRET_ACCESS_KEY` (R2 state sync + full-export upload). (`GEMINI_API_KEY`
optional/unused.) Netlify auth for local deploys is handled separately by `netlify
login`, not an env var — see Automated updates above for the CI equivalent.

## Adding a new regulation

1. `mkdir regulations/<slug>/`, drop the regulations.gov bulk CSV in as `source.csv`.
2. Write `regulation_metadata.json` (name, docket_id, agency, brief_description).
3. Write `analyzer_config.yaml`. Ground the `fields:` taxonomy (stances/entities) by
   sampling the actual comments first. Add `regex_flags`, `second_pass.prompts`,
   `report.colors`, and (optional) `rule_text`.
4. `python pipeline.py --regulation <slug> --workers 24` → then generate the report,
   optionally `fetch_rule_text.py`, and deploy.

## Conventions

- Fail fast, don't add fallbacks unless asked
- Prefer editing config over code — the tool is config-driven by design
- Use `myenv/` virtualenv, install with `uv pip install`; regenerate `requirements.txt` via `pip freeze`
- No client- or regulation-specific strings in committed code — keep it generic
- Commit regularly, don't create backup files
- **Never assume Document IDs are unique** — the govt export reuses them across different comments. Any recovery/rebuild of the parquet must join analysis to comments **by text, never by ID** (an ID-keyed join once stapled an oppose classification onto the support form-letter, and text-key reuse then flipped the whole ~2,400-comment support campaign to oppose → headline jumped 94%→98%). Acceptance tests after any such rebuild: headline ~94% oppose and every campaign near-100% stance-pure.
