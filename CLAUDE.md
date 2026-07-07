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
- `pipeline.py` — CSV → attachments → dedup → LLM analysis → second-pass verification → campaign detection → parquet → report. `--regulation <slug>` chdirs into `regulations/<slug>/`. **Resume is text-keyed** (checkpoint every 50 comments + parquet snapshot every 250) so restarts don't lose work.
- `verify_stances.py` — second-pass verification (stance/entity/state/political/cosigner). Prompts + triggers come from `second_pass` in the config; enum outputs are config-constrained. The `cosigner_span` task detects joint/coalition letters (phrase triggers + a structural repeated-short-line check), locates the signer-block span via verbatim quotes, and parses it into names/count in plain Python — no extra LLM call.
- `attachment_utils.py` — download/extract attachment text (PyMuPDF for PDFs — preserves visual reading order, unlike PyPDF2 which garbles multi-column layouts; docx via python-docx; caches to `.extracted.txt`). Image OCR uses OpenAI vision via LiteLLM (opt-in `--use-gemini`, a legacy flag name). `reextract_attachment_text()` re-runs extraction for one comment's cached PDF, refreshing the cache — used to pick up extractor fixes without a full re-run.
- `generate_report.py` — renders `index.html` from the parquet + config, and `read-the-rule.html` if `rule_sections.json` is present. Everything (columns, cards, filters, flag/section/campaign bars, colors) is derived from the config.
- `fetch_rule_text.py` — fetches the proposed rule's XML from the Federal Register (per the config `rule_text` block) and parses it into `rule_sections.json` (per-section text).
- `check_new.py` — compares regulations.gov comment counts to the local CSV for a docket.

## The config: `analyzer_config.yaml` (single source of truth)

- **`fields:`** — the analysis schema. Each field is declared once and drives the Pydantic model, the prompt, and the frontend:
  `name`, `type` (`multi_enum` | `single_enum` | `enum_or_empty` | `text` | `quote` | `short_text` | `multi_value`), `options`/`options_from` (`stances`|`entity_types`), `source` (default LLM, or `regex` with a `pattern` — extracted at report time, excluded from the LLM schema), `label`, `prompt`, `justifies` (for quotes), and `show: [cards, column, filter, section, modal]` (empty = extracted but not surfaced).
- **`stances:` / `entity_types:`** — the value lists referenced by `options_from`.
- **`regex_flags:`** — `name → {label, description, patterns}`; boolean per-comment flags → clickable stat cards + filters.
- **`second_pass:`** — `model`, `max_workers`, per-field triggers (`stance`, `entity_type`, `state`, `political_affiliation`), and required `prompts.stance` / `prompts.entity` (+ optional `prompts.state` / `.political` / `.cosigner`). Optional `cosigner_span.trigger_patterns` (regex list) opts a regulation into joint/coalition-letter detection (e.g. `omb-financial-assistance`); omitting the key disables it entirely.
- **`report:`** — display options: `colors:` (full palette — `bg, surface, text, accent, oppose, support, unclear, mixed, highlight, border, …`; edit any color here, it flows everywhere), `show_state`, `show_political`.
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
```

## Deploy

The report is a single self-contained HTML (~120 MB at ~47k comments — over GitHub's
100 MB limit), so host on **Netlify**, not GitHub Pages:
`netlify deploy --dir=<dir with index.html + read-the-rule.html> --prod --site <id>`.

## Environment

`.env` (next to the code): `OPENAI_API_KEY`. (`GEMINI_API_KEY` optional/unused.)

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
