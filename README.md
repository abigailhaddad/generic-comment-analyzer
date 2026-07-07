# generic-comment-analyzer

A regulation-agnostic pipeline for analyzing public comments on U.S. federal
regulations (regulations.gov). It classifies each comment's position and concerns,
identifies who's commenting, detects form-letter campaigns, and produces an
interactive HTML report — plus an optional "Read the Rule" page that lets you read
the proposed rule section-by-section and see the comments citing each section.

Everything regulation-specific lives in one config file per regulation
(`analyzer_config.yaml`) — the code is generic.

## How it works

1. **Load** a regulations.gov bulk-export CSV (+ extract attachment text: PDF/DOCX/TXT, optional OpenAI image OCR).
2. **Deduplicate** identical comment text.
3. **Analyze** each unique comment with an LLM (OpenAI via LiteLLM) into a validated,
   config-defined Pydantic schema (positions/concerns, entity type, key quote, etc.).
4. **Verify** ambiguous stance/entity classifications with a second LLM pass.
5. **Detect campaigns** (MinHash near-duplicate clustering) and derive each campaign's stance.
6. **Report** to a self-contained `index.html` dashboard, and a `read-the-rule.html` page.

Runs are **resumable** (text-keyed checkpoint + parquet snapshots) so a restart never
loses work.

## The report

- Grouped stat cards (Overview / Position / Topics) with consistent, config-driven stance
  colors (Oppose / Support / Unclear).
- Concern, CFR-section, and campaign breakdowns as stacked Oppose/Support bars.
- An interactive comments table with a "+ Add filter" chip system and **shareable filter URLs**,
  CSV export, and detail modals.
- Campaigns labeled by their actual text; a per-section citation view.
- **Read the Rule**: the proposed rule's text by section, each colored by the stance of the
  comments citing it, with a link into the filtered analysis.

## Config drives everything

`analyzer_config.yaml` is the single source of truth. Key blocks:

- **`fields:`** — the analysis schema; each field (name + type + options) declared once drives
  the Pydantic model, the LLM prompt, and the frontend (columns/filters/cards). `source: regex`
  fields (e.g. CFR-section extraction) are computed at report time, no LLM.
- **`regex_flags:`** — boolean topic flags → stat cards + filters.
- **`second_pass:`** — verification model, triggers, and prompts.
- **`report:`** — `colors:` (the full palette — edit any color in one place) and display toggles.
- **`rule_text:`** — Federal Register document + CFR part for the Read-the-Rule page.

## Layout

```
regulations/<slug>/
  analyzer_config.yaml       # config: fields, flags, prompts, colors, rule_text   (committed)
  regulation_metadata.json   # name, docket id, agency                             (committed)
  source.csv                 # regulations.gov bulk export                         (local only)
  full_run.parquet           # analysis output                                     (local only)
  rule_sections.json         # parsed proposed-rule text                           (local only)
  index.html / read-the-rule.html   # the report pages                            (local only)
```

Only the two config files per regulation are version-controlled; large data stays local.

## Setup

```bash
python -m venv myenv && source myenv/bin/activate
uv pip install -r requirements.txt
echo "OPENAI_API_KEY=sk-..." > .env
```

## Run

```bash
# analyze
python pipeline.py --regulation <slug> --workers 24
# (optional) fetch the proposed-rule text for the Read-the-Rule page
python fetch_rule_text.py --regulation <slug>
```

The report is a large single-file HTML (~120 MB at ~47k comments), so deploy it to a static
host like Netlify rather than GitHub Pages. See `CLAUDE.md` for the full config schema and the
"add a new regulation" walkthrough.
