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
- **`campaigns:`** — form-letter detection settings, including the two opt-in latent-semantic
  refinements described under [Campaign similarity](#campaign-similarity-optional-lsa-refinements).
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

# (optional) test your model on just a few comments and compile a full report
python pipeline.py --regulation <slug> --sample 10
python generate_report.py --parquet regulations/<slug>/sample_10.parquet --output regulations/<slug>/index-10.html
```

The report is a large single-file HTML (~120 MB at ~47k comments), so deploy it locally or to a static
host like Netlify rather than GitHub Pages.

## Infographic slides

`make_infographic.py` turns a finished analysis into a one-slide (16:9) summary that you can post or drop
into a deck. It reads the same `full_run.parquet` the report uses, so it works for any regulation with no
code changes. The default slide has the comment total, the share opposed (or supporting) as a dial, two word
clouds (oppose and support), the top topics, and comments per day. The rule ID and the comment date range
are in the header.

```bash
python make_infographic.py --regulation <slug>
```

This reads `regulations/<slug>/full_run.parquet` and writes `infographic.png` and `infographic.json` (the
numbers behind the slide) next to it. Use `--parquet` and `--out-dir` to read or write elsewhere; both
accept a plain or `.gz` parquet. The rule ID and title come from `regulation_metadata.json`.

### Choosing what goes on the slide

The count and dial sit at the left. To their right is a 2 x 2 grid of quadrants:

```
        middle      right
top      tl          tr
bottom   bl          br
```

Any figure can go in any quadrant:

| Figure | Shows |
|---|---|
| `cloud_oppose`, `cloud_support` | One word cloud per side. Words are the ones that set that side apart from the other. |
| `cloud_both` | One cloud for both sides, coloured by side. Each side keeps its own word order and the two get the same size at each rank, so neither owns all the large words. |
| `topics` | Top concerns, each bar split oppose / support. |
| `timeline` | Comments per day (per week on long dockets), stacked by position. |
| `entities` | Who is commenting, by entity type. |
| `state_map` | Tile map of comments by state: an oppose cell and a support cell per state, each shaded against its own highest state. Counts print when they fit. Needs the pipeline's `state_identified` field. |
| `backlog` | Comments received but not yet posted, over time. |
| `cumulative` | Cumulative comments, with a marker for the deadline and days left. |
| `none` | Leave the quadrant empty. |

```bash
# the default
--layout tl=cloud_oppose,bl=cloud_support,tr=topics,br=timeline

# one combined cloud; the freed quadrant gets the state map
--clouds combined

# pick any figures, in any quadrants (naming only some is fine)
--layout tl=cloud_both,bl=state_map,tr=topics,br=cumulative --deadline 2027-01-15

# 'top' / 'bottom' merge a row's two quadrants into one wide slot
--layout top=state_map,bl=cloud_both,br=timeline
```

A figure can appear only once. Naming a wide slot and one of its quadrants together is rejected with a clear
message. `--meter support` makes the dial show the support share instead of oppose.

### Look and feel

- **Colours:** five roles (`bg`, `ink`, `oppose`, `support`, `neutral`). Pick a built-in `--palette`
  (`okabe-ito` is the default, plus `plum-gold`, `dark`, `mono`) and override any role with
  `--color oppose=#RRGGBB`. The script warns when a pair would be hard to tell apart under common forms of colour
  blindness or has low contrast; `--strict` turns the warnings into errors.
- **Fonts:** `--font` for body text, `--display-font` for the large labels, `--cloud-font`, `--mono-font` for
  the header. Each takes an installed font name or a path to a `.ttf`/`.otf` file. A missing font is an error
  rather than a silent substitute.
- **Header:** `--title`, `--logo` (PNG or JPG) and `--website`.
- **Word clouds:** `--words`, `--contrast` (higher favours words distinctive to one side), `--stopwords`
  (comma-separated), and `--stopwords-file` (plain text, one or more words per line, `#` for comments). A trailing
  `*` is a prefix match: `oppos*` drops oppose, opposed, opposition. Words like these are already built in, so a
  docket's own noise words belong in its stop-word file. Form-letter text is stripped before counting, and each
  campaign counts once, so a template copied thousands of times does not fill the cloud.
- **Size and formats:** `--size 1920x1080`, `--formats png,svg,pdf`.

### A second slide

`--slides summary,detail` also writes `<name>_2.png`: topics by position as a heatmap, how often topics
co-occur, the posting backlog, and cumulative comments against a deadline. The deadline is never inferred.
Pass `--deadline YYYY-MM-DD`, and `--as-of` to fix the date used for "days left".

### A slide of one figure

Any figure can be a slide of its own, in the same frame and header as the others. Name it in `--slides`; it is
written as `<name>_<figure>.png`, with a `.json` of the numbers behind it.

```bash
python make_infographic.py --regulation <slug> --slides state_map
python make_infographic.py --regulation <slug> --slides summary,topics,cumulative --deadline 2027-01-15
```

The figures are the ones in the table above, plus three that exist only as slides: `meter` (the count and dial),
`heatmap` and `overlap` (the two topic figures from the second slide). `summary` and `detail` are the two slides
already described, so they mix freely with figure names. On its own, a figure gets the whole area under the
header, so its text and chart are drawn larger, a word cloud holds more words, and the state map prints both counts
in each tile and carries its own oppose / support key (in a layout, the word cloud carries that). Colours, fonts,
logo, stop words and the other options apply as usual.

### Using it on a regulation with different labels

The script reads the pipeline's stance tags by substring. If a regulation words them differently, tell it
which tags mean oppose or support, and which prefix marks topics:

```bash
python make_infographic.py --regulation <slug>     --oppose-tag "Stance: Against" --support-tag "Stance: For" --topic-prefix "Topic:"
```

### Keeping the choices in the regulation's config

Every option above can live in an `infographic:` block of `analyzer_config.yaml`; command-line flags override
it. Config values for files (logo, stop-word file, fonts) are relative to the regulation folder.

```yaml
infographic:
  palette: okabe-ito
  colors: {oppose: "#cf5d00", support: "#2072b1"}
  clouds: combined
  extra_figure: state_map
  layout: {br: timeline}
  meter: oppose
  deadline: "2027-01-15"
  website: example.org
  logo: logo.png
  stopwords_file: stopwords.txt
  oppose_tags: ["Stance: Against"]
  support_tags: ["Stance: For"]
```

Outputs hold real counts, so keep them out of version control (`regulations/<slug>/` is already ignored).
`python make_infographic.py --help` lists every option.

## Campaign similarity (optional LSA refinements)

Campaigns are found with **MinHash** on word 5-grams: fast, deterministic, and precise about
"is this the same form letter, word for word". That stays the default and decides who is *in* a
campaign. Two things it handles less well, which is what the optional latent-semantic (LSA:
TF-IDF followed by a truncated SVD) tools in `lsa_similarity.py` are for:

- A comment can land in more than one raw cluster. By default it goes to whichever cluster is
  processed last, and a campaign's stored `campaign_size` counts members that ended up elsewhere.
- One letter often shows up as several campaigns, because variants differ word for word (a
  different opening, a personalised paragraph).

Both options below are **off by default**. With them off, a comment gets no new keys and
`campaign_id`, `campaign_size` and the family fields come out exactly as before. They are set in the
`campaigns:` block of `analyzer_config.yaml`:

```yaml
campaigns:
  resolve_overlaps: true   # settle multi-cluster comments; store the true campaign_size
  letters: true            # add letter_id / letter_label: campaigns that are the same letter
  letters_threshold: 0.9   # cosine of campaign centres above which campaigns merge (default 0.9)
  lsa_k: 200               # SVD dimensions (default 200)
```

- **`resolve_overlaps`** gives a multi-cluster comment to the cluster whose canonical text it
  matches best word for word. LSA similarity only breaks near-ties; on its own it was no more
  reliable than the default rule at picking the better match. `campaign_size` becomes the number
  of comments the campaign actually holds. `campaign_id` is unchanged.
- **`letters`** groups campaigns whose LSA centres are close (average linkage, so a chain of
  similar campaigns does not merge its unrelated ends) and tags every member with `letter_id` (the
  id of the group's largest campaign) and `letter_label`. It never changes campaign or family
  membership. The report does not use these fields yet; they are columns in `full_run.parquet`.

If a corpus is too small or too uniform to fit a subspace, the pipeline logs a warning and falls
back to the default behaviour instead of failing.

### Campaign map

`campaign_map.py` draws one map of every campaign in a regulation, reading the same
`full_run.parquet` as the report (it only reads `campaign_id` / `family_id`):

```bash
python campaign_map.py --regulation <slug>
python campaign_map.py --regulation <slug> --k 200 --layout pca --formats png,svg
```

- Each campaign is a shaded disc (orange = opposes, blue = supports, by majority stance), with
  radius growing with the cube root of its members so small campaigns stay visible next to huge ones.
- Each *distinct* text in a campaign is a dot with area proportional to its exact copies; the
  farther a dot is from its disc's centre, the more that text deviates from the campaign's template.
- Distinct texts that are not in a campaign are small grey dots. Nearby means similar wording.
  Letter families are labelled.

It writes `campaign_map.png` (and `.svg`/`.pdf` if asked) and `campaign_map_campaigns.csv` (one row
per campaign: size, stance, how tight it is, and the terms that most define it) into the regulation
folder, or `--out-dir`. Like the other outputs, they reflect the comment data, so keep them local.

The 2-D layout is UMAP, an optional dependency; `--layout pca` needs nothing extra:

```bash
pip install -r requirements-optional.txt
```

Only campaign centres and non-campaign texts go through the layout. Members of a form letter are
nearly identical, and UMAP inflates tiny differences into fake clusters, so members fan out around
their campaign instead.

### Reading LSA similarity safely

- A short or idiosyncratic comment can keep very little of its text in the subspace. Its direction
  is then mostly noise, and scaling it to unit length makes a confident-looking cosine out of
  nothing. `lsa_similarity.retained_norm` reports how much was kept; gate on it.
- Too small a `k` produces near-1 cosines between campaigns that share no wording at all. Raise `k`
  until they disappear (the default of 200 is a starting point, not a rule).
- The subspace is fit on distinct texts weighted by the square root of their copies, so one huge
  campaign neither owns every axis nor drops out of the subspace.
