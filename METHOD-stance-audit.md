# Auditing a stance label

## Why this exists

The pipeline decides a comment's position and, separately, writes a rationale
explaining it. Nothing compares the two. On 15 Aug 2026 a reader pointed out that
the Data Foundation's comment (`OMB-2026-0034-32514`) was labelled **Support** — it
asks OMB for more time to review the proposal and takes no side on it. The
classifier's own rationale said so: *"indicating concern about timing/administrative
feasibility rather than opposing or supporting the rule's substance."* The label and
the reasoning disagreed and nothing noticed.

The cause was the prompt, not the model. Two separate defects, found in sequence:

1. **`Unclear` was defined as junk only** — off-topic, a single word, incoherent,
   bare metadata. A comment that engaged with the subject but stated no position was
   *ineligible* for Unclear. With no opposition sentiment to detect, `Support` was the
   only option left. The model followed the prompt exactly.
2. **`Support` was defined too broadly, and contradicted the fix for (1).** It said to
   look for *"endorsement of stronger oversight against waste/fraud/abuse"* — which is
   what a grievance about misspent grant money looks like. Fixing (1) alone left this
   instruction in place, and being older and more specific, it won.

Fixing only the first got 76% agreement with hand labels, with **every** error in the
same direction. Fixing both got 88%, with the remaining disagreements going both ways
— the systematic bias was gone and only genuine edge cases were left.

## The tool

    python audit_stances.py --regulation <slug> --position support
    python audit_stances.py --regulation <slug> --position oppose --sample 2500 \
        --emit-ids /tmp/disputed.json

It reads the parquet, takes every comment labelled with that position, and asks a
model whether the comment text actually holds it — against a definition written to be
**stricter than the classifier's**, so it can disagree rather than agree by
construction. It only reports; correcting is a separate deliberate step.

Definitions live under `stance_audit:` in the regulation's `analyzer_config.yaml`, so
each docket audits against its own wording. `audit_stances.py` carries fallbacks.

Resume is automatic — judging is the only cost, so a re-run never pays twice.

## Correcting what it finds

`--emit-ids` writes the disputed ids. To re-run just those through the pipeline's own
second pass:

```python
from verify_stances import verify_stances
# clear verified_stance first — find_ambiguous_comments skips anything already
# carrying one, which is right for a normal run and wrong for a re-verification
for c in comments:
    c['analysis'].pop('verified_stance', None)
    c['analysis'].pop('stance_verification_reasoning', None)
verify_stances(comments)          # modifies in place
```

Then write back the **whole** analysis dict, not just `stances` — persisting stances
alone leaves `verified_stance` stale and the row self-contradictory. The verdicts also
land in `stance_verification_log.csv`, which is the authority if the two disagree.

## What it found (15 Aug 2026, OMB-2026-0034)

| | audited | disputed | rate |
|---|---:|---:|---|
| Support | 5,412 | 134 | 2.48% |
| Support, re-audited after tightening | 5,301 | 194 | 3.66% |
| Oppose (sample) | 2,500 | 29 | **1.16%** (CI 0.74–1.58) |

**161 comments moved off Support** across two correction rounds. Totals barely moved —
support 3.23% → 3.11%, oppose 96.28% → 96.29% — which is the point: this was wrong on
individual comments, not in aggregate, and individual comments are what people check.

**Oppose was left alone deliberately.** At 1.16% it is ordinary classifier noise with no
structural cause, and correcting it means a full 161k pass to move the topline by about
a tenth of a point.

## Validation

50 comments were hand-labelled from their text, blind to what the audit said, stratified
25/25 across flagged and passed. The audit scored **precision 90%, recall 100%** — it
over-flags, which is the safe direction, since flagged comments are re-verified rather
than auto-corrected. Both false positives were comments endorsing the rule's *goal*
without endorsing the rule, which is the same seam the whole exercise is about.

## Dead ends — don't repeat these

- **Auditing the rationale instead of the comment.** The first version asked only
  "does this rationale assert the position?". It catches self-contradiction and nothing
  else — a comment whose rationale confidently agrees with a wrong label sails through.
  Judge the comment text.
- **Deriving one position's audit from another's by find-and-replace.** A sed-built
  Oppose audit reported 6.02%; written properly it was 1.16%. Five times too high, and
  it would have justified an expensive and pointless re-run.
- **A free-text `reads_as` field with no definition of its values.** The model filled it
  inconsistently — 263 comments came back `reads_as: support` while `holds_position: true`,
  because it was answering "supports peer review" rather than "supports the rule". Define
  every enum value in the field description or don't ask for it.
- **A lexical pre-screen instead of an LLM pass.** Tested against the labelled Oppose
  sample, the best variant flagged 9.4% of comments to catch 41% of errors at 5%
  precision. At a ~1% base rate no cheap keyword screen has the lift; plenty of genuine
  opposition never uses the word "oppose" (*"Do not let political appointees determine
  what scientific research is funded"*).
