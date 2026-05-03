# Working with this repo

## This is a public repo

Everything committed here is visible to anyone on the internet — code, comments,
commit messages, issues, anything. Keep all written content **generic**: refer to
broker formats, field names, codes, and parser logic, but never to specific personal
data.

Do not put any of the following into commits, comments, or docs:

- account numbers, account holder names, or any IDs from source CSVs
- specific trade values, dates, prices, quantities, fees, or P/L figures
- ticker positions or portfolio composition
- output snippets that contain real numbers

It is fine for a commit to imply *what kind of data* is being parsed (e.g. "IBKR
Forex partial-fill row") because that's a category, not personal information. It is
not fine to quote the actual row.

When verifying a fix locally, you may use real data freely — just describe the
result in generic terms when writing it up.

## What this project does

This tool converts CSV exports from stock brokers (Schwab, IBKR, eToro, IG) into the
import format used by summ.com. The output feeds directly into UK HMRC
tax calculations. **Incorrect output can produce incorrect tax filings, which carry
real financial penalties.** Treat every change as if it could cost the user money.

## Core principles

### Correctness over everything

This is tax software. There is no "good enough." If you can't verify a change is
correct, don't make it. Wrong is worse than incomplete — silently wrong is worst of
all.

### Small, targeted changes

One issue, one fix. Do not bundle "while I'm here" cleanups, refactors, or
opportunistic improvements with a bug fix. If a refactor is genuinely needed,
discuss it first as a separate change.

### Diagnose before editing

Before touching code:

1. State exactly what is broken, citing the specific row / line / value.
2. Explain *why* it's broken — the root cause, not the symptom.
3. Propose the minimum change that addresses the root cause.
4. Wait for confirmation before editing.

If you don't fully understand the cause, say so and ask. Do not guess. Do not invent
narratives ("this is probably the old format") without evidence in the actual data.

### No half-analysis

If a tool result, CSV row, or test outcome is ambiguous, dig until it isn't. Read
the surrounding rows. Read the function that produced it. Do not paper over
uncertainty with hedged language and a hopeful patch.

## Parser conventions

The parsers (`src/parsers/<broker>/`) are intentionally strict. They:

- **Fail loudly on anything unrecognised** (unknown codes, unexpected asset
  categories, unfamiliar action types). Silent mishandling of a single transaction
  can shift a year's tax bill — explosions are a feature.
- **Evolve forward, not backward.** When a broker changes their CSV format, the
  parser is updated to match the new format exactly. There is no backward-
  compatibility shimming. Old exports are expected to be re-run only against the
  parser version that handled their format at the time.
- **Use exact equality, not loose matching.** When a field has a known single
  literal value, check `field == 'Stocks'` rather than `field.startswith('Stocks')`.
  Loose matches hide format drift.

When extending a parser to handle a new code / category / action:

1. Find the actual row in the source CSV that triggered the issue. Quote it.
2. Explain what that field means in the broker's domain (commission code, partial
   fill, contingent value right, etc.).
3. Confirm whether the existing transformation is still numerically correct for the
   new case — many flags are informational and the math is unchanged. Some are not.
4. Make the smallest change that admits the new case.

## When in doubt

Stop and ask. The user prefers a five-minute discussion over a wrong commit.
