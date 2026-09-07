# scd2-ingestion — E3

**Tier:** 3 🔥 · **Category:** E — Data engineering & DBs · **Wave:** 3

Root rules in `../GUIDELINES.md` apply. This file is project-specific only — keep it under 40 lines.

## What this is

An SCD Type 2 dimension merge built around one provable property: applying the same
change events in any order produces byte-identical history. The first design failed a
randomized property test 100/100 trials; the shipped design (buffer per entity, rebuild
from a full chronological sort on every event) passes it 500/500 at scale. See NOTES.md
for the bug and the redesign — that IS the project, not an incident report bolted on.

## Stack

Python 3.13 · DuckDB · pandas · argparse CLI. No services, no API keys, no env vars.

## Acceptance criteria

- [x] Incremental, idempotent ingestion (replaying an identical event is a no-op)
- [x] SCD Type 2 history (valid_from/valid_to/is_current), correct point-in-time queries
- [x] Late-arriving and out-of-order events handled correctly, not just accepted
- [x] Randomized order-independence proof (500 trials, 0 mismatches), not hand-picked cases
- [ ] Ship gate passes (`/ship`) — deliberately not run this session (built-not-shipped,
      per user request; see PROGRESS.md)

## Project-specific notes

- **`build_history` rebuilds an entity's ENTIRE version list from its FULL buffered
  event set on every new event** — this is deliberate, not an oversight. See scd2.py's
  module docstring and NOTES.md before "optimizing" this into an incremental patch; that
  was the first design, and it was provably order-dependent.
- **`AmbiguousRetractionError` no longer exists** (was in the v1 incremental design,
  removed in the rewrite). If you see it referenced anywhere outside git history, that's
  stale.
- pandas builds `valid_from`/`valid_to` as `object` dtype from `datetime.date` values;
  an all-null `valid_to` column (a single still-open version) breaks DuckDB's type
  inference unless explicitly cast with `pd.to_datetime` in `history_frame()` — already
  fixed, but a real bug if reintroduced. See NOTES.md.
- Local pytest needs `--basetemp=<scratchpad>/pt`; the sandbox blocks `%TEMP%`.
