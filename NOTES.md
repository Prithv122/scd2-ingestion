# Build Notes — scd2-ingestion

Working notes: what broke, what you tried, why you chose X over Y.
Not for recruiters — for you, six months from now, in an interview.

---

## Log

### 2026-09-08 — the incremental merge, and the bug that killed it

**Tried:** an incremental SCD2 merge. `apply_event(versions, event)` took an entity's
*existing* sorted, non-overlapping version list and one new event, located where the
event's `effective_at` fell — before everything, strictly inside an existing version's
window, or in a gap/after the terminal version — and patched the list accordingly:
prepend, split, or append. The elegant part (and I was pleased with it at the time): a
completely ordinary forward-in-time update and a genuinely late-arriving correction from
months ago are *the same operation* under this framing — both just split whatever
interval currently covers the event's timestamp. No special case for "normal" vs. "late."

Wrote 20-ish unit tests. Hand-traced several multi-event reorderings by hand (create
before delete before late-upsert, in both orders) and confirmed by hand that they
converged to the same result. Felt confident enough to write the randomized property
test as the "final proof."

**Broke:** the property test (`Dimension` applied the same ~600-event stream via 100
different random arrival-order shuffles) mismatched the baseline **on every single
trial.** Not intermittent — 100/100.

**Root cause, found by isolating one entity's 26 events and stepping through the exact
failing arrival order event-by-event:** `create@01-29 → delete@05-18 → upsert@06-17 →
delete@03-03 → ...` (26 events total, arrival order from a specific shuffle seed).
`delete@03-03` was applied while only two coarse versions existed so far: `(01-29, 05-18,
open)` and `(06-17, None)` — the several intervening upserts between 01-29 and 05-18
hadn't been applied yet. The delete correctly (for *that moment*) shrank the first
version to `(01-29, 03-03)`. But this entity's true history has a **second**
delete/recreate cycle later (`delete@04-21 → upsert@04-29`), and by the time those two
events were applied, the interval structure around them was built out of whatever
partial state existed *then* — which depended on what had already been applied, which
depended on arrival order. The 04-21 deletion ended up silently absorbed: the final
history showed one continuous `(03-02, 04-03, open)` version where there should have been
`(03-02, 04-03), (04-03, 04-21, deleted), (04-29, ...)`.

The structural problem: an *incremental* merge that decides what to do with a new event
by looking only at *today's* partial interval structure can commit to a boundary that
later turns out to be wrong once more events for the same entity are applied — and once
committed, nothing in the algorithm ever revisits that decision. This isn't a bug you can
patch with one more special case; it's inherent to deciding structure incrementally from
partial information.

**Fixed by throwing the incremental version away.** New design: buffer every event per
entity (don't discard it after use), and **rebuild that entity's entire version list from
scratch, from a full sort of everything currently buffered for it, on every new event.**
Sorting by `(effective_at, source_seq)` and doing one linear forward scan has no
"late-arriving" case left to get wrong, because a linear scan over chronologically
sorted events *is* just normal forward-only history construction — "late" was only ever
a property of *arrival* order, and the buffer-then-sort design makes arrival order
disappear before the merge logic ever runs. Reran the same property test: **0
mismatches across 100 trials on the first try**, then 500 trials at a larger scale (50
entities, 2 years, 4,115 events) for the version that shipped — still 0.

**Learned:** "I hand-traced a few cases and they worked" is not evidence an algorithm is
order-independent — it's evidence the cases I happened to pick weren't adversarial
enough. The property test isn't a formality here; it's the only reason I know the second
design is actually correct rather than merely "not obviously broken in the traces I
picked by hand." Also: when an incrementally-stateful algorithm needs a piece of
information from an event that hasn't arrived yet, the right fix is usually "stop being
incremental about that piece of state," not "add another special case."

### 2026-09-08 — a second real bug, found by a coverage-driven test

**Tried:** writing `test_asof_subcommand_finds_a_known_version` with a *deterministic*
single-event fixture (a lone open version, `valid_to=None`) instead of relying on the
random generator and accepting either CLI exit code, to close a coverage gap in
`cmd_asof`'s success branch.

**Broke:** `_duckdb.BinderException: Cannot compare values of type INTEGER and type
DATE`. `Dimension.history_frame()` built its `valid_from`/`valid_to` columns straight
from Python `datetime.date` objects; `pd.DataFrame(rows)` doesn't promote a column of
`date` objects to `datetime64` on its own, so both columns came through as pandas
`object` dtype. That had been silently fine in every prior test, because those fixtures
always mixed real dates with `None`, and DuckDB's DataFrame registration could infer
`DATE` from the non-null values it found. A dimension with a *single, still-open* row
has a `valid_to` column that is entirely `None` — nothing for DuckDB to infer a type
from — and it fell back to something DuckDB treats as `INTEGER`, which then can't be
compared against a `DATE`-typed query parameter.

**Fixed by** explicitly casting both columns with `pd.to_datetime(...)` inside
`history_frame()`, so the DataFrame hand it to DuckDB is always consistently typed
regardless of how many nulls happen to be in it.

**Learned:** a coverage number chasing a specific branch found a real bug that a
"looks representative" random-data test had been silently not exercising for two other
test files' worth of fixtures. The lesson isn't "always use fixtures over random data" —
it's that a single-row, all-null-in-one-column case is exactly the kind of edge a
coverage tool will point you at and a plausible-looking random dataset won't.

---

## Rejected approaches

| Approach | Why rejected |
|---|---|
| Incremental interval-patching merge (v1) | Provably order-dependent -- see the bug above. Kept the module docstring's account of it rather than deleting the history. |
| dbt's built-in `snapshot` macro | Assumes each check run sees strictly-forward-moving source data; that assumption is exactly what this project's late-arriving/out-of-order scenario violates. Using it would have answered a different, easier question. |
| Postgres for storage | The subject here is the merge algorithm's correctness, not a service; DuckDB gives real SQL with zero infrastructure, which is the right amount for what's being demonstrated. |
| "First event wins" / arrival-order tie-break for same-timestamp collisions | Arrival order is precisely the thing this project has to not depend on; a source-assigned logical clock (`source_seq`) is the only tie-break that stays order-independent. |

## Open questions

- [ ] The zero-duration discard (create and delete at the exact same `effective_at`)
      silently drops the version rather than recording a zero-width row. Whether a real
      downstream consumer would rather see an explicit "existed for an instant" marker
      is a product decision this project doesn't have a stakeholder to make.
- [ ] `build_history` is O(n log n) per rebuild (the sort) and gets called once per
      applied event when using `Dimension.apply` one at a time — so a naive loop over N
      events for one entity is O(N² log N) overall. Not a problem at this project's
      scale (see README §7 for where it would start to matter), but worth knowing the
      actual complexity rather than assuming "rebuild from scratch" is free.
