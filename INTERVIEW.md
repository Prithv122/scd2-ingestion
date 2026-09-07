# Interview Prep — scd2-ingestion

**Five questions, five answers.** An unanswered question means this project is not shipped.

---

### Q1. Walk me through the architecture in 90 seconds.

_A:_ It's an SCD Type 2 dimension merge for a synthetic product catalog, built around
one property: replaying the same set of change events in a different order must never
produce a different final history. Every event carries two timestamps that matter:
`effective_at` (business time — when the change actually happened) and arrival order
(system time — when the pipeline happens to see it). A "late-arriving event" is simply
one whose `effective_at` is earlier than events already processed. The mechanism: every
event gets buffered per entity, and on every new event, that entity's *entire* version
history is rebuilt from a full sort of its buffered events by `(effective_at,
source_seq)`, followed by one linear forward scan. Arrival order never touches the merge
logic directly — it's erased by the sort before the scan ever runs. That gets persisted
to DuckDB, and a few analytical queries (point-in-time lookups, gap detection, category-
change counts) run in real SQL against the resulting table.

### Q2. Why does the same-input-different-order-same-output property matter enough to build a whole test around it?

_A:_ Because it's the actual failure mode a late-arriving-data pipeline has to survive,
and it's very easy to *think* you've handled it without actually proving it. I built a
first version — an incremental merge that patched an entity's existing version list in
place based on where a new event's timestamp fell relative to known intervals — and
hand-traced several late-arrival scenarios by hand, in both orders, and they matched. I
was confident enough to write the randomized test as what I expected to be a formality.
It failed 100 times out of 100 random trials on the first run. The bug was real and
structural: an incremental merge that decides a version boundary based on *today's*
partial state can permanently commit to the wrong boundary if a delete gets applied
before the upserts that should have carved out the interval it's shrinking. Hand-picked
test cases can't find that, because you only pick cases you can already reason about.

### Q3. Walk me through that bug in more detail — what was actually happening?

_A:_ Concretely: one entity had two separate delete/recreate cycles close together in
time. In one particular random arrival order, the second delete event got applied while
the interval structure around it was still coarse — several intervening upserts for
*that same entity* hadn't been applied yet, so what should have been three distinct
versions was still one merged blob. The delete correctly shrank whatever it found *at
that moment*, but the moment was wrong, because the algorithm had no way to know a more
detailed structure was coming later. Once committed, nothing revisited that decision.
The fix wasn't a patch — I replaced the whole merge with buffer-then-sort-then-scan,
which has no notion of "commit early" to get wrong: a full rebuild from chronological
order is trivially correct because a linear scan over sorted events is the same
operation an ordinary non-late-arriving pipeline would run anyway.

### Q4. How do you know the fix actually works, versus just working better than the bug you found?

_A:_ The same property test, at increasing scale, with zero mismatches: 100 trials on
the original failing dataset, then 500 trials on a larger synthetic catalog (50 products,
2 years, 4,115 events) for the version that shipped. I also added a second, harder
stress variant that maximizes delete/recreate density (25% chance of deletion per
eligible day instead of 3%) specifically because dense delete/recreate cycles were what
broke the first version — the property has to hold under the exact conditions that
found the bug, not just under the same conditions that happened not to trigger it before.
Beyond order-independence, `versions_per_entity()` catches a different failure mode
(silent duplication on replay — every entity landed at 39-99 versions, not obviously
inflated) and there's a separate idempotent-replay unit test (`test_identical_replay_is
_idempotent`) asserting that applying the exact same event twice produces exactly the
same history as applying it once.

### Q5. What's the weakest part of this, and what would break first under load?

_A:_ The complexity tradeoff, honestly stated: rebuilding an entity's entire history on
every new event is O(events-per-entity) work per event, so a naive one-at-a-time apply
loop over N events for one entity is O(N² log N) overall (the sort dominates). At this
project's scale — a genuinely "slowly changing" dimension, tens to under a hundred
versions per entity over two years — that's instant. It stops being free the moment an
entity changes daily for years rather than occasionally; at that point the design needs
a checkpoint (persist the derived version list periodically, only replay events newer
than the checkpoint) rather than a full rebuild every time. Second, the event buffer
lives in a Python dict in memory — a process restart loses it, so a production version
would need to persist the raw event log itself, not just the derived table, so a restart
can rebuild rather than lose state.

---

## 30-second pitch

An SCD Type 2 merge for late-arriving and out-of-order data, built around one provable
property: the same events applied in any order produce byte-identical history. The first
implementation didn't have that property — it failed a randomized 100-trial shuffle test
100 times out of 100 — and finding, diagnosing, and fixing that (by replacing incremental
interval-patching with buffer-then-sort-then-rebuild) is the actual engineering story of
this project, not an afterthought bolted onto a working demo.
