"""The project's actual evaluation method: does replaying the same event stream in a
different arrival order ever produce a different final history?

This is not a rhetorical question -- the first implementation of scd2.py answered "yes"
to this exact test, 50 times out of 50 random trials, on the very first run. See
scd2.py's module docstring and NOTES.md for the bug and the redesign that fixed it. This
file is the reason that redesign happened, and it stays in the suite so a future change
to the merge logic gets the same scrutiny.
"""

from __future__ import annotations

from datetime import date

import pytest

from scd2_ingestion.events import generate_events, shuffle_arrival_order
from scd2_ingestion.scd2 import Dimension


def _build(events, arrival_seed: int):
    dimension = Dimension()
    rejected = dimension.apply_all(shuffle_arrival_order(events, seed=arrival_seed))
    assert rejected == []
    return dimension.history_frame()


@pytest.mark.parametrize(
    ("n_entities", "start", "end", "seed", "n_trials"),
    [
        (30, date(2024, 1, 1), date(2024, 6, 30), 0, 100),
        (50, date(2023, 1, 1), date(2024, 12, 31), 42, 40),
    ],
)
def test_final_history_is_identical_across_random_arrival_orders(
    n_entities, start, end, seed, n_trials
) -> None:
    events = generate_events(n_entities=n_entities, start=start, end=end, seed=seed)
    baseline = _build(events, arrival_seed=0)

    mismatches = []
    for trial in range(1, n_trials + 1):
        frame = _build(events, arrival_seed=trial)
        if not frame.equals(baseline):
            mismatches.append(trial)

    assert mismatches == [], (
        f"{len(mismatches)}/{n_trials} arrival orders produced a different final "
        f"history than the baseline: trials {mismatches[:5]}..."
    )


def test_incremental_one_at_a_time_apply_matches_batch_apply_all() -> None:
    """The self-healing single-event `Dimension.apply` path (no batch buffering ahead
    of time) must converge to the same history as `apply_all`, given enough events."""
    events = generate_events(n_entities=15, seed=7)

    batch = Dimension()
    batch.apply_all(shuffle_arrival_order(events, seed=1))

    incremental = Dimension()
    for event in shuffle_arrival_order(events, seed=2):
        incremental.apply(event)

    assert incremental.history_frame().equals(batch.history_frame())


def test_order_independence_holds_even_with_dense_delete_recreate_cycles() -> None:
    """A harder stress case than the organic generator: every entity is deleted and
    recreated many times in a short window, maximizing the chance that a delete lands
    before the upsert that should bound it."""
    events = generate_events(
        n_entities=10,
        start=date(2024, 1, 1),
        end=date(2024, 3, 31),
        seed=1,
        change_prob=0.4,
        delete_prob=0.25,
    )
    baseline = _build(events, arrival_seed=0)
    for trial in range(1, 31):
        frame = _build(events, arrival_seed=trial)
        assert frame.equals(baseline), f"mismatch at trial {trial}"
