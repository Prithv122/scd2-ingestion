"""Unit tests for scd2_ingestion.scd2: build_history and Dimension.

Every case here also has to survive tests/test_order_independence.py's randomized
shuffle -- these are the named, easy-to-read versions of specific behaviors; the
property test is what actually proves arrival order doesn't matter in general.
"""

from __future__ import annotations

from datetime import date

import pytest

from scd2_ingestion.events import ChangeEvent
from scd2_ingestion.scd2 import DeleteBeforeCreateError, Dimension, build_history


def U(entity_id: str, d: str, seq: int, **attrs) -> ChangeEvent:
    return ChangeEvent.upsert(entity_id, date.fromisoformat(d), seq, **attrs)


def D(entity_id: str, d: str, seq: int) -> ChangeEvent:
    return ChangeEvent.delete(entity_id, date.fromisoformat(d), seq)


def test_single_create_is_open_ended() -> None:
    history = build_history("e1", [U("e1", "2024-01-01", 0, price=10)])
    assert len(history) == 1
    v = history[0]
    assert v.valid_from == date(2024, 1, 1)
    assert v.valid_to is None
    assert v.is_current
    assert v.attributes == {"price": 10}


def test_create_then_update_closes_and_opens() -> None:
    events = [U("e1", "2024-01-01", 0, price=10), U("e1", "2024-02-01", 1, price=20)]
    history = build_history("e1", events)
    assert len(history) == 2
    assert history[0].valid_from == date(2024, 1, 1)
    assert history[0].valid_to == date(2024, 2, 1)
    assert history[0].attributes == {"price": 10}
    assert not history[0].deleted
    assert history[1].valid_from == date(2024, 2, 1)
    assert history[1].valid_to is None
    assert history[1].attributes == {"price": 20}


def test_update_order_in_the_input_list_does_not_matter() -> None:
    events = [U("e1", "2024-02-01", 1, price=20), U("e1", "2024-01-01", 0, price=10)]
    history = build_history("e1", events)
    assert [v.attributes["price"] for v in history] == [10, 20]


def test_create_then_delete_closes_with_deleted_flag() -> None:
    events = [U("e1", "2024-01-01", 0, price=10), D("e1", "2024-03-01", 1)]
    history = build_history("e1", events)
    assert len(history) == 1
    assert history[0].valid_to == date(2024, 3, 1)
    assert history[0].deleted is True
    assert not history[0].is_current


def test_delete_then_recreate_leaves_a_gap() -> None:
    events = [
        U("e1", "2024-01-01", 0, price=10),
        D("e1", "2024-02-01", 1),
        U("e1", "2024-03-01", 2, price=99),
    ]
    history = build_history("e1", events)
    assert len(history) == 2
    assert history[0].valid_to == date(2024, 2, 1)
    assert history[0].deleted
    assert history[1].valid_from == date(2024, 3, 1)
    assert history[1].valid_to is None
    assert history[1].attributes == {"price": 99}


def test_late_arriving_upsert_splits_an_existing_window() -> None:
    # True chronology: create@1, update@10 -- but the merge is given update@10 first.
    events = [U("e1", "2024-01-10", 1, price=20), U("e1", "2024-01-01", 0, price=10)]
    history = build_history("e1", events)
    assert len(history) == 2
    assert history[0].valid_from == date(2024, 1, 1)
    assert history[0].valid_to == date(2024, 1, 10)
    assert history[0].attributes == {"price": 10}
    assert history[1].valid_from == date(2024, 1, 10)
    assert history[1].valid_to is None
    assert history[1].attributes == {"price": 20}


def test_late_update_splits_the_correct_historical_window_not_the_current_one() -> None:
    events = [
        U("e1", "2024-01-01", 0, price=10),
        U("e1", "2024-06-01", 1, price=99),  # current, far in the future
        U("e1", "2024-03-01", 2, price=50),  # late: belongs strictly between the two
    ]
    history = build_history("e1", events)
    prices = [(v.valid_from.isoformat(), v.valid_to, v.attributes["price"]) for v in history]
    assert prices == [
        ("2024-01-01", date(2024, 3, 1), 10),
        ("2024-03-01", date(2024, 6, 1), 50),
        ("2024-06-01", None, 99),
    ]


def test_amendment_same_instant_higher_source_seq_wins_regardless_of_input_order() -> None:
    events_a = [U("e1", "2024-01-01", 0, price=10), U("e1", "2024-01-01", 5, price=999)]
    events_b = list(reversed(events_a))
    for events in (events_a, events_b):
        history = build_history("e1", events)
        assert len(history) == 1
        assert history[0].attributes == {"price": 999}
        assert history[0].source_seq == 5


def test_identical_replay_is_idempotent() -> None:
    event = U("e1", "2024-01-01", 0, price=10)
    history_once = build_history("e1", [event])
    history_twice = build_history("e1", [event, event])
    assert history_once == history_twice


def test_create_and_delete_at_the_same_instant_is_discarded() -> None:
    events = [U("e1", "2024-01-01", 0, price=10), D("e1", "2024-01-01", 1)]
    history = build_history("e1", events)
    assert history == []


def test_duplicate_delete_is_a_no_op() -> None:
    events = [
        U("e1", "2024-01-01", 0, price=10),
        D("e1", "2024-02-01", 1),
        D("e1", "2024-02-01", 2),
    ]
    history = build_history("e1", events)
    assert len(history) == 1
    assert history[0].valid_to == date(2024, 2, 1)


def test_delete_before_any_create_raises() -> None:
    with pytest.raises(DeleteBeforeCreateError):
        build_history("e1", [D("e1", "2024-01-01", 0)])


def test_dimension_apply_all_resolves_delete_before_create_within_one_batch() -> None:
    # The delete is listed before its create in the input -- apply_all buffers the
    # whole batch before rebuilding, so this must not be rejected.
    events = [D("e1", "2024-02-01", 1), U("e1", "2024-01-01", 0, price=10)]
    dim = Dimension()
    rejected = dim.apply_all(events)
    assert rejected == []
    history = dim.history("e1")
    assert len(history) == 1
    assert history[0].deleted


def test_dimension_apply_all_rejects_a_genuinely_orphaned_delete() -> None:
    events = [D("e1", "2024-02-01", 1)]  # no create anywhere for e1
    dim = Dimension()
    rejected = dim.apply_all(events)
    assert len(rejected) == 1
    assert rejected[0][0].entity_id == "e1"


def test_dimension_apply_self_heals_once_the_matching_create_arrives() -> None:
    dim = Dimension()
    dim.apply(D("e1", "2024-02-01", 1))  # arrives first, unresolvable yet
    assert dim.history("e1") == []
    dim.apply(U("e1", "2024-01-01", 0, price=10))  # now resolves
    history = dim.history("e1")
    assert len(history) == 1
    assert history[0].deleted


def test_as_of_returns_the_version_covering_that_date() -> None:
    dim = Dimension()
    dim.apply_all(
        [
            U("e1", "2024-01-01", 0, price=10),
            U("e1", "2024-06-01", 1, price=20),
        ]
    )
    assert dim.as_of("e1", date(2024, 3, 1)).attributes == {"price": 10}
    assert dim.as_of("e1", date(2024, 6, 1)).attributes == {"price": 20}
    assert dim.as_of("e1", date(2023, 1, 1)) is None


def test_current_state_only_includes_open_versions() -> None:
    dim = Dimension()
    dim.apply_all(
        [
            U("e1", "2024-01-01", 0, price=10),
            U("e2", "2024-01-01", 1, price=20),
            D("e2", "2024-02-01", 2),
        ]
    )
    current = dim.current_state()
    assert list(current["entity_id"]) == ["e1"]


def test_entity_ids_returns_a_sorted_list() -> None:
    dim = Dimension()
    dim.apply_all([U("e2", "2024-01-01", 0, price=1), U("e1", "2024-01-01", 1, price=2)])
    assert dim.entity_ids() == ["e1", "e2"]


def test_history_frame_is_sorted_by_entity_then_valid_from() -> None:
    dim = Dimension()
    dim.apply_all(
        [
            U("e2", "2024-01-01", 0, price=1),
            U("e1", "2024-02-01", 1, price=2),
            U("e1", "2024-01-01", 2, price=3),
        ]
    )
    frame = dim.history_frame()
    assert list(frame["entity_id"]) == ["e1", "e1", "e2"]
    # history_frame() casts to datetime64 (see its docstring) so DuckDB registration
    # works regardless of null patterns -- compare as dates, not as Timestamps.
    assert [d.date() for d in frame["valid_from"]] == [
        date(2024, 1, 1),
        date(2024, 2, 1),
        date(2024, 1, 1),
    ]
