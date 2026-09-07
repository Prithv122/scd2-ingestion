"""Unit tests for scd2_ingestion.events."""

from __future__ import annotations

from datetime import date

import pytest

from scd2_ingestion.events import ChangeEvent, EventType, generate_events, shuffle_arrival_order


def test_upsert_factory_sorts_attributes_for_hashability() -> None:
    e = ChangeEvent.upsert("e1", date(2024, 1, 1), 0, price=10, category="toys")
    assert e.attributes == (("category", "toys"), ("price", 10))
    assert e.attrs == {"category": "toys", "price": 10}
    assert e.event_type is EventType.UPSERT


def test_delete_factory_has_no_attributes() -> None:
    e = ChangeEvent.delete("e1", date(2024, 1, 1), 0)
    assert e.attrs == {}
    assert e.event_type is EventType.DELETE


def test_generate_events_every_entity_has_at_least_one_upsert() -> None:
    events = generate_events(n_entities=20, seed=3)
    creates = {e.entity_id for e in events if e.event_type is EventType.UPSERT}
    all_entities = {e.entity_id for e in events}
    assert creates == all_entities  # nobody appears only as a delete


def test_generate_events_source_seq_is_unique_and_monotonic_per_entity() -> None:
    events = generate_events(n_entities=10, seed=1)
    seqs = [e.source_seq for e in events]
    assert len(seqs) == len(set(seqs))  # globally unique
    for entity_id in {e.entity_id for e in events}:
        entity_events = [e for e in events if e.entity_id == entity_id]
        entity_seqs = [e.source_seq for e in entity_events]
        assert entity_seqs == sorted(entity_seqs)


def test_generate_events_is_deterministic_given_a_seed() -> None:
    a = generate_events(n_entities=10, seed=5)
    b = generate_events(n_entities=10, seed=5)
    assert a == b


def test_generate_events_rejects_a_non_positive_range() -> None:
    with pytest.raises(ValueError):
        generate_events(start=date(2024, 1, 10), end=date(2024, 1, 1))


def test_shuffle_arrival_order_is_a_permutation() -> None:
    events = generate_events(n_entities=10, seed=0)
    shuffled = shuffle_arrival_order(events, seed=1)
    assert sorted(shuffled, key=lambda e: e.source_seq) == sorted(
        events, key=lambda e: e.source_seq
    )
    assert shuffled != events  # a real shuffle, not a no-op (astronomically unlikely to tie)


def test_shuffle_arrival_order_is_deterministic_given_a_seed() -> None:
    events = generate_events(n_entities=10, seed=0)
    a = shuffle_arrival_order(events, seed=9)
    b = shuffle_arrival_order(events, seed=9)
    assert a == b
