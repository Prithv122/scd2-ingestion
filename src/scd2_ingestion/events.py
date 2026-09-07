"""Change events: the unit of input to the SCD2 merge in :mod:`scd2_ingestion.scd2`.

A change event carries two distinct notions of time, and keeping them distinct is the
entire point of this project:

* ``effective_at`` -- *business* time: when the change actually happened in the real
  world (e.g. "the price changed on March 3rd").
* the order events are *applied* in -- system/arrival time: when the pipeline happens to
  find out about it. A "late-arriving event" is simply one whose ``effective_at`` is
  earlier than events already applied, even though it is *applied* later.

``source_seq`` is a logical clock assigned by the (simulated) source system, independent
of arrival order. It exists so that two events sharing the exact same ``effective_at``
(a real possibility -- a source correcting its own mistake) have a well-defined,
order-independent tie-break: whichever has the higher ``source_seq`` wins, regardless of
which one the merge happens to see first.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum


class EventType(StrEnum):
    UPSERT = "upsert"
    DELETE = "delete"


@dataclass(frozen=True)
class ChangeEvent:
    entity_id: str
    effective_at: date
    event_type: EventType
    source_seq: int
    attributes: tuple[tuple[str, object], ...] = ()  # frozen for hashability in tests

    @property
    def attrs(self) -> dict[str, object]:
        return dict(self.attributes)

    @staticmethod
    def upsert(entity_id: str, effective_at: date, source_seq: int, **attrs: object) -> ChangeEvent:
        return ChangeEvent(
            entity_id=entity_id,
            effective_at=effective_at,
            event_type=EventType.UPSERT,
            source_seq=source_seq,
            attributes=tuple(sorted(attrs.items())),
        )

    @staticmethod
    def delete(entity_id: str, effective_at: date, source_seq: int) -> ChangeEvent:
        return ChangeEvent(
            entity_id=entity_id,
            effective_at=effective_at,
            event_type=EventType.DELETE,
            source_seq=source_seq,
            attributes=(),
        )


CATEGORIES = ["electronics", "home", "outdoors", "toys", "books", "grocery"]


def generate_events(
    *,
    n_entities: int = 30,
    start: date = date(2024, 1, 1),
    end: date = date(2024, 6, 30),
    seed: int = 0,
    change_prob: float = 0.15,
    delete_prob: float = 0.03,
) -> list[ChangeEvent]:
    """A synthetic but structurally realistic product-catalog change stream.

    Each entity gets a creation event, then for every subsequent day in the range an
    independent chance of a price/category change, and a smaller chance of being
    discontinued (and, after that, a further chance of being relaunched with fresh
    attributes -- the create/delete/recreate lifecycle the merge algorithm has to handle
    is a real pattern here, not a contrived one).

    ``source_seq`` is assigned in true chronological (creation) order, which is what
    makes it a valid tie-break independent of the *application* order tests will later
    shuffle these events into.
    """
    rng = random.Random(seed)
    events: list[ChangeEvent] = []
    seq = 0
    days = (end - start).days
    if days < 1:
        raise ValueError("end must be after start")

    for i in range(n_entities):
        entity_id = f"sku-{i:04d}"
        alive = False
        price = round(rng.uniform(5.0, 500.0), 2)
        category = rng.choice(CATEGORIES)
        creation_day = rng.randint(0, min(30, days))
        for day_offset in range(days + 1):
            current_date = start + timedelta(days=day_offset)
            if not alive:
                if day_offset == creation_day or (
                    day_offset > creation_day and rng.random() < change_prob / 4
                ):
                    events.append(
                        ChangeEvent.upsert(
                            entity_id,
                            current_date,
                            seq,
                            name=f"Product {i:04d}",
                            category=category,
                            price=price,
                        )
                    )
                    seq += 1
                    alive = True
                continue
            if rng.random() < delete_prob:
                events.append(ChangeEvent.delete(entity_id, current_date, seq))
                seq += 1
                alive = False
                continue
            if rng.random() < change_prob:
                if rng.random() < 0.3:
                    category = rng.choice(CATEGORIES)
                price = round(price * rng.uniform(0.85, 1.2), 2)
                events.append(
                    ChangeEvent.upsert(
                        entity_id,
                        current_date,
                        seq,
                        name=f"Product {i:04d}",
                        category=category,
                        price=price,
                    )
                )
                seq += 1
    return events


def shuffle_arrival_order(events: list[ChangeEvent], *, seed: int = 0) -> list[ChangeEvent]:
    """Return the same events in a random application order.

    ``effective_at`` and ``source_seq`` are untouched -- only the order the merge will
    *see* them in changes. This is the whole mechanism behind "late-arriving data" in
    this project's tests: the true history is fixed, only arrival order is randomized.
    """
    rng = random.Random(seed)
    shuffled = list(events)
    rng.shuffle(shuffled)
    return shuffled
