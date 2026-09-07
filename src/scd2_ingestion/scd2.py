"""Slowly Changing Dimension Type 2, order-independent by construction.

**This module went through a real redesign this session -- see NOTES.md for the full
story.** The first implementation tried to be clever: incrementally patch an entity's
existing version list in place, special-casing "does this event's effective_at fall
before/inside/after the known intervals." It passed every hand-written unit test. It
failed a randomized property test almost immediately: 50/50 trials mismatched a
baseline when the same events were replayed in different arrival orders. The root cause
was structural, not a typo -- an incremental merge that only ever looks at *today's*
partial interval structure can permanently commit to a wrong boundary when a delete is
applied before the upserts that should have carved out the interval it's shrinking.

The fix is to stop being clever. Every entity's full **history is rebuilt from scratch,
in true business-time order, every time a new event for it arrives.** Sort that entity's
buffered events by ``(effective_at, source_seq)`` and do one linear forward scan -- there
is no "late-arriving" special case left to get wrong, because a linear scan over
chronologically sorted events is just... normal, forward-only construction. Arrival
order stops mattering because the buffer, not the incremental version list, is the
source of truth, and a sort is order-independent by definition.

The one thing this trades away: this is O(events-per-entity) work on every new event for
that entity, not O(1). For a slowly-changing dimension (the whole reason it's called
that) an entity accumulates a handful to a few dozen versions over its lifetime, so this
is the right tradeoff -- see README.md section 7 for where it stops being one.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from datetime import date

import pandas as pd

from scd2_ingestion.events import ChangeEvent, EventType


class Scd2Error(Exception):
    """Base class for domain errors raised while building an entity's history."""


class DeleteBeforeCreateError(Scd2Error):
    """A delete event's effective_at predates every upsert known for the entity.

    Raised only when this is true across *every* event the Dimension has ever been
    given for that entity_id -- a delete that merely arrived before its create in a
    single batch is not an error, because the buffer is rebuilt from all known events,
    not just the ones seen so far in this call. See ``Dimension.apply``.
    """


@dataclass(frozen=True)
class Version:
    entity_id: str
    valid_from: date
    valid_to: date | None  # None means "open" / still current
    attributes: dict[str, object]
    deleted: bool  # True if this version's end represents a deletion, not a supersede
    source_seq: int  # source_seq of the event that produced these attributes

    @property
    def is_current(self) -> bool:
        return self.valid_to is None

    def covers(self, at: date) -> bool:
        if at < self.valid_from:
            return False
        return self.valid_to is None or at < self.valid_to


def build_history(entity_id: str, events: list[ChangeEvent]) -> list[Version]:
    """Rebuild one entity's complete version history from its complete event set.

    ``events`` may be in any order and may contain duplicates (idempotent replay) --
    the canonical order is derived here, not assumed from the input.
    """
    ordered = sorted(events, key=lambda e: (e.effective_at, e.source_seq))

    versions: list[Version] = []
    open_version: Version | None = None
    ever_created = False

    for event in ordered:
        t = event.effective_at

        if event.event_type is EventType.UPSERT:
            if open_version is not None and open_version.valid_from == t:
                # Same instant as the currently open version: an amendment (a source
                # correcting its own prior event), not a new interval boundary. The
                # (effective_at, source_seq) sort guarantees whichever event has the
                # higher source_seq is processed last and wins.
                open_version = Version(
                    entity_id=entity_id,
                    valid_from=t,
                    valid_to=None,
                    attributes=event.attrs,
                    deleted=False,
                    source_seq=event.source_seq,
                )
                continue
            if open_version is not None:
                versions.append(
                    Version(
                        entity_id=entity_id,
                        valid_from=open_version.valid_from,
                        valid_to=t,
                        attributes=open_version.attributes,
                        deleted=False,
                        source_seq=open_version.source_seq,
                    )
                )
            open_version = Version(
                entity_id=entity_id,
                valid_from=t,
                valid_to=None,
                attributes=event.attrs,
                deleted=False,
                source_seq=event.source_seq,
            )
            ever_created = True
            continue

        # DELETE
        if not ever_created:
            raise DeleteBeforeCreateError(
                f"{entity_id}: delete at {t} but no upsert precedes it in the known event set"
            )
        if open_version is None:
            continue  # already deleted as of t -- duplicate/no-op
        if open_version.valid_from == t:
            # Created and deleted at the exact same instant: zero-duration validity,
            # discarded rather than recorded as a meaningless zero-width row.
            open_version = None
            continue
        versions.append(
            Version(
                entity_id=entity_id,
                valid_from=open_version.valid_from,
                valid_to=t,
                attributes=open_version.attributes,
                deleted=True,
                source_seq=open_version.source_seq,
            )
        )
        open_version = None

    if open_version is not None:
        versions.append(open_version)

    return versions


class Dimension:
    """All entities' version histories, built from buffered events applied in any order.

    Buffers every event it is given, per entity, and rebuilds that entity's version
    list from the *complete* buffer on every call to :meth:`apply` -- see this module's
    docstring for why. A delete that arrives before its matching create in the same
    batch resolves itself as soon as the create is also applied; only a delete with
    genuinely no matching create anywhere in the buffer is a real error.
    """

    def __init__(self) -> None:
        self._events: dict[str, list[ChangeEvent]] = {}
        self._by_entity: dict[str, list[Version]] = {}

    def apply(self, event: ChangeEvent) -> None:
        self._events.setdefault(event.entity_id, []).append(event)
        # Not-yet-resolvable (DeleteBeforeCreateError) is deliberately swallowed here:
        # leave prior state untouched. It resolves itself once the matching create is
        # also applied, or surfaces via apply_all's final rejection pass if it never is.
        with contextlib.suppress(DeleteBeforeCreateError):
            self._by_entity[event.entity_id] = build_history(
                event.entity_id, self._events[event.entity_id]
            )

    def apply_all(self, events: list[ChangeEvent]) -> list[tuple[ChangeEvent, Scd2Error]]:
        """Apply a whole batch, then report genuinely unresolvable events.

        Buffers everything first, then attempts one rebuild per touched entity, so a
        delete-before-create within the same batch never depends on the batch's
        iteration order -- only entities that are *still* unresolvable after every event
        in the batch has been buffered are reported as rejected.
        """
        touched: set[str] = set()
        for event in events:
            self._events.setdefault(event.entity_id, []).append(event)
            touched.add(event.entity_id)

        rejected: list[tuple[ChangeEvent, Scd2Error]] = []
        for entity_id in touched:
            try:
                self._by_entity[entity_id] = build_history(entity_id, self._events[entity_id])
            except DeleteBeforeCreateError as exc:
                offending = min(
                    (e for e in self._events[entity_id] if e.event_type is EventType.DELETE),
                    key=lambda e: e.effective_at,
                )
                rejected.append((offending, exc))
        return rejected

    def history(self, entity_id: str) -> list[Version]:
        return list(self._by_entity.get(entity_id, []))

    def as_of(self, entity_id: str, at: date) -> Version | None:
        for v in self._by_entity.get(entity_id, []):
            if v.covers(at):
                return v
        return None

    def current_state(self) -> pd.DataFrame:
        rows = [
            {"entity_id": v.entity_id, "valid_from": v.valid_from, **v.attributes}
            for versions in self._by_entity.values()
            for v in versions
            if v.is_current
        ]
        return pd.DataFrame(rows)

    #: Present even when there is no history at all (e.g. every event in a batch was
    #: rejected) -- an empty ``pd.DataFrame(rows)`` with no rows has *no columns
    #: either*, which DuckDB's DataFrame registration rejects outright. See NOTES.md.
    _BASE_COLUMNS = ("entity_id", "valid_from", "valid_to", "is_current", "deleted", "source_seq")

    def history_frame(self) -> pd.DataFrame:
        rows = [
            {
                "entity_id": v.entity_id,
                "valid_from": v.valid_from,
                "valid_to": v.valid_to,
                "is_current": v.is_current,
                "deleted": v.deleted,
                "source_seq": v.source_seq,
                **v.attributes,
            }
            for versions in self._by_entity.values()
            for v in versions
        ]
        if not rows:
            return pd.DataFrame(columns=self._BASE_COLUMNS)
        df = pd.DataFrame(rows)
        # pandas builds valid_from/valid_to as `object` dtype from a column of
        # datetime.date values (it doesn't auto-promote to datetime64), and a column
        # that happens to be *all* None (e.g. a one-row, still-open dimension) has no
        # value at all to infer a type from. DuckDB's DataFrame registration then can't
        # tell these are dates, and a parameterized date comparison against them fails
        # with a binder error ("Cannot compare values of type INTEGER and type DATE").
        # Casting explicitly avoids depending on what values happen to be present.
        df["valid_from"] = pd.to_datetime(df["valid_from"])
        df["valid_to"] = pd.to_datetime(df["valid_to"])
        return df.sort_values(["entity_id", "valid_from"]).reset_index(drop=True)

    def entity_ids(self) -> list[str]:
        return sorted(self._by_entity.keys())
