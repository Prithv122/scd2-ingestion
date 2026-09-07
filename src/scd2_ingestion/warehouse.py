"""Persist a :class:`~scd2_ingestion.scd2.Dimension`'s history to DuckDB and answer the
analytical questions an SCD2 table exists to answer -- point-in-time lookups and
history-shape queries -- in real SQL rather than by re-walking Python objects.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from scd2_ingestion.scd2 import Dimension

HISTORY_TABLE = "dim_products_scd2"


def write_dimension(
    dimension: Dimension, db_path: str | Path, *, table: str = HISTORY_TABLE
) -> int:
    """Replace ``table`` in the DuckDB file at ``db_path`` with the current history."""
    frame = dimension.history_frame()
    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.register("history_frame", frame)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM history_frame")
        con.unregister("history_frame")
        return len(frame)
    finally:
        con.close()


def as_of_sql(
    con: duckdb.DuckDBPyConnection, entity_id: str, at: date, *, table: str = HISTORY_TABLE
) -> pd.DataFrame:
    """The version of one entity valid at a given date -- the query an SCD2 table
    exists to make possible: "what did we believe was true on this date," reconstructed
    entirely from history, not from whatever the current row happens to say.
    """
    return con.execute(
        f"""
        SELECT * FROM {table}
        WHERE entity_id = ?
          AND valid_from <= ?
          AND (valid_to IS NULL OR valid_to > ?)
        """,
        [entity_id, at, at],
    ).df()


def versions_per_entity(
    con: duckdb.DuckDBPyConnection, *, table: str = HISTORY_TABLE
) -> pd.DataFrame:
    """How many versions each entity accumulated -- a cheap sanity check that the merge
    isn't silently exploding history (e.g. every replay creating a duplicate version)."""
    return con.execute(
        f"""
        SELECT entity_id, count(*) AS n_versions
        FROM {table}
        GROUP BY entity_id
        ORDER BY n_versions DESC
        """
    ).df()


def entities_with_gaps(
    con: duckdb.DuckDBPyConnection, *, table: str = HISTORY_TABLE
) -> pd.DataFrame:
    """Entities that were deleted and later recreated -- a gap in valid_to/valid_from
    continuity, found in SQL with a self-join on adjacency rather than in Python."""
    return con.execute(
        f"""
        WITH ordered AS (
            SELECT
                entity_id,
                valid_from,
                valid_to,
                lead(valid_from) OVER (
                    PARTITION BY entity_id ORDER BY valid_from
                ) AS next_valid_from
            FROM {table}
        )
        SELECT entity_id, valid_to AS gap_start, next_valid_from AS gap_end
        FROM ordered
        WHERE valid_to IS NOT NULL AND next_valid_from IS NOT NULL AND valid_to < next_valid_from
        ORDER BY entity_id, gap_start
        """
    ).df()


def category_change_counts(
    con: duckdb.DuckDBPyConnection, *, table: str = HISTORY_TABLE
) -> pd.DataFrame:
    """How many times each entity's category changed across its recorded history."""
    return con.execute(
        f"""
        WITH ordered AS (
            SELECT
                entity_id,
                category,
                lag(category) OVER (PARTITION BY entity_id ORDER BY valid_from) AS prev_category
            FROM {table}
        )
        SELECT entity_id, count(*) AS category_changes
        FROM ordered
        WHERE prev_category IS NOT NULL AND category != prev_category
        GROUP BY entity_id
        HAVING count(*) > 0
        ORDER BY category_changes DESC
        """
    ).df()
