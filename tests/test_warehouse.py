"""Unit tests for scd2_ingestion.warehouse against a real (file-based) DuckDB."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from scd2_ingestion.events import ChangeEvent
from scd2_ingestion.scd2 import Dimension
from scd2_ingestion.warehouse import (
    as_of_sql,
    category_change_counts,
    entities_with_gaps,
    versions_per_entity,
    write_dimension,
)


def U(entity_id: str, d: str, seq: int, **attrs) -> ChangeEvent:
    return ChangeEvent.upsert(entity_id, date.fromisoformat(d), seq, **attrs)


def D(entity_id: str, d: str, seq: int) -> ChangeEvent:
    return ChangeEvent.delete(entity_id, date.fromisoformat(d), seq)


@pytest.fixture
def sample_dimension() -> Dimension:
    dim = Dimension()
    dim.apply_all(
        [
            U("e1", "2024-01-01", 0, category="toys", price=10),
            U("e1", "2024-03-01", 1, category="toys", price=20),
            U("e1", "2024-06-01", 2, category="books", price=15),
            U("e2", "2024-01-01", 3, category="home", price=50),
            D("e2", "2024-02-01", 4),
            U("e2", "2024-04-01", 5, category="home", price=55),
        ]
    )
    return dim


def test_write_dimension_creates_the_table(tmp_path, sample_dimension) -> None:
    db_path = tmp_path / "catalog.duckdb"
    n = write_dimension(sample_dimension, db_path)
    assert n == 5  # e1: 3 versions, e2: 2 versions
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        (count,) = con.execute("SELECT count(*) FROM dim_products_scd2").fetchone()
        assert count == 5
    finally:
        con.close()


def test_write_dimension_creates_missing_parent_directories(tmp_path, sample_dimension) -> None:
    # A clean clone has no `data/` directory -- it holds only a gitignored .duckdb file,
    # so git never creates it. write_dimension must not assume the parent dir exists.
    db_path = tmp_path / "data" / "catalog.duckdb"
    n = write_dimension(sample_dimension, db_path)
    assert n == 5
    assert db_path.exists()


def test_write_dimension_is_a_clean_replace_not_an_append(tmp_path, sample_dimension) -> None:
    db_path = tmp_path / "catalog.duckdb"
    write_dimension(sample_dimension, db_path)
    write_dimension(sample_dimension, db_path)  # simulate a re-run
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        (count,) = con.execute("SELECT count(*) FROM dim_products_scd2").fetchone()
        assert count == 5  # not 10
    finally:
        con.close()


def test_as_of_sql_reconstructs_point_in_time_state(tmp_path, sample_dimension) -> None:
    db_path = tmp_path / "catalog.duckdb"
    write_dimension(sample_dimension, db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        result = as_of_sql(con, "e1", date(2024, 2, 1))
        assert result.loc[0, "price"] == 10
        result = as_of_sql(con, "e1", date(2024, 4, 1))
        assert result.loc[0, "price"] == 20
        result = as_of_sql(con, "e2", date(2024, 3, 1))  # inside the gap
        assert result.empty
    finally:
        con.close()


def test_versions_per_entity_counts_correctly(tmp_path, sample_dimension) -> None:
    db_path = tmp_path / "catalog.duckdb"
    write_dimension(sample_dimension, db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        result = versions_per_entity(con).set_index("entity_id")["n_versions"].to_dict()
        assert result == {"e1": 3, "e2": 2}
    finally:
        con.close()


def test_entities_with_gaps_finds_the_deleted_and_recreated_entity(
    tmp_path, sample_dimension
) -> None:
    db_path = tmp_path / "catalog.duckdb"
    write_dimension(sample_dimension, db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        result = entities_with_gaps(con)
        assert list(result["entity_id"]) == ["e2"]
        # DuckDB's Python client returns DATE columns as pandas Timestamps via .df().
        assert result.loc[0, "gap_start"].date() == date(2024, 2, 1)
        assert result.loc[0, "gap_end"].date() == date(2024, 4, 1)
    finally:
        con.close()


def test_category_change_counts_only_counts_actual_changes(tmp_path, sample_dimension) -> None:
    db_path = tmp_path / "catalog.duckdb"
    write_dimension(sample_dimension, db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        result = category_change_counts(con).set_index("entity_id")["category_changes"].to_dict()
        # e1: toys -> toys (no change) -> books (1 change). e2: home -> home (0 changes).
        assert result == {"e1": 1}
    finally:
        con.close()
