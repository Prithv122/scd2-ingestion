"""CLI smoke tests."""

from __future__ import annotations

from datetime import date

import pytest

from scd2_ingestion.cli import build_parser, main


def test_simulate_writes_a_duckdb_file(tmp_path, capsys) -> None:
    db_path = tmp_path / "catalog.duckdb"
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "simulate",
                "--entities",
                "5",
                "--start",
                "2024-01-01",
                "--end",
                "2024-03-31",
                "--db-path",
                str(db_path),
            ]
        )
    assert exc.value.code == 0
    assert db_path.exists()
    out = capsys.readouterr().out
    assert "rejected events: 0" in out


def test_verify_order_independence_reports_pass(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "verify-order-independence",
                "--entities",
                "5",
                "--start",
                "2024-01-01",
                "--end",
                "2024-02-29",
                "--trials",
                "5",
            ]
        )
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "PASS" in out
    assert "mismatches: 0" in out


def test_asof_subcommand_reports_no_version_before_the_dataset_starts(tmp_path, capsys) -> None:
    db_path = tmp_path / "catalog.duckdb"
    with pytest.raises(SystemExit):
        main(
            [
                "simulate",
                "--entities",
                "3",
                "--start",
                "2024-01-01",
                "--end",
                "2024-02-29",
                "--db-path",
                str(db_path),
            ]
        )
    capsys.readouterr()

    with pytest.raises(SystemExit) as exc:
        main(["asof", "sku-0000", "2020-01-01", "--db-path", str(db_path)])
    assert exc.value.code == 1
    assert "no version" in capsys.readouterr().out


def test_simulate_reports_rejected_events(tmp_path, capsys, monkeypatch) -> None:
    from scd2_ingestion.events import ChangeEvent

    orphan = [ChangeEvent.delete("ghost-sku", date(2024, 1, 1), 0)]
    monkeypatch.setattr("scd2_ingestion.cli.generate_events", lambda **kwargs: orphan)
    monkeypatch.setattr("scd2_ingestion.cli.shuffle_arrival_order", lambda events, seed: events)

    db_path = tmp_path / "catalog.duckdb"
    with pytest.raises(SystemExit) as exc:
        main(["simulate", "--db-path", str(db_path)])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "rejected events (1)" in out
    assert "ghost-sku" in out


def test_asof_subcommand_finds_a_known_version(tmp_path, capsys) -> None:
    from scd2_ingestion.events import ChangeEvent
    from scd2_ingestion.scd2 import Dimension
    from scd2_ingestion.warehouse import write_dimension

    db_path = tmp_path / "catalog.duckdb"
    dim = Dimension()
    dim.apply(ChangeEvent.upsert("sku-0000", date(2024, 1, 1), 0, price=10))
    write_dimension(dim, db_path)

    with pytest.raises(SystemExit) as exc:
        main(["asof", "sku-0000", "2024-06-01", "--db-path", str(db_path)])
    assert exc.value.code == 0
    assert "10" in capsys.readouterr().out


def test_parser_requires_a_subcommand() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
