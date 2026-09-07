"""Command-line entry point: `scd2-ingestion <subcommand>`.

Every number in the README comes from one of these subcommands, run with a fixed seed.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime

import duckdb

from scd2_ingestion import warehouse
from scd2_ingestion.events import generate_events, shuffle_arrival_order
from scd2_ingestion.scd2 import Dimension


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def cmd_simulate(args: argparse.Namespace) -> int:
    events = generate_events(
        n_entities=args.entities,
        start=_parse_date(args.start),
        end=_parse_date(args.end),
        seed=args.seed,
    )
    arrival_order = shuffle_arrival_order(events, seed=args.arrival_seed)

    dimension = Dimension()
    rejected = dimension.apply_all(arrival_order)

    n_written = warehouse.write_dimension(dimension, args.db_path)

    print(f"generated {len(events)} events for {args.entities} entities")
    print(f"applied in a shuffled arrival order (seed={args.arrival_seed})")
    print(f"history rows written: {n_written} -> {args.db_path}")
    if rejected:
        print(f"rejected events ({len(rejected)}):")
        for event, exc in rejected:
            print(f"  {event.entity_id} @ {event.effective_at}: {exc}")
    else:
        print("rejected events: 0")
    return 0


def cmd_verify_order_independence(args: argparse.Namespace) -> int:
    events = generate_events(
        n_entities=args.entities,
        start=_parse_date(args.start),
        end=_parse_date(args.end),
        seed=args.seed,
    )

    baseline = Dimension()
    baseline.apply_all(shuffle_arrival_order(events, seed=0))
    baseline_frame = baseline.history_frame()

    mismatches = 0
    for trial in range(args.trials):
        dimension = Dimension()
        dimension.apply_all(shuffle_arrival_order(events, seed=trial + 1))
        frame = dimension.history_frame()
        if not frame.equals(baseline_frame):
            mismatches += 1
            print(f"trial {trial}: MISMATCH vs. baseline")

    print(f"{args.trials} random arrival orders tested against {len(events)} events")
    print(f"mismatches: {mismatches}")
    print("PASS" if mismatches == 0 else "FAIL")
    return 0 if mismatches == 0 else 1


def cmd_asof(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db_path, read_only=True)
    try:
        result = warehouse.as_of_sql(con, args.entity_id, _parse_date(args.date))
    finally:
        con.close()
    if result.empty:
        print(f"no version of {args.entity_id} was valid on {args.date}")
        return 1
    print(result.to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scd2-ingestion")
    sub = parser.add_subparsers(dest="command", required=True)

    p_sim = sub.add_parser("simulate", help="generate events, apply out of order, write to DuckDB")
    p_sim.add_argument("--entities", type=int, default=30)
    p_sim.add_argument("--start", default="2024-01-01")
    p_sim.add_argument("--end", default="2024-06-30")
    p_sim.add_argument("--seed", type=int, default=0)
    p_sim.add_argument("--arrival-seed", type=int, default=1)
    p_sim.add_argument("--db-path", default="data/catalog.duckdb")
    p_sim.set_defaults(func=cmd_simulate)

    p_verify = sub.add_parser(
        "verify-order-independence", help="apply the same events in many random orders"
    )
    p_verify.add_argument("--entities", type=int, default=30)
    p_verify.add_argument("--start", default="2024-01-01")
    p_verify.add_argument("--end", default="2024-06-30")
    p_verify.add_argument("--seed", type=int, default=0)
    p_verify.add_argument("--trials", type=int, default=50)
    p_verify.set_defaults(func=cmd_verify_order_independence)

    p_asof = sub.add_parser("asof", help="query one entity's state as of a given date")
    p_asof.add_argument("entity_id")
    p_asof.add_argument("date")
    p_asof.add_argument("--db-path", default="data/catalog.duckdb")
    p_asof.set_defaults(func=cmd_asof)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    sys.exit(args.func(args))
