"""
Run the proven post-ingestion stages for one historical Kafka replay.

This script does not send data to Kafka and does not consume Kafka directly.
It starts after Spark has already written the account-event Parquet log.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run_module(module_name: str, *arguments: str) -> None:
    """Run one existing pipeline module and stop immediately on failure."""
    command = [sys.executable, "-m", module_name, *arguments]

    print("\n" + "=" * 80)
    print("Running:", " ".join(command))
    print("=" * 80)

    subprocess.run(command, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize all graph features and package one replay window."
        )
    )
    parser.add_argument(
        "--account-events-dir",
        type=Path,
        required=True,
        help="Parquet account-event log produced by Spark Structured Streaming.",
    )
    parser.add_argument(
        "--static-output-dir",
        type=Path,
        required=True,
        help=(
            "Static baseline output used for Bank and Entity metadata."
        ),
    )
    parser.add_argument(
        "--run-name",
        required=True,
        help="Unique name for this replay run, for example replay_1000.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/streaming/runs"),
        help="Parent directory for this run's feature and graph outputs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    account_events_dir = args.account_events_dir
    static_output_dir = args.static_output_dir
    run_dir = args.output_root / args.run_name

    if not account_events_dir.exists():
        raise FileNotFoundError(
            f"Account-event input directory does not exist: "
            f"{account_events_dir}"
        )
    if not static_output_dir.exists():
        raise FileNotFoundError(
            f"Static baseline directory does not exist: "
            f"{static_output_dir}"
        )

    if run_dir.exists():
        raise FileExistsError(
            f"Run output directory already exists: {run_dir}\n"
            "Use a new --run-name so an old replay is never overwritten."
        )

    account_features_dir = run_dir / "node_account_features.parquet"
    account_edges_dir = run_dir / "edge_account_to_account.parquet"
    bank_features_dir = run_dir / "node_bank_features.parquet"
    entity_features_dir = run_dir / "node_entity_features.parquet"
    graph_delta_dir = run_dir / "graph_delta"

    # Exact 24 Account features from the frozen static feature contract.
    run_module(
        "streaming.materialize_account_features",
        "--input-dir",
        str(account_events_dir),
        "--output-dir",
        str(account_features_dir),
    )

    # Exact 9 directed Account -> Account edge features.
    run_module(
        "streaming.materialize_account_edges",
        "--input-dir",
        str(account_events_dir),
        "--output-dir",
        str(account_edges_dir),
    )

    # Exact 15 Bank and 15 Entity features.
    run_module(
        "streaming.materialize_bank_entity_features",
        "--input-dir",
        str(account_events_dir),
        "--static-output-dir",
        str(static_output_dir),
        "--bank-output-dir",
        str(bank_features_dir),
        "--entity-output-dir",
        str(entity_features_dir),
    )
    # Package nodes, edges, and metadata relations into one graph update.
    run_module(
        "streaming.build_graph_delta",
        "--account-events-dir",
        str(account_events_dir),
        "--account-features-dir",
        str(account_features_dir),
        "--bank-features-dir",
        str(bank_features_dir),
        "--entity-features-dir",
        str(entity_features_dir),
        "--account-edges-dir",
        str(account_edges_dir),
        "--output-dir",
        str(graph_delta_dir),
    )

    print("\nReplay materialization completed successfully.")
    print(f"All run outputs: {run_dir}")
    print(f"Graph package:    {graph_delta_dir}")


if __name__ == "__main__":
    main()
    