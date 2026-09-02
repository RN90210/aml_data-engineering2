from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.materialization.materialize_account_edges import (
    build_account_edges,
    create_spark_session,
    rebuild_transaction_rows,
)


def build_incremental_account_edges(
    state_events: DataFrame,
    current_batch_events: DataFrame,
) -> DataFrame:
    """
    Recompute exact edge features only for directed pairs touched by a batch.

    Each selected edge uses its complete transaction history from replay start
    through the current micro-batch.
    """
    current_transactions = rebuild_transaction_rows(
        current_batch_events
    )

    affected_edge_keys = current_transactions.select(
        "sender_account_id",
        "receiver_account_id",
    ).distinct()

    affected_edge_events = (
        state_events
        .filter(F.col("direction") == "outgoing")
        .join(
            affected_edge_keys,
            (
                (F.col("account_id")
                 == F.col("sender_account_id"))
                & (F.col("counterparty_account_id")
                   == F.col("receiver_account_id"))
            ),
            how="inner",
        )
        .select(state_events["*"])
    )

    global_snapshot_time = state_events.agg(
        F.max("event_time").alias("snapshot_time")
    )

    return build_account_edges(
        account_events=affected_edge_events,
        snapshot_time=global_snapshot_time,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize exact nine-feature updates for directed "
            "Account-to-Account edges touched by one Kafka micro-batch."
        )
    )
    parser.add_argument(
        "--event-state-dir",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--run-id",
        required=True,
    )
    parser.add_argument(
        "--batch-id",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/streaming/model_updates"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_state_dir = (
        args.event_state_dir / f"run_id={args.run_id}"
    )

    if not run_state_dir.exists():
        raise FileNotFoundError(
            f"Event state run directory not found: {run_state_dir}"
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    state_events = (
        spark.read
        .parquet(str(run_state_dir))
        .filter(
            F.col("batch_id").cast("long")
            <= F.lit(args.batch_id).cast("long")
        )
    )

    current_batch_events = state_events.filter(
        F.col("batch_id").cast("long")
        == F.lit(args.batch_id).cast("long")
    )

    if not current_batch_events.head(1):
        raise ValueError(
            "No valid events found for "
            f"run_id={args.run_id}, batch_id={args.batch_id}."
        )

    account_edge_updates = build_incremental_account_edges(
        state_events,
        current_batch_events,
    )

    output_dir = (
        args.output_root
        / f"run_id={args.run_id}"
        / f"batch_id={args.batch_id}"
        / "account_to_account_edges.parquet"
    )

    (
        account_edge_updates.write
        .mode("overwrite")
        .parquet(str(output_dir))
    )

    print(f"Output directory: {output_dir}")
    print(
        "Affected directed edges updated: "
        f"{account_edge_updates.count()}"
    )


if __name__ == "__main__":
    main()
