from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.materialization.materialize_account_features import (
    build_account_features,
    create_spark_session,
)


def build_incremental_account_features(
    state_events: DataFrame,
    current_batch_events: DataFrame,
) -> DataFrame:
    affected_account_ids = current_batch_events.select(
        "account_id"
    ).distinct()

    affected_account_history = state_events.join(
        affected_account_ids,
        on="account_id",
        how="inner",
    )

    global_snapshot_time = state_events.agg(
        F.max("event_time").alias("snapshot_time")
    )

    return build_account_features(
        affected_account_history,
        snapshot_time=global_snapshot_time,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Update Account features for Accounts touched by one batch."
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
    run_state_dir = args.event_state_dir / f"run_id={args.run_id}"

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

    affected_account_count = (
        current_batch_events
        .select("account_id")
        .distinct()
        .count()
    )

    account_features = build_incremental_account_features(
        state_events,
        current_batch_events,
    )

    output_dir = (
        args.output_root
        / f"run_id={args.run_id}"
        / f"batch_id={args.batch_id}"
        / "account_features.parquet"
    )

    (
        account_features.write
        .mode("overwrite")
        .parquet(str(output_dir))
    )

    print(f"Output directory: {output_dir}")
    print(f"Affected Accounts updated: {affected_account_count}")


if __name__ == "__main__":
    main()
