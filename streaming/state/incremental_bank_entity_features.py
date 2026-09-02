from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.materialization.materialize_bank_entity_features import (
    build_bank_features,
    build_entity_features,
    create_spark_session,
)


def build_incremental_bank_features(
    state_events: DataFrame,
    current_batch_events: DataFrame,
    static_output_dir: Path,
    bank_account_counts: DataFrame | None = None,
) -> DataFrame:
    """Update only Banks touched by the current micro-batch."""
    affected_banks = current_batch_events.select(
        "bank_id"
    ).distinct()

    affected_bank_history = state_events.join(
        affected_banks,
        on="bank_id",
        how="inner",
    )

    return build_bank_features(
        affected_bank_history,
        static_output_dir=static_output_dir,
        bank_account_counts=bank_account_counts,
    )


def build_incremental_entity_features(
    state_events: DataFrame,
    current_batch_events: DataFrame,
    static_output_dir: Path,
    entity_types: DataFrame | None = None,
) -> DataFrame:
    """Update only Entities touched by the current micro-batch."""
    affected_entities = current_batch_events.select(
        "entity_id"
    ).distinct()

    affected_entity_history = state_events.join(
        affected_entities,
        on="entity_id",
        how="inner",
    )

    return build_entity_features(
        affected_entity_history,
        static_output_dir=static_output_dir,
        entity_types=entity_types,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize exact Bank and Entity feature updates for one "
            "Kafka micro-batch."
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
        "--static-output-dir",
        type=Path,
        default=Path("data/processed/spark_static_full"),
        help=(
            "Static metadata output. Only Bank account counts and Entity "
            "type columns are read from this directory."
        ),
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

    if not args.static_output_dir.exists():
        raise FileNotFoundError(
            "Static metadata directory not found: "
            f"{args.static_output_dir}"
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

    bank_feature_updates = build_incremental_bank_features(
        state_events,
        current_batch_events,
        args.static_output_dir,
    )
    entity_feature_updates = build_incremental_entity_features(
        state_events,
        current_batch_events,
        args.static_output_dir,
    )

    batch_output_dir = (
        args.output_root
        / f"run_id={args.run_id}"
        / f"batch_id={args.batch_id}"
    )

    bank_output_dir = batch_output_dir / "bank_features.parquet"
    entity_output_dir = batch_output_dir / "entity_features.parquet"

    bank_feature_updates.write.mode("overwrite").parquet(
        str(bank_output_dir)
    )
    entity_feature_updates.write.mode("overwrite").parquet(
        str(entity_output_dir)
    )

    print(f"Bank output directory: {bank_output_dir}")
    print(f"Entity output directory: {entity_output_dir}")
    print(
        "Affected Banks updated: "
        f"{bank_feature_updates.count()}"
    )
    print(
        "Affected Entities updated: "
        f"{entity_feature_updates.count()}"
    )


if __name__ == "__main__":
    main()