from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.materialization.materialize_account_features import (
    create_spark_session,
)
from streaming.state.incremental_account_edges import (
    build_incremental_account_edges,
)
from streaming.state.incremental_account_features import (
    build_incremental_account_features,
)
from streaming.state.incremental_bank_entity_features import (
    build_incremental_bank_features,
    build_incremental_entity_features,
)


def build_incremental_relations(
    current_batch_events: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Create only the stable Account mappings touched by one batch."""
    account_to_bank = current_batch_events.select(
        "account_id",
        "bank_id",
    ).distinct()

    entity_to_account = current_batch_events.select(
        "entity_id",
        "account_id",
    ).distinct()

    multiple_bank_mappings = (
        account_to_bank
        .groupBy("account_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    multiple_entity_mappings = (
        entity_to_account
        .groupBy("account_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if multiple_bank_mappings != 0:
        raise ValueError(
            "An affected Account maps to multiple Banks."
        )

    if multiple_entity_mappings != 0:
        raise ValueError(
            "An affected Account maps to multiple Entities."
        )

    return account_to_bank, entity_to_account


def write_batch_update(
    state_events: DataFrame,
    current_batch_events: DataFrame,
    static_output_dir: Path,
    output_root: Path,
    run_id: str,
    batch_id: int,
    bank_account_counts: DataFrame | None = None,
    entity_types: DataFrame | None = None,
) -> dict[str, int]:
    """
    Materialize one model-ready incremental package without a graph snapshot.

    Each table contains only rows affected by the current micro-batch, but
    every feature is calculated from that row's cumulative state history.
    """
    account_features = build_incremental_account_features(
        state_events,
        current_batch_events,
    )
    account_edges = build_incremental_account_edges(
        state_events,
        current_batch_events,
    )
    bank_features = build_incremental_bank_features(
        state_events,
        current_batch_events,
        static_output_dir,
        bank_account_counts=bank_account_counts,
    )
    entity_features = build_incremental_entity_features(
        state_events,
        current_batch_events,
        static_output_dir,
        entity_types=entity_types,
    )
    account_to_bank, entity_to_account = build_incremental_relations(
        current_batch_events
    )

    batch_output_dir = (
        output_root
        / f"run_id={run_id}"
        / f"batch_id={batch_id}"
    )

    outputs = {
        "account_features.parquet": account_features,
        "bank_features.parquet": bank_features,
        "entity_features.parquet": entity_features,
        "account_to_account_edges.parquet": account_edges,
        "account_to_bank.parquet": account_to_bank,
        "entity_to_account.parquet": entity_to_account,
    }

    output_counts = {}

    for name, dataframe in outputs.items():
        output_path = batch_output_dir / name
        dataframe = dataframe.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            output_counts[name] = dataframe.count()
            dataframe.write.mode("overwrite").parquet(str(output_path))
        finally:
            dataframe.unpersist()

    manifest = {
        "kind": "incremental_model_update",
        "run_id": run_id,
        "batch_id": batch_id,
        "description": (
            "Only nodes, edges, and mappings affected by this Kafka "
            "micro-batch. Feature values use cumulative event state "
            "through this batch."
        ),
        "counts": output_counts,
    }

    manifest_path = batch_output_dir / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    return output_counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write one model-ready incremental feature package for a "
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
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    current_batch_events = state_events.filter(
        F.col("batch_id").cast("long")
        == F.lit(args.batch_id).cast("long")
    ).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        if not current_batch_events.head(1):
            raise ValueError(
                "No valid events found for "
                f"run_id={args.run_id}, batch_id={args.batch_id}."
            )

        output_counts = write_batch_update(
            state_events=state_events,
            current_batch_events=current_batch_events,
            static_output_dir=args.static_output_dir,
            output_root=args.output_root,
            run_id=args.run_id,
            batch_id=args.batch_id,
        )
    finally:
        current_batch_events.unpersist()
        state_events.unpersist()

    print("Incremental model-update package written.")
    print(json.dumps(output_counts, indent=2))


if __name__ == "__main__":
    main()
