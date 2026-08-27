from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from preprocessing.spark_static_pipeline import (
    ACCOUNT_FEATURE_COLUMNS,
    ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS,
    BANK_FEATURE_COLUMNS,
    ENTITY_FEATURE_COLUMNS,
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MuleDetectorStreamingGraphDelta")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def require_columns(
    dataframe: DataFrame,
    expected_columns: list[str],
    table_name: str,
) -> None:
    actual_columns = dataframe.columns

    if actual_columns != expected_columns:
        raise ValueError(
            f"{table_name} schema mismatch.\n"
            f"Expected: {expected_columns}\n"
            f"Actual:   {actual_columns}"
        )


def build_metadata_relations(
    account_events: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    account_to_bank = account_events.select(
        "account_id",
        "bank_id",
    ).distinct()

    entity_to_account = account_events.select(
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
        raise ValueError("An active Account maps to multiple Banks.")

    if multiple_entity_mappings != 0:
        raise ValueError("An active Account maps to multiple Entities.")

    return account_to_bank, entity_to_account


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Package updated streaming node, edge, and relation tables "
            "as a graph delta."
        )
    )
    parser.add_argument(
        "--account-events-dir",
        type=Path,
        default=Path("data/streaming/account_events_v3"),
    )
    parser.add_argument(
        "--account-features-dir",
        type=Path,
        default=Path("data/streaming/current_account_features_v1"),
    )
    parser.add_argument(
        "--bank-features-dir",
        type=Path,
        default=Path("data/streaming/current_bank_features_v1"),
    )
    parser.add_argument(
        "--entity-features-dir",
        type=Path,
        default=Path("data/streaming/current_entity_features_v1"),
    )
    parser.add_argument(
        "--account-edges-dir",
        type=Path,
        default=Path("data/streaming/current_account_edges_v1"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/streaming/graph_delta_v1"),
    )
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help=(
            "Mark this package as a full current graph snapshot "
            "built from baseline history plus streamed events."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_paths = [
        args.account_events_dir,
        args.account_features_dir,
        args.bank_features_dir,
        args.entity_features_dir,
        args.account_edges_dir,
    ]

    for path in input_paths:
        if not path.exists():
            raise FileNotFoundError(f"Required input not found: {path}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    account_events = spark.read.parquet(str(args.account_events_dir))
    account_features = spark.read.parquet(str(args.account_features_dir))
    bank_features = spark.read.parquet(str(args.bank_features_dir))
    entity_features = spark.read.parquet(str(args.entity_features_dir))
    account_edges = spark.read.parquet(str(args.account_edges_dir))

    require_columns(
        account_features,
        ["account_id", *ACCOUNT_FEATURE_COLUMNS],
        "Account feature table",
    )
    require_columns(
        bank_features,
        ["bank_id", *BANK_FEATURE_COLUMNS],
        "Bank feature table",
    )
    require_columns(
        entity_features,
        ["entity_id", *ENTITY_FEATURE_COLUMNS],
        "Entity feature table",
    )
    require_columns(
        account_edges,
        [
            "sender_account_id",
            "receiver_account_id",
            *ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS,
        ],
        "Account-to-Account edge table",
    )

    account_to_bank, entity_to_account = build_metadata_relations(
        account_events
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    outputs = {
        "node_account_features.parquet": account_features,
        "node_bank_features.parquet": bank_features,
        "node_entity_features.parquet": entity_features,
        "edge_account_to_account.parquet": account_edges,
        "edge_account_to_bank.parquet": account_to_bank,
        "edge_entity_to_account.parquet": entity_to_account,
    }

    output_counts = {}

    for name, dataframe in outputs.items():
        output_path = args.output_dir / name

        dataframe.write.mode("overwrite").parquet(str(output_path))
        output_counts[name] = dataframe.count()

    if args.snapshot:
        manifest = {
            "kind": "current_graph_snapshot",
            "version": 1,
            "mode": "baseline_plus_post_cutoff_events",
            "baseline_graph": (
                "spark_static_baseline_20220901_0030"
            ),
            "description": (
                "Complete current graph snapshot recomputed from "
                "static baseline event history plus validated "
                "post-cutoff Kafka events."
            ),
            "counts": output_counts,
        }
        package_name = "Current graph snapshot"
    else:
        manifest = {
            "kind": "streaming_graph_delta",
            "version": 1,
            "mode": "historical_replay_window",
            "baseline_graph": None,
            "description": (
                "Node features, edges, and relations calculated solely "
                "from events in this replay window."
            ),
            "counts": output_counts,
        }
        package_name = "Streaming graph delta"
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    print(f"Completed. {package_name} written to: {args.output_dir}")
    print(json.dumps(output_counts, indent=2))


if __name__ == "__main__":
    main()