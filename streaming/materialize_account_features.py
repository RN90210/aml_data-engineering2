from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from preprocessing.spark_static_pipeline import (
    compute_directional_account_features,
    compute_shared_account_features,
    compute_temporal_account_features,
    finalize_account_features,
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MuleDetectorStreamingAccountFeatures")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_account_features(account_events):
    """
    Reuse the exact static feature formulas over the streamed account-event log.

    The output is the same 24-feature Account contract used by the graph/model.
    """
    all_accounts = account_events.select("account_id").distinct()

    directional_features = compute_directional_account_features(
        account_events
    )
    shared_features = compute_shared_account_features(account_events)
    temporal_features = compute_temporal_account_features(account_events)

    feature_stage = (
        all_accounts
        .join(directional_features, on="account_id", how="left")
        .join(shared_features, on="account_id", how="left")
        .join(temporal_features, on="account_id", how="left")
        .fillna(0)
    )

    return finalize_account_features(feature_stage)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize the exact 24 Account features "
            "from streaming account events."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/streaming/account_events_v3"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/streaming/current_account_features_v1"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.input_dir.exists():
        raise FileNotFoundError(
            f"Streaming account events not found: {args.input_dir}"
        )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    account_events = spark.read.parquet(str(args.input_dir))
    account_features = build_account_features(account_events)

    (
        account_features
        .write
        .mode("overwrite")
        .parquet(str(args.output_dir))
    )

    print(
        "Completed. Exact 24-feature Account table written to: "
        f"{args.output_dir}"
    )


if __name__ == "__main__":
    main()