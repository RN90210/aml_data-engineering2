from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from preprocessing.spark_static_pipeline import (
    BANK_FEATURE_COLUMNS,
    ENTITY_FEATURE_COLUMNS,
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MuleDetectorStreamingBankEntityFeatures")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_bank_features(
    account_events: DataFrame,
    static_output_dir: Path,
) -> DataFrame:
    """Build exact Bank features for Banks affected by streamed events."""
    bank_events = account_events.select(
        "bank_id",
        "counterparty_bank_id",
        F.col("account_id").alias("participant_account_id"),
        "direction",
        "amount",
        "currency",
        "payment_format",
        "is_cross_bank",
        "is_odd_hour",
        "is_weekend",
    )

    bank_account_counts = (
        spark_read_parquet(
            static_output_dir / "node_bank_features.parquet"
        )
        .select("bank_id", "bank_account_count")
    )

    active_banks = bank_events.select("bank_id").distinct()
    outgoing = bank_events.filter(F.col("direction") == "outgoing")
    incoming = bank_events.filter(F.col("direction") == "incoming")

    outgoing_features = outgoing.groupBy("bank_id").agg(
        F.count("*").alias("outgoing_txn_count"),
        F.sum("amount").alias("outgoing_total_paid"),
        F.avg("amount").alias("outgoing_avg_paid"),
        F.countDistinct("participant_account_id").alias(
            "unique_sender_accounts"
        ),
    )

    incoming_features = incoming.groupBy("bank_id").agg(
        F.count("*").alias("incoming_txn_count"),
        F.sum("amount").alias("incoming_total_received"),
        F.avg("amount").alias("incoming_avg_received"),
        F.countDistinct("participant_account_id").alias(
            "unique_receiver_accounts"
        ),
    )

    shared_features = bank_events.groupBy("bank_id").agg(
        F.countDistinct("counterparty_bank_id").alias(
            "counterparty_bank_count"
        ),
        F.countDistinct("currency").alias("currency_diversity"),
        F.countDistinct("payment_format").alias(
            "payment_format_diversity"
        ),
        F.avg("is_cross_bank").alias("cross_bank_txn_ratio"),
        F.avg("is_odd_hour").alias("odd_hour_txn_ratio"),
        F.avg("is_weekend").alias("weekend_txn_ratio"),
    )

    features = (
        active_banks
        .join(bank_account_counts, on="bank_id", how="left")
        .join(outgoing_features, on="bank_id", how="left")
        .join(incoming_features, on="bank_id", how="left")
        .join(shared_features, on="bank_id", how="left")
        .fillna(0)
    )

    return features.select(
        F.col("bank_id"),
        *[
            F.col(feature_name).cast("double").alias(feature_name)
            for feature_name in BANK_FEATURE_COLUMNS
        ],
    )


def build_entity_features(
    account_events: DataFrame,
    static_output_dir: Path,
) -> DataFrame:
    """Build exact Entity features for Entities affected by streamed events."""
    entity_events = account_events.select(
        "entity_id",
        "bank_id",
        "account_id",
        "counterparty_account_id",
        "direction",
        "amount",
        "payment_format",
        "is_cross_bank",
        "is_odd_hour",
    )

    entity_type_columns = [
        column_name
        for column_name in ENTITY_FEATURE_COLUMNS
        if column_name.startswith("entity_type_")
    ]

    entity_types = (
        spark_read_parquet(
            static_output_dir / "node_entity_features.parquet"
        )
        .select("entity_id", *entity_type_columns)
    )

    active_entities = entity_events.select("entity_id").distinct()
    outgoing = entity_events.filter(F.col("direction") == "outgoing")
    incoming = entity_events.filter(F.col("direction") == "incoming")

    ownership_features = entity_events.groupBy("entity_id").agg(
        F.countDistinct("account_id").alias("owned_account_count"),
        F.countDistinct("bank_id").alias("connected_bank_count"),
    )

    outgoing_features = outgoing.groupBy("entity_id").agg(
        F.count("*").alias("outgoing_txn_count"),
        F.sum("amount").alias("outgoing_total_paid"),
    )

    incoming_features = incoming.groupBy("entity_id").agg(
        F.count("*").alias("incoming_txn_count"),
        F.sum("amount").alias("incoming_total_received"),
    )

    shared_features = entity_events.groupBy("entity_id").agg(
        F.countDistinct("counterparty_account_id").alias(
            "unique_counterparty_accounts"
        ),
        F.avg("is_cross_bank").alias("cross_bank_txn_ratio"),
        F.avg("is_odd_hour").alias("odd_hour_txn_ratio"),
        F.countDistinct("payment_format").alias(
            "payment_format_diversity"
        ),
    )

    features = (
        active_entities
        .join(entity_types, on="entity_id", how="left")
        .join(ownership_features, on="entity_id", how="left")
        .join(outgoing_features, on="entity_id", how="left")
        .join(incoming_features, on="entity_id", how="left")
        .join(shared_features, on="entity_id", how="left")
        .fillna(0)
    )

    return features.select(
        F.col("entity_id"),
        *[
            F.col(feature_name).cast("double").alias(feature_name)
            for feature_name in ENTITY_FEATURE_COLUMNS
        ],
    )


def spark_read_parquet(path: Path) -> DataFrame:
    """Read a static Spark output through the active Spark session."""
    return SparkSession.getActiveSession().read.parquet(str(path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize updated Bank and Entity features "
            "from streaming account events."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/streaming/account_events_v3"),
    )
    parser.add_argument(
        "--static-output-dir",
        type=Path,
        default=Path("data/processed/spark_static_full"),
    )
    parser.add_argument(
        "--bank-output-dir",
        type=Path,
        default=Path("data/streaming/current_bank_features_v1"),
    )
    parser.add_argument(
        "--entity-output-dir",
        type=Path,
        default=Path("data/streaming/current_entity_features_v1"),
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

    bank_features = build_bank_features(
        account_events,
        args.static_output_dir,
    )
    entity_features = build_entity_features(
        account_events,
        args.static_output_dir,
    )

    bank_features.write.mode("overwrite").parquet(
        str(args.bank_output_dir)
    )
    entity_features.write.mode("overwrite").parquet(
        str(args.entity_output_dir)
    )

    print(
        "Completed. Updated Bank features written to: "
        f"{args.bank_output_dir}"
    )
    print(
        "Completed. Updated Entity features written to: "
        f"{args.entity_output_dir}"
    )


if __name__ == "__main__":
    main()