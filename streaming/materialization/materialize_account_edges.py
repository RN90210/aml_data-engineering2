from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from preprocessing.spark_static_pipeline import (
    compute_base_account_to_account_edges,
    finalize_account_to_account_edges,
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MuleDetectorStreamingAccountEdges")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def rebuild_transaction_rows(account_events):
    """
    Reconstruct one transaction row from each outgoing Account event.

    Every source transaction creates exactly one outgoing event, so this retains
    the fields required by the static 9-feature edge formulas.
    """
    return (
        account_events
        .filter(F.col("direction") == "outgoing")
        .select(
            F.col("account_id").alias("sender_account_id"),
            F.col("counterparty_account_id").alias(
                "receiver_account_id"
            ),
            F.col("amount").alias("Amount Paid"),
            F.col("payment_format").alias("Payment Format"),
            F.col("event_time"),
            F.col("is_currency_switch"),
        )
    )


def build_account_edges(
    account_events,
    snapshot_time=None,
):
    """Build the exact nine Account-to-Account edge features."""
    transactions = rebuild_transaction_rows(account_events)
    base_edges = compute_base_account_to_account_edges(transactions)

    return finalize_account_to_account_edges(
        base_edges,
        transactions,
        snapshot_time=snapshot_time,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize exact Account-to-Account edge features "
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
        default=Path("data/streaming/current_account_edges_v1"),
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
    account_edges = build_account_edges(account_events)

    (
        account_edges
        .write
        .mode("overwrite")
        .parquet(str(args.output_dir))
    )

    print(
        "Completed. Exact 9-feature Account edge table written to: "
        f"{args.output_dir}"
    )


if __name__ == "__main__":
    main()
