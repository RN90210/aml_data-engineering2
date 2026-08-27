from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ACCOUNT_EVENT_COLUMNS = [
    "account_id",
    "counterparty_account_id",
    "bank_id",
    "counterparty_bank_id",
    "entity_id",
    "event_time",
    "direction",
    "amount",
    "currency",
    "payment_format",
    "is_currency_switch",
    "is_cross_bank",
    "is_odd_hour",
    "is_weekend",
    "is_self_transfer",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Combine static baseline Account events with post-cutoff "
            "streaming events into one current graph-history table."
        )
    )
    parser.add_argument(
        "--baseline-events-dir",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--stream-events-dir",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("MuleDetectorCurrentGraphSnapshot")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_event_columns(events, source_name: str) -> None:
    missing_columns = [
        column_name
        for column_name in ACCOUNT_EVENT_COLUMNS
        if column_name not in events.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{source_name} is missing required Account-event columns: "
            f"{missing_columns}"
        )


def main() -> None:
    args = parse_args()

    if not args.baseline_events_dir.exists():
        raise FileNotFoundError(
            f"Baseline events not found: {args.baseline_events_dir}"
        )

    if not args.stream_events_dir.exists():
        raise FileNotFoundError(
            f"Streaming events not found: {args.stream_events_dir}"
        )

    if args.output_dir.exists():
        raise FileExistsError(
            f"Output directory already exists: {args.output_dir}\n"
            "Choose a new path so an existing snapshot is not overwritten."
        )

    spark = create_spark_session()

    try:
        baseline_events = spark.read.parquet(
            str(args.baseline_events_dir)
        )
        stream_events = spark.read.parquet(
            str(args.stream_events_dir)
        )

        validate_event_columns(baseline_events, "Baseline events")
        validate_event_columns(stream_events, "Streaming events")

        baseline_events = baseline_events.select(
            *ACCOUNT_EVENT_COLUMNS
        )
        stream_events = stream_events.select(
            *ACCOUNT_EVENT_COLUMNS
        )

        baseline_latest_time = baseline_events.agg(
            F.max("event_time").alias("latest_event_time")
        ).first()["latest_event_time"]

        stream_earliest_time = stream_events.agg(
            F.min("event_time").alias("earliest_event_time")
        ).first()["earliest_event_time"]

        if baseline_latest_time >= stream_earliest_time:
            raise ValueError(
                "Baseline and streaming events overlap in event time. "
                "Use a strict baseline cutoff and a post-cutoff stream."
            )

        combined_events = baseline_events.unionByName(stream_events)

        baseline_count = baseline_events.count()
        stream_count = stream_events.count()
        combined_count = combined_events.count()

        if combined_count != baseline_count + stream_count:
            raise ValueError(
                "Combined event count does not equal baseline plus "
                "stream event counts."
            )

        (
            combined_events.write
            .mode("errorifexists")
            .parquet(str(args.output_dir))
        )

        print(
            "Completed. Current graph-history events written to:"
        )
        print(args.output_dir)
        print(f"Baseline Account events: {baseline_count:,}")
        print(f"Post-cutoff Account events: {stream_count:,}")
        print(f"Combined Account events: {combined_count:,}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()