from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from preprocessing.spark_static_pipeline import TRANSACTION_SCHEMA


TIMESTAMP_FORMAT = "yyyy/MM/dd HH:mm"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare one timestamp-ordered Kafka replay window "
            "from the raw IBM AML transaction CSV."
        )
    )
    parser.add_argument("--input-path", type=Path, required=True)
    parser.add_argument(
        "--start-timestamp",
        required=True,
        help=(
            "Inclusive window start in YYYY/MM/DD HH:MM format."
        ),
    )
    parser.add_argument(
        "--end-timestamp",
        required=True,
        help=(
            "Exclusive window end in YYYY/MM/DD HH:MM format."
        ),
    )
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--output-path", type=Path, required=True)
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("MuleDetectorPrepareReplayWindow")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    args = parse_args()

    if args.max_rows <= 0:
        raise ValueError("--max-rows must be greater than zero.")

    if not args.input_path.exists():
        raise FileNotFoundError(
            f"Transaction CSV not found: {args.input_path}"
        )

    if args.output_path.exists():
        raise FileExistsError(
            f"Replay file already exists: {args.output_path}\n"
            "Use a new output path rather than overwriting it."
        )

    spark = build_spark_session()

    try:
        raw_transactions = (
            spark.read
            .option("header", "false")
            .schema(TRANSACTION_SCHEMA)
            .csv(str(args.input_path))
        )

        transactions = (
            raw_transactions
            .filter(F.col("Timestamp") != "Timestamp")
            .withColumn(
                "_event_time",
                F.to_timestamp(
                    F.col("Timestamp"),
                    TIMESTAMP_FORMAT,
                ),
            )
        )

        replay_window = (
            transactions
            .filter(
                (F.col("_event_time") >= F.to_timestamp(
                    F.lit(args.start_timestamp),
                    TIMESTAMP_FORMAT,
                ))
                & (
                    F.col("_event_time")
                    < F.to_timestamp(
                        F.lit(args.end_timestamp),
                        TIMESTAMP_FORMAT,
                    )
                )
            )
            .orderBy(
                F.col("_event_time"),
                F.col("From Bank"),
                F.col("Account"),
                F.col("To Bank"),
                F.col("`Account.1`"),
                F.col("Amount Paid"),
                F.col("Payment Format"),
            )
            .limit(args.max_rows)
            .drop("_event_time")
        )

        replay_count = replay_window.count()

        if replay_count != args.max_rows:
            raise ValueError(
                f"Only {replay_count:,} transactions were found in "
                "the requested window. Choose a wider time window."
            )

        args.output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        replay_window.toPandas().to_csv(
            args.output_path,
            index=False,
        )

        print(
            f"Prepared {replay_count:,} chronologically ordered "
            f"transactions."
        )
        print(
            f"Replay window: [{args.start_timestamp}, "
            f"{args.end_timestamp})"
        )
        print(f"Saved replay file: {args.output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()