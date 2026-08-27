from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.spark_kafka_consumer import (
    build_spark_session,
    parse_transaction_stream,
)


REQUIRED_EVENT_COLUMNS = [
    "account_id",
    "counterparty_account_id",
    "bank_id",
    "counterparty_bank_id",
    "entity_id",
    "event_time",
    "amount",
    "currency",
    "payment_format",
]


def add_transaction_flags(transactions: DataFrame) -> DataFrame:
    return (
        transactions
        .withColumn(
            "sender_account_id",
            F.concat_ws(
                "::",
                F.col("From Bank").cast("string"),
                F.col("Account"),
            ),
        )
        .withColumn(
            "receiver_account_id",
            F.concat_ws(
                "::",
                F.col("To Bank").cast("string"),
                F.col("`Account.1`"),
            ),
        )
        .withColumn(
            "is_currency_switch",
            (
                F.col("payment_currency")
                != F.col("Receiving Currency")
            ).cast("int"),
        )
        .withColumn(
            "is_cross_bank",
            (
                F.col("From Bank") != F.col("To Bank")
            ).cast("int"),
        )
        .withColumn(
            "is_odd_hour",
            F.hour("event_time").between(0, 5).cast("int"),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("event_time").isin([1, 7]).cast("int"),
        )
        .withColumn(
            "is_self_transfer",
            (
                F.col("sender_account_id")
                == F.col("receiver_account_id")
            ).cast("int"),
        )
    )


def build_account_events(transactions: DataFrame) -> DataFrame:
    """Create exactly one outgoing and one incoming event per transaction."""
    common_columns = [
        "event_time",
        "payment_format",
        "is_currency_switch",
        "is_cross_bank",
        "is_odd_hour",
        "is_weekend",
        "is_self_transfer",
        "kafka_partition",
        "kafka_offset",
        "kafka_ingested_at",
    ]

    outgoing_event = F.struct(
        F.col("sender_account_id").alias("account_id"),
        F.col("receiver_account_id").alias(
            "counterparty_account_id"
        ),
        F.col("From Bank").alias("bank_id"),
        F.col("To Bank").alias("counterparty_bank_id"),
        F.lit("outgoing").alias("direction"),
        F.col("Amount Paid").alias("amount"),
        F.col("payment_currency").alias("currency"),
        *[F.col(name) for name in common_columns],
    )

    incoming_event = F.struct(
        F.col("receiver_account_id").alias("account_id"),
        F.col("sender_account_id").alias(
            "counterparty_account_id"
        ),
        F.col("To Bank").alias("bank_id"),
        F.col("From Bank").alias("counterparty_bank_id"),
        F.lit("incoming").alias("direction"),
        F.col("Amount Received").alias("amount"),
        F.col("Receiving Currency").alias("currency"),
        *[F.col(name) for name in common_columns],
    )

    return (
        transactions
        .select(
            F.explode(
                F.array(outgoing_event, incoming_event)
            ).alias("event")
        )
        .select("event.*")
    )


def filter_valid_events(events: DataFrame) -> DataFrame:
    valid_condition = F.lit(True)

    for column_name in REQUIRED_EVENT_COLUMNS:
        valid_condition = valid_condition & F.col(
            column_name
        ).isNotNull()

    return events.filter(valid_condition)


def load_entity_lookup(
    spark: SparkSession,
    account_entity_lookup_path: Path,
) -> DataFrame:
    """Load full Account-to-Entity metadata for streaming enrichment."""
    if not account_entity_lookup_path.exists():
        raise FileNotFoundError(
            "Account metadata lookup not found: "
            f"{account_entity_lookup_path}"
        )

    return (
        spark.read.parquet(str(account_entity_lookup_path))
        .select("account_id", "entity_id")
        .dropDuplicates(["account_id"])
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create validated Account events from Kafka transactions."
    )
    parser.add_argument("--topic", default="aml_transactions")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/streaming/account_events_v1"),
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=Path(
            "data/checkpoints/account_event_pipeline_v2"
        ),
    )
    parser.add_argument(
        "--account-entity-lookup-path",
        type=Path,
        default=Path(
            "data/processed/account_entity_lookup.parquet"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    entity_lookup = load_entity_lookup(
        spark,
        args.account_entity_lookup_path,
    )

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            args.bootstrap_servers,
        )
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option(
            "maxOffsetsPerTrigger",
            args.max_offsets_per_trigger,
        )
        .option("failOnDataLoss", "false")
        .load()
    )

    transactions = add_transaction_flags(
        parse_transaction_stream(kafka_stream)
    )

    valid_events = (
        build_account_events(transactions)
        .join(entity_lookup, on="account_id", how="left")
        .transform(filter_valid_events)
        .withColumn("event_date", F.to_date("event_time"))
    )

    query = (
        valid_events.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", str(args.output_dir))
        .option(
            "checkpointLocation",
            str(args.checkpoint_dir),
        )
        .partitionBy("event_date")
        .trigger(availableNow=True)
        .start()
    )

    print("Processing every Kafka event currently available...")
    query.awaitTermination()
    print(f"Completed. Account events written to: {args.output_dir}")


if __name__ == "__main__":
    main()