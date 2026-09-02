from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if sys.platform == "win32":
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.ingestion.spark_streaming_pipeline import (
    add_transaction_flags,
    add_transaction_rejection_reason,
    build_account_events,
    enrich_transaction_entities,
    load_entity_lookup,
    split_valid_and_rejected_events,
)
from streaming.kafka.spark_kafka_consumer import (
    build_spark_session,
    parse_transaction_stream,
)


def batch_output_path(
    output_dir: Path,
    run_id: str,
    batch_id: int,
    relation_name: str,
) -> Path:
    """Return the deterministic path for one batch relation."""
    return (
        output_dir
        / f"run_id={run_id}"
        / f"batch_id={batch_id}"
        / f"{relation_name}.parquet"
    )


def validate_account_mappings(account_mappings: DataFrame) -> None:
    """Reject a batch containing an unstable Bank or Entity mapping."""
    conflicts = (
        account_mappings
        .groupBy("account_id")
        .agg(
            F.countDistinct("bank_id").alias("bank_count"),
            F.countDistinct("entity_id").alias("entity_count"),
        )
        .filter(
            (F.col("bank_count") > 1)
            | (F.col("entity_count") > 1)
        )
        .limit(1)
        .collect()
    )

    if not conflicts:
        return

    conflict = conflicts[0]
    problems = []
    if conflict["bank_count"] > 1:
        problems.append("multiple Banks")
    if conflict["entity_count"] > 1:
        problems.append("multiple Entities")

    raise ValueError(
        f"Account {conflict['account_id']} maps to "
        f"{' and '.join(problems)} in the current batch."
    )


def write_account_relation_batch(
    valid_account_events: DataFrame,
    batch_id: int,
    output_dir: Path,
    run_id: str,
) -> None:
    """Validate and write stable Account relations for one batch."""
    account_mappings = valid_account_events.select(
        "account_id",
        "bank_id",
        "entity_id",
    ).distinct().persist(StorageLevel.MEMORY_AND_DISK)

    try:
        validate_account_mappings(account_mappings)

        account_to_bank = account_mappings.select(
            "account_id",
            "bank_id",
        ).distinct()
        entity_to_account = account_mappings.select(
            "entity_id",
            "account_id",
        ).distinct()

        account_to_bank.write.mode("overwrite").parquet(
            str(
                batch_output_path(
                    output_dir,
                    run_id,
                    batch_id,
                    "account_to_bank",
                )
            )
        )
        entity_to_account.write.mode("overwrite").parquet(
            str(
                batch_output_path(
                    output_dir,
                    run_id,
                    batch_id,
                    "entity_to_account",
                )
            )
        )
    finally:
        account_mappings.unpersist()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write stable Account-to-Bank and Entity-to-Account "
            "mapping updates from Kafka transactions."
        )
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=500,
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--account-entity-lookup-path",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        required=True,
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

    transactions = (
        parse_transaction_stream(kafka_stream)
        .withColumn("kafka_topic", F.lit(args.topic))
    )
    flagged_transactions = add_transaction_flags(transactions)
    enriched_transactions = enrich_transaction_entities(
        flagged_transactions,
        entity_lookup,
    )
    validated_transactions = add_transaction_rejection_reason(
        enriched_transactions
    )
    account_events = build_account_events(validated_transactions)
    valid_account_events, _ = split_valid_and_rejected_events(
        account_events
    )

    query = (
        valid_account_events.writeStream
        .outputMode("append")
        .foreachBatch(
            lambda batch_events, batch_id: write_account_relation_batch(
                valid_account_events=batch_events,
                batch_id=batch_id,
                output_dir=args.output_dir,
                run_id=args.run_id,
            )
        )
        .option(
            "checkpointLocation",
            str(args.checkpoint_dir),
        )
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    print("Account relation update run complete.")
    print(f"Output directory: {args.output_dir / f'run_id={args.run_id}'}")


if __name__ == "__main__":
    main()
