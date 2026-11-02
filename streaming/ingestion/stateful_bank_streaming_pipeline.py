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
from streaming.state.stateful_bank_processor import (
    build_stateful_bank_features,
    finalize_stateful_bank_feature_updates,
)


def batch_output_path(
    output_dir: Path,
    run_id: str,
    batch_id: int,
) -> Path:
    """Return the deterministic Bank output path for one batch."""
    return (
        output_dir
        / f"run_id={run_id}"
        / f"batch_id={batch_id}"
        / "bank_features.parquet"
    )


def write_bank_feature_batch(
    bank_updates: DataFrame,
    batch_id: int,
    output_dir: Path,
    run_id: str,
    bank_account_counts: DataFrame,
) -> None:
    """Finalize and write one micro-batch of Bank feature updates."""
    bank_features = finalize_stateful_bank_feature_updates(
        bank_updates,
        bank_account_counts,
    )
    output_path = batch_output_path(output_dir, run_id, batch_id)

    bank_features.write.mode("overwrite").parquet(str(output_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create checkpointed, stateful Bank feature updates from "
            "Kafka transactions."
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
        "--static-metadata-dir",
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
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.conf.set(
        "spark.sql.streaming.stateStore.providerClass",
        (
            "org.apache.spark.sql.execution.streaming.state."
            "RocksDBStateStoreProvider"
        ),
    )
    spark.sparkContext.setLogLevel("WARN")

    entity_lookup = load_entity_lookup(
        spark,
        args.account_entity_lookup_path,
    )
    cached_bank_account_counts = (
        spark.read
        .parquet(
            str(
                args.static_metadata_dir
                / "node_bank_features.parquet"
            )
        )
        .select("bank_id", "bank_account_count")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    cached_bank_account_counts.count()
    bank_account_counts = F.broadcast(cached_bank_account_counts)

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
    bank_updates = build_stateful_bank_features(
        valid_account_events
    )

    try:
        query = (
            bank_updates.writeStream
            .outputMode("update")
            .foreachBatch(
                lambda batch_updates, batch_id: write_bank_feature_batch(
                    bank_updates=batch_updates,
                    batch_id=batch_id,
                    output_dir=args.output_dir,
                    run_id=args.run_id,
                    bank_account_counts=bank_account_counts,
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
    finally:
        cached_bank_account_counts.unpersist()
        entity_lookup.unpersist()

    print("Stateful Bank feature run complete.")
    print(f"Output directory: {args.output_dir / f'run_id={args.run_id}'}")


if __name__ == "__main__":
    main()
