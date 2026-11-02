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
from streaming.state.stateful_entity_processor import (
    ENTITY_TYPE_FEATURE_COLUMNS,
    build_stateful_entity_features,
    finalize_stateful_entity_feature_updates,
)


def batch_output_path(
    output_dir: Path,
    run_id: str,
    batch_id: int,
) -> Path:
    """Return the deterministic Entity output path for one batch."""
    return (
        output_dir
        / f"run_id={run_id}"
        / f"batch_id={batch_id}"
        / "entity_features.parquet"
    )


def write_entity_feature_batch(
    entity_updates: DataFrame,
    batch_id: int,
    output_dir: Path,
    run_id: str,
    entity_types: DataFrame,
) -> None:
    """Finalize and write one micro-batch of Entity updates."""
    entity_features = finalize_stateful_entity_feature_updates(
        entity_updates,
        entity_types,
    )
    output_path = batch_output_path(output_dir, run_id, batch_id)

    entity_features.write.mode("overwrite").parquet(str(output_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create checkpointed, stateful Entity feature updates from "
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
    cached_entity_types = (
        spark.read
        .parquet(
            str(
                args.static_metadata_dir
                / "node_entity_features.parquet"
            )
        )
        .select("entity_id", *ENTITY_TYPE_FEATURE_COLUMNS)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    cached_entity_types.count()
    entity_types = F.broadcast(cached_entity_types)

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
    entity_updates = build_stateful_entity_features(
        valid_account_events
    )

    try:
        query = (
            entity_updates.writeStream
            .outputMode("update")
            .foreachBatch(
                lambda batch_updates, batch_id: (
                    write_entity_feature_batch(
                        entity_updates=batch_updates,
                        batch_id=batch_id,
                        output_dir=args.output_dir,
                        run_id=args.run_id,
                        entity_types=entity_types,
                    )
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
        cached_entity_types.unpersist()
        entity_lookup.unpersist()

    print("Stateful Entity feature run complete.")
    print(f"Output directory: {args.output_dir / f'run_id={args.run_id}'}")


if __name__ == "__main__":
    main()
