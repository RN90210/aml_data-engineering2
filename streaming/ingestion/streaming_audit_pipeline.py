from __future__ import annotations

import argparse
import json
import os
import sys
import time
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


def rejected_batch_output_path(
    rejected_output_dir: Path,
    run_id: str,
    batch_id: int,
) -> Path:
    """Return the deterministic rejected-event path for one batch."""
    return (
        rejected_output_dir
        / f"run_id={run_id}"
        / f"batch_id={batch_id}"
        / "rejected_account_events.parquet"
    )


def metrics_batch_output_path(
    metrics_output_dir: Path,
    run_id: str,
    batch_id: int,
) -> Path:
    """Return the deterministic JSON metrics path for one batch."""
    return (
        metrics_output_dir
        / f"run_id={run_id}"
        / f"batch_id={batch_id}.json"
    )


def write_audit_batch(
    batch_events: DataFrame,
    batch_id: int,
    run_id: str,
    rejected_output_dir: Path,
    metrics_output_dir: Path,
    totals: dict,
    release_input: bool = True,
) -> None:
    """Validate, retain rejections, and record metrics for one batch.

    A shared stateless-input owner may retain the input cache for another write
    using release_input=False; it must unpersist that input in its own finally.
    """
    started = time.perf_counter()
    batch_events = batch_events.persist(StorageLevel.MEMORY_AND_DISK)
    valid_events = None
    rejected_events = None

    try:
        valid_events, rejected_events = split_valid_and_rejected_events(
            batch_events
        )
        valid_events = valid_events.persist(StorageLevel.MEMORY_AND_DISK)
        rejected_events = rejected_events.persist(
            StorageLevel.MEMORY_AND_DISK
        )

        kafka_transactions = (
            batch_events
            .select(
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
            )
            .distinct()
        )
        kafka_transaction_count = kafka_transactions.count()
        valid_event_count = valid_events.count()
        rejected_event_count = rejected_events.count()
        expected_event_count = kafka_transaction_count * 2

        partition_offset_rows = (
            kafka_transactions
            .groupBy("kafka_topic", "kafka_partition")
            .agg(
                F.min("kafka_offset").alias("minimum_offset"),
                F.max("kafka_offset").alias("maximum_offset"),
            )
            .orderBy("kafka_topic", "kafka_partition")
            .collect()
        )
        partition_offsets = [
            {
                "kafka_topic": str(row["kafka_topic"]),
                "kafka_partition": int(row["kafka_partition"]),
                "minimum_offset": int(row["minimum_offset"]),
                "maximum_offset": int(row["maximum_offset"]),
            }
            for row in partition_offset_rows
        ]

        rejected_events.write.mode("overwrite").parquet(
            str(
                rejected_batch_output_path(
                    rejected_output_dir,
                    run_id,
                    batch_id,
                )
            )
        )

        validation_passed = (
            valid_event_count + rejected_event_count
            == expected_event_count
        )
        processing_seconds = round(
            time.perf_counter() - started,
            3,
        )

        metrics_record = {
            "run_id": run_id,
            "batch_id": batch_id,
            "kafka_transaction_count": kafka_transaction_count,
            "expected_account_event_count": expected_event_count,
            "valid_account_event_count": valid_event_count,
            "rejected_account_event_count": rejected_event_count,
            "processing_seconds": processing_seconds,
            "partition_offsets": partition_offsets,
            "validation_status": (
                "passed" if validation_passed else "failed"
            ),
        }

        metrics_path = metrics_batch_output_path(
            metrics_output_dir,
            run_id,
            batch_id,
        )
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_metrics_path = metrics_path.with_suffix(".json.tmp")
        temporary_metrics_path.write_text(
            json.dumps(metrics_record, indent=2),
            encoding="utf-8",
        )
        temporary_metrics_path.replace(metrics_path)

        totals["kafka_transactions"] += kafka_transaction_count
        totals["valid_events"] += valid_event_count
        totals["rejected_events"] += rejected_event_count
        totals["batches"] += 1
        totals["processing_seconds"] += processing_seconds
        totals["validation_passed"] = (
            totals["validation_passed"] and validation_passed
        )
    finally:
        if valid_events is not None:
            valid_events.unpersist()
        if rejected_events is not None:
            rejected_events.unpersist()
        if release_input:
            batch_events.unpersist()


def print_final_summary(totals: dict) -> None:
    """Print one compact summary after all available batches finish."""
    if not totals["validation_passed"]:
        overall_status = "FAILED"
    elif totals["rejected_events"] > 0:
        overall_status = "PASSED WITH REJECTIONS"
    else:
        overall_status = "PASSED"

    print("Kafka audit run complete.")
    print(
        "Kafka transactions processed: "
        f"{totals['kafka_transactions']:,}"
    )
    print(f"Valid Account events: {totals['valid_events']:,}")
    print(f"Rejected Account events: {totals['rejected_events']:,}")
    print(f"Batches completed: {totals['batches']:,}")
    print(
        "Total processing seconds: "
        f"{totals['processing_seconds']:.3f}"
    )
    print(f"Overall validation: {overall_status}")


def parse_args() -> argparse.Namespace:
    """Parse Kafka, lookup, output, and checkpoint locations."""
    parser = argparse.ArgumentParser(
        description=(
            "Audit Kafka Account events and write batch validation metrics."
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
        "--rejected-output-dir",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--metrics-output-dir",
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
    """Run the independent available-now Kafka audit stream."""
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

    totals = {
        "kafka_transactions": 0,
        "valid_events": 0,
        "rejected_events": 0,
        "batches": 0,
        "processing_seconds": 0.0,
        "validation_passed": True,
    }

    try:
        query = (
            account_events.writeStream
            .outputMode("append")
            .foreachBatch(
                lambda batch_events, batch_id: write_audit_batch(
                    batch_events=batch_events,
                    batch_id=batch_id,
                    run_id=args.run_id,
                    rejected_output_dir=args.rejected_output_dir,
                    metrics_output_dir=args.metrics_output_dir,
                    totals=totals,
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
        entity_lookup.unpersist()
    print_final_summary(totals)


if __name__ == "__main__":
    main()
