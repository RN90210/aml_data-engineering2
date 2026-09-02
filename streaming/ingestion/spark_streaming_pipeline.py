from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.kafka.spark_kafka_consumer import (
    build_spark_session,
    parse_transaction_stream,
)
from streaming.state.materialize_batch_update import (
    write_batch_update,
)
from streaming.materialization.materialize_bank_entity_features import (
    load_bank_account_counts,
    load_entity_types,
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
    """Create transaction-level flags from the fixed feature contract."""
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


def enrich_transaction_entities(
    transactions: DataFrame,
    entity_lookup: DataFrame,
) -> DataFrame:
    """Attach sender and receiver Entity IDs with static lookups."""
    sender_lookup = entity_lookup.select(
        F.col("account_id").alias("_sender_lookup_account_id"),
        F.col("entity_id").alias("sender_entity_id"),
    )
    receiver_lookup = entity_lookup.select(
        F.col("account_id").alias("_receiver_lookup_account_id"),
        F.col("entity_id").alias("receiver_entity_id"),
    )

    return (
        transactions
        .join(
            sender_lookup,
            F.col("sender_account_id")
            == F.col("_sender_lookup_account_id"),
            "left",
        )
        .drop("_sender_lookup_account_id")
        .join(
            receiver_lookup,
            F.col("receiver_account_id")
            == F.col("_receiver_lookup_account_id"),
            "left",
        )
        .drop("_receiver_lookup_account_id")
    )


def add_transaction_rejection_reason(
    transactions: DataFrame,
) -> DataFrame:
    """Add one atomic validation reason for both transaction sides."""
    return transactions.withColumn(
        "_transaction_rejection_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("Account").isNull()
                | F.col("`Account.1`").isNull(),
                F.lit("missing_account_id"),
            ),
            F.when(
                F.col("From Bank").isNull()
                | F.col("To Bank").isNull(),
                F.lit("missing_bank_id"),
            ),
            F.when(
                F.col("sender_entity_id").isNull()
                | F.col("receiver_entity_id").isNull(),
                F.lit("missing_entity_id"),
            ),
            F.when(
                F.col("event_time").isNull(),
                F.lit("missing_event_time"),
            ),
            F.when(
                F.col("Amount Paid").isNull()
                | F.col("Amount Received").isNull(),
                F.lit("missing_amount"),
            ),
            F.when(
                F.col("payment_currency").isNull()
                | F.col("Receiving Currency").isNull(),
                F.lit("missing_currency"),
            ),
            F.when(
                F.col("payment_format").isNull(),
                F.lit("missing_payment_format"),
            ),
        ),
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
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_ingested_at",
        "_transaction_rejection_reason",
    ]

    outgoing_event = F.struct(
        F.col("sender_account_id").alias("account_id"),
        F.col("receiver_account_id").alias(
            "counterparty_account_id"
        ),
        F.col("From Bank").alias("bank_id"),
        F.col("To Bank").alias("counterparty_bank_id"),
        F.col("sender_entity_id").alias("entity_id"),
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
        F.col("receiver_entity_id").alias("entity_id"),
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


def load_entity_lookup(
    spark: SparkSession,
    account_entity_lookup_path: Path,
) -> DataFrame:
    """Load stable Account-to-Entity metadata for streaming enrichment."""
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


def split_valid_and_rejected_events(
    events: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Separate valid events from rejected events.

    Rejected events retain their Kafka topic, partition, offset, event
    direction, and rejection reason for later debugging.
    """
    valid_events = (
        events
        .filter(F.col("_transaction_rejection_reason") == "")
        .drop("_transaction_rejection_reason")
    )

    rejected_events = (
        events
        .filter(F.col("_transaction_rejection_reason") != "")
        .withColumn(
            "rejection_reason",
            F.col("_transaction_rejection_reason"),
        )
        .drop("_transaction_rejection_reason")
    )

    return valid_events, rejected_events


def batch_output_path(
    output_dir: Path,
    run_id: str,
    batch_id: int,
) -> Path:
    """Create a deterministic output path for one micro-batch."""
    return (
        output_dir
        / f"run_id={run_id}"
        / f"batch_id={batch_id}"
    )


def write_microbatch(
    batch_events: DataFrame,
    batch_id: int,
    run_id: str,
    valid_output_dir: Path,
    rejected_output_dir: Path,
    metrics_output_dir: Path,
    static_metadata_dir: Path,
    model_updates_output_dir: Path,
    bank_account_counts: DataFrame,
    entity_types: DataFrame,
) -> None:
    """
    Process one Kafka micro-batch without terminal logging.

    Each batch writes:
    - valid Account events;
    - rejected Account events, if any;
    - one compact metrics record;
    - one model-ready incremental feature package, if valid events exist.
    """
    batch_started_at = datetime.now(timezone.utc).isoformat()
    started = time.perf_counter()

    event_write_seconds = 0.0
    model_update_seconds = 0.0

    batch_events = batch_events.persist(StorageLevel.MEMORY_AND_DISK)
    valid_events = None
    rejected_events = None

    try:
        valid_events, rejected_events = split_valid_and_rejected_events(
            batch_events
        )

        valid_events = valid_events.persist(
            StorageLevel.MEMORY_AND_DISK
        )
        rejected_events = rejected_events.persist(
            StorageLevel.MEMORY_AND_DISK
        )

        kafka_transaction_count = (
            batch_events
            .select(
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
            )
            .distinct()
            .count()
        )

        valid_event_count = valid_events.count()
        rejected_event_count = rejected_events.count()
        expected_event_count = kafka_transaction_count * 2

        event_write_started = time.perf_counter()

        if valid_event_count > 0:
            (
                valid_events.write
                .mode("overwrite")
                .partitionBy("event_date")
                .parquet(
                    str(
                        batch_output_path(
                            valid_output_dir,
                            run_id,
                            batch_id,
                        )
                    )
                )
            )

        if rejected_event_count > 0:
            (
                rejected_events.write
                .mode("overwrite")
                .partitionBy("event_date")
                .parquet(
                    str(
                        batch_output_path(
                            rejected_output_dir,
                            run_id,
                            batch_id,
                        )
                    )
                )
            )

        event_write_seconds = round(
            time.perf_counter() - event_write_started,
            3,
        )

        model_update_counts = {}

        if valid_event_count > 0:
            model_update_started = time.perf_counter()
            state_events = None

            try:
                state_events = (
                    batch_events.sparkSession.read
                    .parquet(
                        str(
                            valid_output_dir
                            / f"run_id={run_id}"
                        )
                    )
                    .filter(
                        F.col("batch_id").cast("long")
                        <= F.lit(batch_id).cast("long")
                    )
                    .persist(StorageLevel.MEMORY_AND_DISK)
                )

                model_update_counts = write_batch_update(
                    state_events=state_events,
                    current_batch_events=valid_events,
                    static_output_dir=static_metadata_dir,
                    output_root=model_updates_output_dir,
                    run_id=run_id,
                    batch_id=batch_id,
                    bank_account_counts=bank_account_counts,
                    entity_types=entity_types,
                )
            finally:
                if state_events is not None:
                    state_events.unpersist()

            model_update_seconds = round(
                time.perf_counter() - model_update_started,
                3,
            )

        processing_seconds = round(
            time.perf_counter() - started,
            3,
        )

        validation_status = (
            "passed"
            if valid_event_count + rejected_event_count
            == expected_event_count
            else "failed"
        )

        metrics_path = (
            metrics_output_dir
            / f"run_id={run_id}"
            / f"batch_id={batch_id}.json"
        )
        metrics_path.parent.mkdir(parents=True, exist_ok=True)

        metrics_record = {
            "run_id": run_id,
            "batch_id": batch_id,
            "batch_started_at": batch_started_at,
            "kafka_transaction_count": kafka_transaction_count,
            "expected_account_event_count": expected_event_count,
            "valid_account_event_count": valid_event_count,
            "rejected_account_event_count": rejected_event_count,
            "event_write_seconds": event_write_seconds,
            "model_update_seconds": model_update_seconds,
            "processing_seconds": processing_seconds,
            "validation_status": validation_status,
            "model_update_status": (
                "written"
                if valid_event_count > 0
                else "skipped_no_valid_events"
            ),
            "model_update_counts": model_update_counts,
        }

        temporary_metrics_path = metrics_path.with_suffix(
            ".json.tmp"
        )
        temporary_metrics_path.write_text(
            json.dumps(metrics_record, indent=2),
            encoding="utf-8",
        )
        temporary_metrics_path.replace(metrics_path)

    finally:
        if valid_events is not None:
            valid_events.unpersist()

        if rejected_events is not None:
            rejected_events.unpersist()

        batch_events.unpersist()

def print_final_summary(
    metrics_output_dir: Path,
    run_id: str,
) -> None:
    """Print one concise summary after every available batch has finished."""
    run_metrics_dir = (
        metrics_output_dir / f"run_id={run_id}"
    )
    metrics_files = sorted(
        run_metrics_dir.glob("batch_id=*.json")
    )

    if not metrics_files:
        raise FileNotFoundError(
            "No batch metrics were written for run: "
            f"{run_id}"
        )

    metrics_records = [
        json.loads(
            metrics_file.read_text(encoding="utf-8")
        )
        for metrics_file in metrics_files
    ]

    kafka_transaction_count = sum(
        record["kafka_transaction_count"]
        for record in metrics_records
    )
    valid_account_event_count = sum(
        record["valid_account_event_count"]
        for record in metrics_records
    )
    rejected_account_event_count = sum(
        record["rejected_account_event_count"]
        for record in metrics_records
    )
    total_processing_seconds = sum(
        record["processing_seconds"]
        for record in metrics_records
    )
    failed_batch_count = sum(
        record["validation_status"] == "failed"
        for record in metrics_records
    )
    model_update_batch_count = sum(
        record.get("model_update_status") == "written"
        for record in metrics_records
    )

    if failed_batch_count == 0:
        overall_status = (
            "PASSED"
            if rejected_account_event_count == 0
            else "PASSED WITH REJECTIONS"
        )
    else:
        overall_status = "FAILED"

    print("\nRun complete")
    print(
        "Kafka transactions processed: "
        f"{kafka_transaction_count:,}"
    )
    print(
        "Valid Account events: "
        f"{valid_account_event_count:,}"
    )
    print(
        "Rejected Account events: "
        f"{rejected_account_event_count:,}"
    )
    print(
        "Micro-batches completed: "
        f"{len(metrics_records):,}"
    )
    print(
        "Model-update packages written: "
        f"{model_update_batch_count:,}"
    )
    print(
        "Processing time across batches: "
        f"{total_processing_seconds:.3f} seconds"
    )
    print(f"Overall validation: {overall_status}")

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
        "--run-id",
        required=True,
        help=(
            "Unique name for this replay or continuous-processing run. "
            "Used to organize event and metrics output."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/streaming/account_events"),
        help="Parent directory for valid Account events.",
    )
    parser.add_argument(
        "--rejected-output-dir",
        type=Path,
        default=Path("data/streaming/rejected_account_events"),
        help="Parent directory for rejected Account events.",
    )
    parser.add_argument(
        "--metrics-output-dir",
        type=Path,
        default=Path("data/streaming/metrics/account_event_batches"),
        help="Parent directory for one compact metrics record per batch.",
    )
    parser.add_argument(
        "--static-metadata-dir",
        type=Path,
        default=Path("data/processed/spark_static_full"),
        help=(
            "Static metadata output. Only Bank account counts and Entity "
            "type columns are read from this directory."
        ),
    )
    parser.add_argument(
        "--model-updates-output-dir",
        type=Path,
        default=Path("data/streaming/model_updates"),
        help="Parent directory for model-ready incremental batch packages.",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=Path("data/checkpoints/account_event_pipeline"),
        help="Spark checkpoint directory for this streaming query.",
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

    if not args.static_metadata_dir.exists():
        raise FileNotFoundError(
            "Static metadata directory not found: "
            f"{args.static_metadata_dir}"
        )

    entity_lookup = load_entity_lookup(
        spark,
        args.account_entity_lookup_path,
    )

    # These are fixed reference values, so load them once for the whole
    # streaming run rather than rereading their Parquet files every batch.
    bank_account_counts = (
        load_bank_account_counts(args.static_metadata_dir)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    entity_types = (
        load_entity_types(args.static_metadata_dir)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
        print("Caching stable Bank and Entity metadata...")

        # This triggers the one-time load now. Later micro-batches reuse it.
        bank_account_counts.count()
        entity_types.count()

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

        account_events = (
            build_account_events(validated_transactions)
            .withColumn("event_date", F.to_date("event_time"))
        )

        query = (
            account_events.writeStream
            .foreachBatch(
                lambda batch_events, batch_id: write_microbatch(
                    batch_events=batch_events,
                    batch_id=batch_id,
                    run_id=args.run_id,
                    valid_output_dir=args.output_dir,
                    rejected_output_dir=args.rejected_output_dir,
                    metrics_output_dir=args.metrics_output_dir,
                    static_metadata_dir=args.static_metadata_dir,
                    model_updates_output_dir=(
                        args.model_updates_output_dir
                    ),
                    bank_account_counts=bank_account_counts,
                    entity_types=entity_types,
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

        print_final_summary(
            metrics_output_dir=args.metrics_output_dir,
            run_id=args.run_id,
        )
    finally:
        bank_account_counts.unpersist()
        entity_types.unpersist()


if __name__ == "__main__":
    main()
