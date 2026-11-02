"""Validate stateful replay packages against cumulatively rebuilt features."""

from __future__ import annotations

import argparse
import os
import sys
from functools import reduce
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

if sys.platform == "win32":
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import LongType, StructField, StructType

from preprocessing.spark_static_pipeline import (
    ACCOUNT_FEATURE_COLUMNS,
    ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS,
    BANK_FEATURE_COLUMNS,
    ENTITY_FEATURE_COLUMNS,
    TRANSACTION_SCHEMA,
)
from streaming.ingestion.spark_streaming_pipeline import (
    add_transaction_flags,
    add_transaction_rejection_reason,
    build_account_events,
    enrich_transaction_entities,
    load_entity_lookup,
    split_valid_and_rejected_events,
)
from streaming.state.incremental_account_edges import (
    build_incremental_account_edges,
)
from streaming.state.incremental_account_features import (
    build_incremental_account_features,
)
from streaming.state.incremental_bank_entity_features import (
    build_incremental_bank_features,
    build_incremental_entity_features,
)
from streaming.state.materialize_batch_update import (
    build_incremental_relations,
)


NUMERIC_TOLERANCE = 1e-6

EXPECTED_TABLE_COLUMNS: Dict[str, List[str]] = {
    "account_features": ["account_id", *ACCOUNT_FEATURE_COLUMNS],
    "bank_features": ["bank_id", *BANK_FEATURE_COLUMNS],
    "entity_features": ["entity_id", *ENTITY_FEATURE_COLUMNS],
    "account_to_account_edges": [
        "sender_account_id",
        "receiver_account_id",
        *ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS,
    ],
    "account_to_bank": ["account_id", "bank_id"],
    "entity_to_account": ["entity_id", "account_id"],
}

TABLE_KEYS: Dict[str, List[str]] = {
    "account_features": ["account_id"],
    "bank_features": ["bank_id"],
    "entity_features": ["entity_id"],
    "account_to_account_edges": [
        "sender_account_id",
        "receiver_account_id",
    ],
    "account_to_bank": ["account_id", "bank_id"],
    "entity_to_account": ["entity_id", "account_id"],
}


def parse_args() -> argparse.Namespace:
    """Parse validation inputs."""
    parser = argparse.ArgumentParser(
        description=(
            "Validate every incremental stateful replay package against "
            "features rebuilt through the same replay batch."
        )
    )
    parser.add_argument("--replay-path", type=Path, required=True)
    parser.add_argument(
        "--account-entity-lookup-path",
        type=Path,
        required=True,
    )
    parser.add_argument("--static-output-dir", type=Path, required=True)
    parser.add_argument(
        "--model-updates-output-dir",
        type=Path,
        required=True,
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        required=True,
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    """Create the local Spark session used only for replay validation."""
    return (
        SparkSession.builder
        .appName("ValidateStatefulReplay")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
    )


def add_input_row_offsets(
    spark: SparkSession,
    replay_rows: DataFrame,
) -> DataFrame:
    """Assign Kafka-like offsets from the replay DataFrame's input-row order."""
    indexed_schema = StructType(
        list(replay_rows.schema.fields)
        + [StructField("_input_row_offset", LongType(), nullable=False)]
    )
    indexed_rows = replay_rows.rdd.zipWithIndex().map(
        lambda row_and_index: (
            tuple(row_and_index[0]) + (row_and_index[1],)
        )
    )
    return spark.createDataFrame(indexed_rows, schema=indexed_schema)


def build_replay_account_events(
    spark: SparkSession,
    replay_path: Path,
    entity_lookup: DataFrame,
    run_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """Build valid Account events and return the original replay offsets."""
    replay_rows = (
        spark.read
        .option("header", "true")
        .schema(TRANSACTION_SCHEMA)
        .csv(str(replay_path))
    )
    indexed_rows = add_input_row_offsets(spark, replay_rows)
    replay_offsets = indexed_rows.select(
        F.col("_input_row_offset").alias("kafka_offset")
    )

    parsed_transactions = (
        indexed_rows
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("Timestamp"), "yyyy/MM/dd HH:mm"),
        )
        .withColumn(
            "payment_currency",
            F.col("Payment Currency"),
        )
        .withColumn("payment_format", F.col("Payment Format"))
        .withColumn("kafka_topic", F.lit(f"replay:{run_id}"))
        .withColumn("kafka_partition", F.lit(0).cast("int"))
        .withColumn(
            "kafka_ingested_at",
            F.col("event_time").cast("timestamp"),
        )
        .withColumnRenamed("_input_row_offset", "kafka_offset")
    )

    flagged_transactions = add_transaction_flags(parsed_transactions)
    enriched_transactions = enrich_transaction_entities(
        flagged_transactions,
        entity_lookup,
    )
    validated_transactions = add_transaction_rejection_reason(
        enriched_transactions
    )
    account_events = build_account_events(validated_transactions)
    valid_events, _ = split_valid_and_rejected_events(account_events)
    return valid_events, replay_offsets


def expected_tables_for_batch(
    cumulative_events: DataFrame,
    current_events: DataFrame,
    static_output_dir: Path,
) -> Dict[str, DataFrame]:
    """Build six expected update tables with validated incremental helpers."""
    account_to_bank, entity_to_account = build_incremental_relations(
        current_events
    )

    return {
        "account_features": build_incremental_account_features(
            cumulative_events,
            current_events,
        ),
        "bank_features": build_incremental_bank_features(
            cumulative_events,
            current_events,
            static_output_dir,
        ),
        "entity_features": build_incremental_entity_features(
            cumulative_events,
            current_events,
            static_output_dir,
        ),
        "account_to_account_edges": build_incremental_account_edges(
            cumulative_events,
            current_events,
        ),
        "account_to_bank": account_to_bank,
        "entity_to_account": entity_to_account,
    }


def has_rows(rows: DataFrame) -> bool:
    """Return whether a DataFrame has at least one row."""
    return rows.limit(1).count() != 0


def has_duplicate_keys(
    rows: DataFrame,
    key_columns: Sequence[str],
) -> bool:
    """Return whether any complete table key occurs more than once."""
    return has_rows(
        rows.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )


def compare_table(
    expected: DataFrame,
    actual: DataFrame,
    table_name: str,
) -> Tuple[bool, str]:
    """Compare schema order, exact keys, and numeric values for one table."""
    expected_columns = EXPECTED_TABLE_COLUMNS[table_name]
    key_columns = TABLE_KEYS[table_name]

    if expected.columns != expected_columns:
        return False, "expected builder produced an unexpected column order"
    if actual.columns != expected_columns:
        return False, "actual column order differs"

    if has_duplicate_keys(expected, key_columns):
        return False, "expected table contains duplicate complete keys"
    if has_duplicate_keys(actual, key_columns):
        return False, "actual table contains duplicate complete keys"

    expected_keys = expected.select(*key_columns).distinct()
    actual_keys = actual.select(*key_columns).distinct()
    if has_rows(expected_keys.subtract(actual_keys)):
        return False, "actual table is missing expected keys"
    if has_rows(actual_keys.subtract(expected_keys)):
        return False, "actual table contains unexpected keys"

    numeric_columns = [
        column
        for column in expected_columns
        if column not in key_columns
    ]
    if not numeric_columns:
        return True, ""

    joined = expected.alias("expected").join(
        actual.alias("actual"),
        on=key_columns,
        how="inner",
    )
    mismatch_conditions = []
    for column in numeric_columns:
        expected_value = F.col(f"expected.`{column}`").cast("double")
        actual_value = F.col(f"actual.`{column}`").cast("double")
        expected_nan = F.isnan(expected_value)
        actual_nan = F.isnan(actual_value)
        mismatch_conditions.append(
            (expected_value.isNull() != actual_value.isNull())
            | (
                expected_value.isNotNull()
                & actual_value.isNotNull()
                & (
                    (expected_nan != actual_nan)
                    | (
                        ~expected_nan
                        & ~actual_nan
                        & (
                            F.abs(expected_value - actual_value)
                            > F.lit(NUMERIC_TOLERANCE)
                        )
                    )
                )
            )
        )

    any_mismatch = reduce(lambda left, right: left | right, mismatch_conditions)
    if has_rows(joined.filter(any_mismatch)):
        return False, f"numeric values differ by more than {NUMERIC_TOLERANCE}"
    return True, ""


def validate_replay(args: argparse.Namespace) -> bool:
    """Validate every reconstructed replay batch and return overall success."""
    if args.max_offsets_per_trigger <= 0:
        raise ValueError("--max-offsets-per-trigger must be greater than zero")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    overall_passed = True

    try:
        entity_lookup = load_entity_lookup(
            spark,
            args.account_entity_lookup_path,
        )
        valid_events, replay_offsets = build_replay_account_events(
            spark,
            args.replay_path,
            entity_lookup,
            args.run_id,
        )
        valid_events = valid_events.persist(StorageLevel.MEMORY_AND_DISK)
        valid_events.count()

        maximum_offset_row = replay_offsets.agg(
            F.max("kafka_offset").alias("maximum_offset")
        ).first()
        maximum_offset = maximum_offset_row["maximum_offset"]
        if maximum_offset is None:
            raise ValueError(f"Replay contains no transaction rows: {args.replay_path}")

        batch_count = (
            int(maximum_offset) // args.max_offsets_per_trigger
        ) + 1
        run_directory = (
            args.model_updates_output_dir / f"run_id={args.run_id}"
        )

        for batch_id in range(batch_count):
            first_offset = batch_id * args.max_offsets_per_trigger
            last_offset = first_offset + args.max_offsets_per_trigger - 1
            cumulative_events = valid_events.filter(
                F.col("kafka_offset") <= F.lit(last_offset)
            )
            current_events = cumulative_events.filter(
                F.col("kafka_offset") >= F.lit(first_offset)
            )
            # The shared pipeline publishes no model package when every event
            # in this Kafka batch was rejected. Verify that case from the replay
            # itself, rather than assuming an absent package means success.
            batch_directory = run_directory / f"batch_id={batch_id}"
            if not batch_directory.exists() and not has_rows(current_events):
                for table_name in TABLE_KEYS:
                    print(f"batch={batch_id} table={table_name} PASS (no valid events; no package)")
                continue
            expected_tables = expected_tables_for_batch(
                cumulative_events,
                current_events,
                args.static_output_dir,
            )

            for table_name, expected in expected_tables.items():
                table_path = (
                    run_directory
                    / f"batch_id={batch_id}"
                    / f"{table_name}.parquet"
                )
                expected = expected.persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    actual = spark.read.parquet(str(table_path))
                    passed, reason = compare_table(
                        expected,
                        actual,
                        table_name,
                    )
                except Exception as error:
                    passed = False
                    reason = str(error).splitlines()[0]
                finally:
                    expected.unpersist()

                if passed:
                    print(f"batch={batch_id} table={table_name} PASS")
                else:
                    overall_passed = False
                    print(
                        f"batch={batch_id} table={table_name} FAIL: {reason}"
                    )
    finally:
        if "valid_events" in locals():
            valid_events.unpersist()
        if "entity_lookup" in locals():
            entity_lookup.unpersist()
        spark.stop()

    print(f"overall={'PASS' if overall_passed else 'FAIL'}")
    return overall_passed


def main() -> None:
    """Run replay validation and return a failing process status on mismatch."""
    if not validate_replay(parse_args()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
