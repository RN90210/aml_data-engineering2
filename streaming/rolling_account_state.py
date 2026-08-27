from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.spark_kafka_consumer import build_spark_session


ACCOUNT_EVENT_SCHEMA = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("counterparty_account_id", StringType(), False),
        StructField("bank_id", LongType(), False),
        StructField("counterparty_bank_id", LongType(), False),
        StructField("direction", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("payment_format", StringType(), False),
        StructField("is_currency_switch", IntegerType(), False),
        StructField("is_cross_bank", IntegerType(), False),
        StructField("is_odd_hour", IntegerType(), False),
        StructField("is_weekend", IntegerType(), False),
        StructField("is_self_transfer", IntegerType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False),
        StructField("kafka_ingested_at", TimestampType(), False),
        StructField("entity_id", StringType(), False),
        StructField("event_date", DateType(), True),
    ]
)


def build_rolling_account_state(events: DataFrame) -> DataFrame:
    """
    Maintain cumulative account features across micro-batches.

    Spark stores the intermediate state in the checkpoint directory and emits
    an updated row only for accounts touched by the current micro-batch.
    """
    state = (
        events
        .groupBy("account_id", "entity_id", "bank_id")
        .agg(
            F.count(F.lit(1)).alias("total_event_count"),
            F.sum(
                F.when(F.col("direction") == "outgoing", 1).otherwise(0)
            ).alias("outgoing_txn_count"),
            F.sum(
                F.when(F.col("direction") == "incoming", 1).otherwise(0)
            ).alias("incoming_txn_count"),
            F.sum(
                F.when(F.col("direction") == "outgoing", F.col("amount"))
                .otherwise(F.lit(0.0))
            ).alias("outgoing_total_paid"),
            F.sum(
                F.when(F.col("direction") == "incoming", F.col("amount"))
                .otherwise(F.lit(0.0))
            ).alias("incoming_total_received"),
            F.sum("is_cross_bank").alias("cross_bank_event_count"),
            F.sum("is_odd_hour").alias("odd_hour_event_count"),
            F.sum("is_weekend").alias("weekend_event_count"),
            F.sum("is_self_transfer").alias("self_transfer_event_count"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
        )
    )

    return (
        state
        .withColumn(
            "outgoing_avg_paid",
            F.when(
                F.col("outgoing_txn_count") > 0,
                F.col("outgoing_total_paid")
                / F.col("outgoing_txn_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "incoming_avg_received",
            F.when(
                F.col("incoming_txn_count") > 0,
                F.col("incoming_total_received")
                / F.col("incoming_txn_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "cross_bank_txn_ratio",
            F.col("cross_bank_event_count") / F.col("total_event_count"),
        )
        .withColumn(
            "odd_hour_txn_ratio",
            F.col("odd_hour_event_count") / F.col("total_event_count"),
        )
        .withColumn(
            "weekend_txn_ratio",
            F.col("weekend_event_count") / F.col("total_event_count"),
        )
        .withColumn(
            "self_transfer_ratio",
            F.col("self_transfer_event_count")
            / F.col("total_event_count"),
        )
    )


def write_state_updates(
    account_state: DataFrame,
    batch_id: int,
    output_dir: Path,
) -> None:
    """Write changed account-state rows for one micro-batch."""
    batch_dir = output_dir / f"batch_id={batch_id}"

    if batch_dir.exists():
        print(f"Skipping already-written state batch {batch_id}.")
        return

    (
        account_state
        .withColumn("state_batch_id", F.lit(batch_id))
        .write
        .mode("overwrite")
        .parquet(str(batch_dir))
    )

    print(f"Saved rolling account-state updates: {batch_dir}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build rolling account state from streaming account events."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/streaming/account_events_v3"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/streaming/account_state_updates_v1"),
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=Path("data/checkpoints/rolling_account_state_v1"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    event_stream = (
        spark.readStream
        .schema(ACCOUNT_EVENT_SCHEMA)
        .parquet(str(args.input_dir))
    )

    rolling_state = build_rolling_account_state(event_stream)

    def process_batch(batch_state: DataFrame, batch_id: int) -> None:
        write_state_updates(
            account_state=batch_state,
            batch_id=batch_id,
            output_dir=args.output_dir,
        )

    query = (
        rolling_state.writeStream
        .outputMode("update")
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(args.checkpoint_dir))
        .trigger(availableNow=True)
        .start()
    )

    print("Building rolling state from all currently available event files...")
    query.awaitTermination()
    print(f"Completed. State updates written to: {args.output_dir}")


if __name__ == "__main__":
    main()