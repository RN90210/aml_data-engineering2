from __future__ import annotations

import math
from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import (
    StatefulProcessor,
    StatefulProcessorHandle,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.state._processor_batch import (
    combine_event_frames,
    consecutive_gap_delta,
    event_time_bounds,
    ordered_amounts,
    sum_numeric_column,
)

from preprocessing.spark_static_pipeline import (
    ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS,
)


EDGE_FEATURE_STATE_SCHEMA = StructType(
    [
        StructField("edge_txn_count", LongType(), False),
        StructField("edge_total_paid", DoubleType(), False),
        StructField("edge_amount_sum_squares", DoubleType(), False),
        StructField("currency_switch_sum", DoubleType(), False),
        StructField(
            "payment_formats",
            ArrayType(StringType(), True),
            True,
        ),
        StructField("first_event_time", TimestampType(), True),
        StructField("last_event_time", TimestampType(), True),
        StructField(
            "last_ordered_event_time",
            TimestampType(),
            True,
        ),
        StructField(
            "inter_txn_gap_hours_sum",
            DoubleType(),
            False,
        ),
        StructField("inter_txn_gap_count", LongType(), False),
    ]
)


EDGE_FEATURE_OUTPUT_SCHEMA = StructType(
    [
        StructField("sender_account_id", StringType(), False),
        StructField("receiver_account_id", StringType(), False),
        StructField("edge_txn_count", DoubleType(), False),
        StructField("edge_total_paid", DoubleType(), False),
        StructField("edge_avg_paid", DoubleType(), False),
        StructField("edge_amount_std", DoubleType(), False),
        StructField("edge_is_repeated", DoubleType(), False),
        StructField(
            "edge_currency_switch_ratio",
            DoubleType(),
            False,
        ),
        StructField(
            "edge_payment_format_diversity",
            DoubleType(),
            False,
        ),
        StructField(
            "edge_inter_txn_gap_hours",
            DoubleType(),
            False,
        ),
        StructField("last_event_time", TimestampType(), True),
    ]
)


def sample_standard_deviation(
    count: int,
    value_sum: float,
    value_sum_squares: float,
) -> float:
    """Calculate sample standard deviation from cumulative values."""
    if count <= 1:
        return 0.0

    variance = (
        value_sum_squares - ((value_sum * value_sum) / count)
    ) / (count - 1)
    return math.sqrt(max(variance, 0.0))

def unpack_edge_key(key) -> tuple[str, str]:
    """Support Spark's composite key and TwsTester's wrapped key."""
    if len(key) == 1 and isinstance(key[0], tuple):
        key = key[0]

    if len(key) != 2:
        raise ValueError(
            "Expected sender and receiver Account IDs in edge key."
        )

    return str(key[0]), str(key[1])

class EdgeRunningTotalsProcessor(StatefulProcessor):
    """Maintain directed Account-to-Account edge feature totals."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.running_state = handle.getValueState(
            "account_edge_feature_state",
            EDGE_FEATURE_STATE_SCHEMA,
        )

    def handleInputRows(
        self,
        key,
        rows,
        timer_values,
    ) -> Iterator[pd.DataFrame]:
        del timer_values

        events = combine_event_frames(rows)
        if events is None:
            return

        if self.running_state.exists():
            previous_state = self.running_state.get()
            edge_txn_count = int(previous_state[0])
            edge_total_paid = float(previous_state[1])
            edge_amount_sum_squares = float(previous_state[2])
            currency_switch_sum = float(previous_state[3])
            payment_formats = set(previous_state[4] or [])
            first_event_time = previous_state[5]
            last_event_time = previous_state[6]
            last_ordered_event_time = previous_state[7]
            inter_txn_gap_hours_sum = float(previous_state[8])
            inter_txn_gap_count = int(previous_state[9])
        else:
            edge_txn_count = 0
            edge_total_paid = 0.0
            edge_amount_sum_squares = 0.0
            currency_switch_sum = 0.0
            payment_formats = set()
            first_event_time = None
            last_event_time = None
            last_ordered_event_time = None
            inter_txn_gap_hours_sum = 0.0
            inter_txn_gap_count = 0

        batch_first_event_time, batch_last_event_time = event_time_bounds(events)
        if (
            last_ordered_event_time is not None
            and batch_first_event_time < last_ordered_event_time
        ):
            raise ValueError(
                "Stateful Account edge features require chronological "
                "event-time input."
            )

        amounts, _ = ordered_amounts(events, ["event_time"])
        edge_txn_count += len(events)
        edge_total_paid += float(amounts.sum())
        edge_amount_sum_squares += float((amounts * amounts).sum())
        currency_switch_sum += sum_numeric_column(events, "is_currency_switch")
        payment_formats.update(
            str(value)
            for value in events["payment_format"].dropna().tolist()
            if str(value) != ""
        )

        gap_sum, gap_count = consecutive_gap_delta(
            batch_first_event_time, batch_last_event_time,
            len(events), last_ordered_event_time,
        )
        inter_txn_gap_hours_sum += gap_sum
        inter_txn_gap_count += gap_count
        last_ordered_event_time = batch_last_event_time

        if (
            first_event_time is None
            or batch_first_event_time < first_event_time
        ):
            first_event_time = batch_first_event_time
        if (
            last_event_time is None
            or batch_last_event_time > last_event_time
        ):
            last_event_time = batch_last_event_time

        self.running_state.update(
            (
                edge_txn_count,
                edge_total_paid,
                edge_amount_sum_squares,
                currency_switch_sum,
                sorted(payment_formats),
                first_event_time,
                last_event_time,
                last_ordered_event_time,
                inter_txn_gap_hours_sum,
                inter_txn_gap_count,
            )
        )

        edge_avg_paid = edge_total_paid / edge_txn_count
        edge_inter_txn_gap_hours = (
            inter_txn_gap_hours_sum / inter_txn_gap_count
            if inter_txn_gap_count > 0
            else 0.0
        )

        sender_account_id, receiver_account_id = unpack_edge_key(key)

        yield pd.DataFrame(
            [
                {
                    "sender_account_id": sender_account_id,
                    "receiver_account_id": receiver_account_id,
                    "edge_txn_count": float(edge_txn_count),
                    "edge_total_paid": edge_total_paid,
                    "edge_avg_paid": edge_avg_paid,
                    "edge_amount_std": sample_standard_deviation(
                        edge_txn_count,
                        edge_total_paid,
                        edge_amount_sum_squares,
                    ),
                    "edge_is_repeated": (
                        1.0 if edge_txn_count > 1 else 0.0
                    ),
                    "edge_currency_switch_ratio": (
                        currency_switch_sum / edge_txn_count
                    ),
                    "edge_payment_format_diversity": float(
                        len(payment_formats)
                    ),
                    "edge_inter_txn_gap_hours": (
                        edge_inter_txn_gap_hours
                    ),
                    "last_event_time": last_event_time,
                }
            ]
        )

    def close(self) -> None:
        """Spark calls this when the stateful processor stops."""
        pass


def build_stateful_account_edges(
    account_events: DataFrame,
) -> DataFrame:
    """Produce cumulative directed edge updates for outgoing events."""
    outgoing_edges = (
        account_events
        .filter(F.col("direction") == "outgoing")
        .select(
            F.col("account_id").alias("sender_account_id"),
            F.col("counterparty_account_id").alias(
                "receiver_account_id"
            ),
            "amount",
            "event_time",
            "payment_format",
            "is_currency_switch",
        )
    )

    return (
        outgoing_edges
        .groupBy("sender_account_id", "receiver_account_id")
        .transformWithStateInPandas(
            statefulProcessor=EdgeRunningTotalsProcessor(),
            outputStructType=EDGE_FEATURE_OUTPUT_SCHEMA,
            outputMode="Update",
            timeMode="None",
        )
    )


def finalize_stateful_account_edge_updates(
    account_edge_updates: DataFrame,
) -> DataFrame:
    """Add batch recency and return the exact directed-edge contract."""
    return (
        account_edge_updates
        .withColumn(
            "_snapshot_time",
            F.max("last_event_time").over(Window.partitionBy()),
        )
        .withColumn(
            "edge_recency_hours",
            (
                F.col("_snapshot_time").cast("long")
                - F.col("last_event_time").cast("long")
            )
            / F.lit(3600.0),
        )
        .select(
            "sender_account_id",
            "receiver_account_id",
            *[
                F.col(feature_name)
                .cast("double")
                .alias(feature_name)
                for feature_name in (
                    ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS
                )
            ],
        )
    )
