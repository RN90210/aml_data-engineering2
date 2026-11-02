from __future__ import annotations

import math
from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.streaming import (
    StatefulProcessor,
    StatefulProcessorHandle,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.state._processor_batch import (
    combine_event_frames,
    directional_amounts,
    consecutive_gap_delta,
    event_time_bounds,
    hourly_counts,
    sum_numeric_column,
)

from preprocessing.spark_static_pipeline import (
    ACCOUNT_FEATURE_COLUMNS,
)


ACCOUNT_FEATURE_STATE_SCHEMA = StructType(
    [
        StructField("outgoing_txn_count", LongType(), False),
        StructField("incoming_txn_count", LongType(), False),
        StructField("outgoing_total_paid", DoubleType(), False),
        StructField("incoming_total_received", DoubleType(), False),
        StructField("outgoing_amount_sum_squares", DoubleType(), False),
        StructField("incoming_amount_sum_squares", DoubleType(), False),
        StructField("total_event_count", LongType(), False),
        StructField("self_transfer_sum", DoubleType(), False),
        StructField("currency_switch_sum", DoubleType(), False),
        StructField("cross_bank_sum", DoubleType(), False),
        StructField("odd_hour_sum", DoubleType(), False),
        StructField("weekend_sum", DoubleType(), False),
        StructField("first_event_time", TimestampType(), True),
        StructField("last_event_time", TimestampType(), True),
        StructField(
            "outgoing_counterparties",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "incoming_counterparties",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "outgoing_currencies",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "incoming_currencies",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "payment_formats",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "counterparty_banks",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "hourly_event_counts",
            MapType(StringType(), LongType(), True),
            True,
        ),
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


ACCOUNT_FEATURE_OUTPUT_SCHEMA = StructType(
    [
        StructField("account_id", StringType(), False),
        StructField("outgoing_txn_count", DoubleType(), False),
        StructField("incoming_txn_count", DoubleType(), False),
        StructField(
            "outgoing_unique_counterparties",
            DoubleType(),
            False,
        ),
        StructField(
            "incoming_unique_counterparties",
            DoubleType(),
            False,
        ),
        StructField(
            "repeat_counterparty_ratio",
            DoubleType(),
            False,
        ),
        StructField("self_transfer_ratio", DoubleType(), False),
        StructField("outgoing_total_paid", DoubleType(), False),
        StructField(
            "incoming_total_received",
            DoubleType(),
            False,
        ),
        StructField("outgoing_avg_paid", DoubleType(), False),
        StructField(
            "incoming_avg_received",
            DoubleType(),
            False,
        ),
        StructField("outgoing_amount_std", DoubleType(), False),
        StructField("incoming_amount_std", DoubleType(), False),
        StructField(
            "payment_currency_diversity",
            DoubleType(),
            False,
        ),
        StructField(
            "receiving_currency_diversity",
            DoubleType(),
            False,
        ),
        StructField(
            "currency_switch_ratio",
            DoubleType(),
            False,
        ),
        StructField(
            "payment_format_diversity",
            DoubleType(),
            False,
        ),
        StructField(
            "unique_connected_banks",
            DoubleType(),
            False,
        ),
        StructField(
            "cross_bank_txn_ratio",
            DoubleType(),
            False,
        ),
        StructField("odd_hour_txn_ratio", DoubleType(), False),
        StructField("weekend_txn_ratio", DoubleType(), False),
        StructField("active_span_hours", DoubleType(), False),
        StructField("max_txns_per_hour", DoubleType(), False),
        StructField(
            "avg_inter_txn_gap_hours",
            DoubleType(),
            False,
        ),
        StructField("first_event_time", TimestampType(), True),
        StructField("last_event_time", TimestampType(), True),
    ]
)


def numeric_sum(events: pd.DataFrame, column_name: str) -> float:
    """Safely sum a numeric column; missing test columns count as zero."""
    if column_name not in events.columns:
        return 0.0

    return sum_numeric_column(events, column_name)


def string_values(
    events: pd.DataFrame,
    column_name: str,
) -> set[str]:
    """Return non-null distinct string values from one event column."""
    if column_name not in events.columns:
        return set()

    return {
        str(value)
        for value in events[column_name].dropna().tolist()
        if str(value) != ""
    }


def sample_standard_deviation(
    count: int,
    value_sum: float,
    value_sum_squares: float,
) -> float:
    """Calculate exact sample standard deviation from running values."""
    if count <= 1:
        return 0.0

    variance = (
        value_sum_squares
        - ((value_sum * value_sum) / count)
    ) / (count - 1)

    return math.sqrt(max(variance, 0.0))


def hour_key(event_time) -> str:
    """Create a stable key for one event-time hour bucket."""
    return pd.Timestamp(event_time).floor("h").isoformat()


class AccountRunningTotalsProcessor(StatefulProcessor):
    """
    Maintain all Account feature ingredients across Spark micro-batches.

    Spark checkpoints this compact state under account_id. A later transaction
    for an Account updates only that Account's saved state.
    """

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.running_state = handle.getValueState(
            "account_feature_state",
            ACCOUNT_FEATURE_STATE_SCHEMA,
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

            outgoing_txn_count = int(previous_state[0])
            incoming_txn_count = int(previous_state[1])
            outgoing_total_paid = float(previous_state[2])
            incoming_total_received = float(previous_state[3])
            outgoing_amount_sum_squares = float(previous_state[4])
            incoming_amount_sum_squares = float(previous_state[5])
            total_event_count = int(previous_state[6])
            self_transfer_sum = float(previous_state[7])
            currency_switch_sum = float(previous_state[8])
            cross_bank_sum = float(previous_state[9])
            odd_hour_sum = float(previous_state[10])
            weekend_sum = float(previous_state[11])
            first_event_time = previous_state[12]
            last_event_time = previous_state[13]
            outgoing_counterparties = set(previous_state[14] or [])
            incoming_counterparties = set(previous_state[15] or [])
            outgoing_currencies = set(previous_state[16] or [])
            incoming_currencies = set(previous_state[17] or [])
            payment_formats = set(previous_state[18] or [])
            counterparty_banks = set(previous_state[19] or [])
            hourly_event_counts = dict(previous_state[20] or {})
            last_ordered_event_time = previous_state[21]
            inter_txn_gap_hours_sum = float(previous_state[22])
            inter_txn_gap_count = int(previous_state[23])
        else:
            outgoing_txn_count = 0
            incoming_txn_count = 0
            outgoing_total_paid = 0.0
            incoming_total_received = 0.0
            outgoing_amount_sum_squares = 0.0
            incoming_amount_sum_squares = 0.0
            total_event_count = 0
            self_transfer_sum = 0.0
            currency_switch_sum = 0.0
            cross_bank_sum = 0.0
            odd_hour_sum = 0.0
            weekend_sum = 0.0
            first_event_time = None
            last_event_time = None
            outgoing_counterparties = set()
            incoming_counterparties = set()
            outgoing_currencies = set()
            incoming_currencies = set()
            payment_formats = set()
            counterparty_banks = set()
            hourly_event_counts = {}
            last_ordered_event_time = None
            inter_txn_gap_hours_sum = 0.0
            inter_txn_gap_count = 0

        outgoing_events = events[
            events["direction"] == "outgoing"
        ]
        incoming_events = events[
            events["direction"] == "incoming"
        ]

        outgoing_amounts, incoming_amounts = directional_amounts(
            events, ["event_time","direction","counterparty_account_id"],
        )

        outgoing_txn_count += len(outgoing_events)
        incoming_txn_count += len(incoming_events)
        total_event_count += len(events)

        outgoing_total_paid += float(outgoing_amounts.sum())
        incoming_total_received += float(incoming_amounts.sum())

        outgoing_amount_sum_squares += float(
            (outgoing_amounts * outgoing_amounts).sum()
        )
        incoming_amount_sum_squares += float(
            (incoming_amounts * incoming_amounts).sum()
        )

        self_transfer_sum += numeric_sum(
            events,
            "is_self_transfer",
        )
        currency_switch_sum += numeric_sum(
            events,
            "is_currency_switch",
        )
        cross_bank_sum += numeric_sum(events, "is_cross_bank")
        odd_hour_sum += numeric_sum(events, "is_odd_hour")
        weekend_sum += numeric_sum(events, "is_weekend")

        outgoing_counterparties.update(
            string_values(
                outgoing_events,
                "counterparty_account_id",
            )
        )
        incoming_counterparties.update(
            string_values(
                incoming_events,
                "counterparty_account_id",
            )
        )
        outgoing_currencies.update(
            string_values(outgoing_events, "currency")
        )
        incoming_currencies.update(
            string_values(incoming_events, "currency")
        )
        payment_formats.update(
            string_values(events, "payment_format")
        )
        counterparty_banks.update(
            string_values(events, "counterparty_bank_id")
        )

        batch_first_event_time, batch_last_event_time = event_time_bounds(events)

        if (
            last_ordered_event_time is not None
            and batch_first_event_time < last_ordered_event_time
        ):
            raise ValueError(
                "Stateful Account features require chronological "
                "event-time input."
            )

        for bucket, count in hourly_counts(events).items():
            hourly_event_counts[bucket] = hourly_event_counts.get(bucket, 0) + count

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
                outgoing_txn_count,
                incoming_txn_count,
                outgoing_total_paid,
                incoming_total_received,
                outgoing_amount_sum_squares,
                incoming_amount_sum_squares,
                total_event_count,
                self_transfer_sum,
                currency_switch_sum,
                cross_bank_sum,
                odd_hour_sum,
                weekend_sum,
                first_event_time,
                last_event_time,
                sorted(outgoing_counterparties),
                sorted(incoming_counterparties),
                sorted(outgoing_currencies),
                sorted(incoming_currencies),
                sorted(payment_formats),
                sorted(counterparty_banks),
                hourly_event_counts,
                last_ordered_event_time,
                inter_txn_gap_hours_sum,
                inter_txn_gap_count,
            )
        )

        unique_counterparties = (
            outgoing_counterparties | incoming_counterparties
        )

        outgoing_avg_paid = (
            outgoing_total_paid / outgoing_txn_count
            if outgoing_txn_count > 0
            else 0.0
        )
        incoming_avg_received = (
            incoming_total_received / incoming_txn_count
            if incoming_txn_count > 0
            else 0.0
        )
        active_span_hours = (
            (
                pd.Timestamp(last_event_time)
                - pd.Timestamp(first_event_time)
            ).total_seconds()
            / 3600.0
        )
        max_txns_per_hour = max(
            hourly_event_counts.values(),
            default=0,
        )
        avg_inter_txn_gap_hours = (
            inter_txn_gap_hours_sum / inter_txn_gap_count
            if inter_txn_gap_count > 0
            else 0.0
        )

        yield pd.DataFrame(
            [
                {
                    "account_id": str(key[0]),
                    "outgoing_txn_count": float(
                        outgoing_txn_count
                    ),
                    "incoming_txn_count": float(
                        incoming_txn_count
                    ),
                    "outgoing_unique_counterparties": float(
                        len(outgoing_counterparties)
                    ),
                    "incoming_unique_counterparties": float(
                        len(incoming_counterparties)
                    ),
                    "repeat_counterparty_ratio": (
                        total_event_count
                        / len(unique_counterparties)
                        if unique_counterparties
                        else 0.0
                    ),
                    "self_transfer_ratio": (
                        self_transfer_sum / total_event_count
                    ),
                    "outgoing_total_paid": outgoing_total_paid,
                    "incoming_total_received": (
                        incoming_total_received
                    ),
                    "outgoing_avg_paid": outgoing_avg_paid,
                    "incoming_avg_received": (
                        incoming_avg_received
                    ),
                    "outgoing_amount_std": (
                        sample_standard_deviation(
                            outgoing_txn_count,
                            outgoing_total_paid,
                            outgoing_amount_sum_squares,
                        )
                    ),
                    "incoming_amount_std": (
                        sample_standard_deviation(
                            incoming_txn_count,
                            incoming_total_received,
                            incoming_amount_sum_squares,
                        )
                    ),
                    "payment_currency_diversity": float(
                        len(outgoing_currencies)
                    ),
                    "receiving_currency_diversity": float(
                        len(incoming_currencies)
                    ),
                    "currency_switch_ratio": (
                        currency_switch_sum / total_event_count
                    ),
                    "payment_format_diversity": float(
                        len(payment_formats)
                    ),
                    "unique_connected_banks": float(
                        len(counterparty_banks)
                    ),
                    "cross_bank_txn_ratio": (
                        cross_bank_sum / total_event_count
                    ),
                    "odd_hour_txn_ratio": (
                        odd_hour_sum / total_event_count
                    ),
                    "weekend_txn_ratio": (
                        weekend_sum / total_event_count
                    ),
                    "active_span_hours": active_span_hours,
                    "max_txns_per_hour": float(
                        max_txns_per_hour
                    ),
                    "avg_inter_txn_gap_hours": (
                        avg_inter_txn_gap_hours
                    ),
                    "first_event_time": first_event_time,
                    "last_event_time": last_event_time,
                }
            ]
        )

    def close(self) -> None:
        """Spark calls this when the stateful processor stops."""
        pass


def build_stateful_account_features(
    account_events: DataFrame,
) -> DataFrame:
    """
    Produce running Account feature rows for Accounts touched by each
    streaming micro-batch.
    """
    return (
        account_events
        .select(
            "account_id",
            "counterparty_account_id",
            "counterparty_bank_id",
            "direction",
            "amount",
            "currency",
            "payment_format",
            "event_time",
            "is_self_transfer",
            "is_currency_switch",
            "is_cross_bank",
            "is_odd_hour",
            "is_weekend",
        )
        .groupBy("account_id")
        .transformWithStateInPandas(
            statefulProcessor=AccountRunningTotalsProcessor(),
            outputStructType=ACCOUNT_FEATURE_OUTPUT_SCHEMA,
            outputMode="Update",
            timeMode="None",
        )
    )


def finalize_stateful_account_feature_updates(
    account_feature_updates: DataFrame,
) -> DataFrame:
    """Add batch-time recency and return the exact 24-feature contract."""
    return (
        account_feature_updates
        .withColumn(
            "_snapshot_time",
            F.max("last_event_time").over(Window.partitionBy()),
        )
        .withColumn(
            "hours_since_last_txn",
            (
                F.col("_snapshot_time").cast("long")
                - F.col("last_event_time").cast("long")
            )
            / F.lit(3600.0),
        )
        .select(
            "account_id",
            *[
                F.col(feature_name)
                .cast("double")
                .alias(feature_name)
                for feature_name in ACCOUNT_FEATURE_COLUMNS
            ],
        )
    )
