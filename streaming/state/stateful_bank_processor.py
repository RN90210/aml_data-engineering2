from __future__ import annotations

from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame
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
    directional_amounts,
    event_time_bounds,
    sum_numeric_column,
)

from preprocessing.spark_static_pipeline import BANK_FEATURE_COLUMNS


BANK_FEATURE_STATE_SCHEMA = StructType(
    [
        StructField("outgoing_txn_count", LongType(), False),
        StructField("incoming_txn_count", LongType(), False),
        StructField("outgoing_total_paid", DoubleType(), False),
        StructField(
            "incoming_total_received",
            DoubleType(),
            False,
        ),
        StructField(
            "sender_accounts",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "receiver_accounts",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "counterparty_banks",
            ArrayType(LongType(), True),
            True,
        ),
        StructField(
            "currencies",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "payment_formats",
            ArrayType(StringType(), True),
            True,
        ),
        StructField("cross_bank_sum", DoubleType(), False),
        StructField("odd_hour_sum", DoubleType(), False),
        StructField("weekend_sum", DoubleType(), False),
        StructField(
            "last_ordered_event_time",
            TimestampType(),
            True,
        ),
    ]
)


BANK_FEATURE_OUTPUT_SCHEMA = StructType(
    [
        StructField("bank_id", LongType(), False),
        StructField("outgoing_txn_count", DoubleType(), False),
        StructField("incoming_txn_count", DoubleType(), False),
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
        StructField(
            "unique_sender_accounts",
            DoubleType(),
            False,
        ),
        StructField(
            "unique_receiver_accounts",
            DoubleType(),
            False,
        ),
        StructField(
            "counterparty_bank_count",
            DoubleType(),
            False,
        ),
        StructField("currency_diversity", DoubleType(), False),
        StructField(
            "payment_format_diversity",
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
    ]
)


def string_values(
    events: pd.DataFrame,
    column_name: str,
) -> set[str]:
    """Return the non-empty distinct strings in a pandas column."""
    return {
        str(value)
        for value in events[column_name].dropna().tolist()
        if str(value) != ""
    }


def numeric_sum(events: pd.DataFrame, column_name: str) -> float:
    """Return a null-safe numeric sum for one event flag."""
    return sum_numeric_column(events, column_name)


def unpack_bank_key(key) -> int:
    """Return a Bank ID from Spark or a wrapped test key."""
    bank_id = key[0]
    if isinstance(bank_id, tuple):
        if len(bank_id) != 1:
            raise ValueError("Expected one Bank ID in the state key.")
        bank_id = bank_id[0]
    return int(bank_id)


class BankRunningTotalsProcessor(StatefulProcessor):
    """Maintain cumulative event-derived features for each Bank."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        """Register the compact cumulative Bank state with Spark."""
        self.running_state = handle.getValueState(
            "bank_feature_state",
            BANK_FEATURE_STATE_SCHEMA,
        )

    def handleInputRows(
        self,
        key,
        rows,
        timer_values,
    ) -> Iterator[pd.DataFrame]:
        """Update one Bank from chronologically ordered event rows."""
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
            sender_accounts = set(previous_state[4] or [])
            receiver_accounts = set(previous_state[5] or [])
            counterparty_banks = set(previous_state[6] or [])
            currencies = set(previous_state[7] or [])
            payment_formats = set(previous_state[8] or [])
            cross_bank_sum = float(previous_state[9])
            odd_hour_sum = float(previous_state[10])
            weekend_sum = float(previous_state[11])
            last_ordered_event_time = previous_state[12]
        else:
            outgoing_txn_count = 0
            incoming_txn_count = 0
            outgoing_total_paid = 0.0
            incoming_total_received = 0.0
            sender_accounts = set()
            receiver_accounts = set()
            counterparty_banks = set()
            currencies = set()
            payment_formats = set()
            cross_bank_sum = 0.0
            odd_hour_sum = 0.0
            weekend_sum = 0.0
            last_ordered_event_time = None

        batch_first_event_time, batch_last_event_time = event_time_bounds(events)
        if (
            last_ordered_event_time is not None
            and batch_first_event_time < last_ordered_event_time
        ):
            raise ValueError(
                "Stateful Bank features require chronological "
                "event-time input."
            )

        outgoing_events = events[
            events["direction"] == "outgoing"
        ]
        incoming_events = events[
            events["direction"] == "incoming"
        ]

        outgoing_amounts, incoming_amounts = directional_amounts(
            events, ["event_time","direction","participant_account_id"],
        )

        outgoing_txn_count += len(outgoing_events)
        incoming_txn_count += len(incoming_events)
        outgoing_total_paid += float(outgoing_amounts.sum())
        incoming_total_received += float(incoming_amounts.sum())

        sender_accounts.update(
            string_values(outgoing_events, "participant_account_id")
        )
        receiver_accounts.update(
            string_values(incoming_events, "participant_account_id")
        )
        counterparty_banks.update(
            int(value)
            for value in events["counterparty_bank_id"].dropna().tolist()
        )
        currencies.update(string_values(events, "currency"))
        payment_formats.update(
            string_values(events, "payment_format")
        )

        cross_bank_sum += numeric_sum(events, "is_cross_bank")
        odd_hour_sum += numeric_sum(events, "is_odd_hour")
        weekend_sum += numeric_sum(events, "is_weekend")
        last_ordered_event_time = batch_last_event_time

        self.running_state.update(
            (
                outgoing_txn_count,
                incoming_txn_count,
                outgoing_total_paid,
                incoming_total_received,
                sorted(sender_accounts),
                sorted(receiver_accounts),
                sorted(counterparty_banks),
                sorted(currencies),
                sorted(payment_formats),
                cross_bank_sum,
                odd_hour_sum,
                weekend_sum,
                last_ordered_event_time,
            )
        )

        total_event_count = outgoing_txn_count + incoming_txn_count
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

        yield pd.DataFrame(
            [
                {
                    "bank_id": unpack_bank_key(key),
                    "outgoing_txn_count": float(
                        outgoing_txn_count
                    ),
                    "incoming_txn_count": float(
                        incoming_txn_count
                    ),
                    "outgoing_total_paid": outgoing_total_paid,
                    "incoming_total_received": (
                        incoming_total_received
                    ),
                    "outgoing_avg_paid": outgoing_avg_paid,
                    "incoming_avg_received": (
                        incoming_avg_received
                    ),
                    "unique_sender_accounts": float(
                        len(sender_accounts)
                    ),
                    "unique_receiver_accounts": float(
                        len(receiver_accounts)
                    ),
                    "counterparty_bank_count": float(
                        len(counterparty_banks)
                    ),
                    "currency_diversity": float(len(currencies)),
                    "payment_format_diversity": float(
                        len(payment_formats)
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
                }
            ]
        )

    def close(self) -> None:
        """Spark calls this when the stateful processor stops."""
        pass


def build_stateful_bank_features(
    valid_account_events: DataFrame,
) -> DataFrame:
    """Build cumulative dynamic Bank updates from valid Account events."""
    bank_events = valid_account_events.select(
        "bank_id",
        "counterparty_bank_id",
        F.col("account_id").alias("participant_account_id"),
        "event_time",
        "direction",
        "amount",
        "currency",
        "payment_format",
        "is_cross_bank",
        "is_odd_hour",
        "is_weekend",
    )

    return (
        bank_events
        .groupBy("bank_id")
        .transformWithStateInPandas(
            statefulProcessor=BankRunningTotalsProcessor(),
            outputStructType=BANK_FEATURE_OUTPUT_SCHEMA,
            outputMode="Update",
            timeMode="None",
        )
    )


def finalize_stateful_bank_feature_updates(
    bank_updates: DataFrame,
    bank_account_counts: DataFrame,
) -> DataFrame:
    """Join fixed account counts and return the exact Bank contract."""
    return (
        bank_updates
        .join(
            bank_account_counts.select(
                "bank_id",
                "bank_account_count",
            ),
            on="bank_id",
            how="left",
        )
        .fillna({"bank_account_count": 0.0})
        .select(
            "bank_id",
            *[
                F.col(feature_name)
                .cast("double")
                .alias(feature_name)
                for feature_name in BANK_FEATURE_COLUMNS
            ],
        )
    )
