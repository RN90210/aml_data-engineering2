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

from preprocessing.spark_static_pipeline import ENTITY_FEATURE_COLUMNS


ENTITY_TYPE_FEATURE_COLUMNS = [
    feature_name
    for feature_name in ENTITY_FEATURE_COLUMNS
    if feature_name.startswith("entity_type_")
]


ENTITY_FEATURE_STATE_SCHEMA = StructType(
    [
        StructField(
            "owned_account_ids",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "connected_bank_ids",
            ArrayType(LongType(), True),
            True,
        ),
        StructField("outgoing_txn_count", LongType(), False),
        StructField("incoming_txn_count", LongType(), False),
        StructField("outgoing_total_paid", DoubleType(), False),
        StructField(
            "incoming_total_received",
            DoubleType(),
            False,
        ),
        StructField(
            "counterparty_account_ids",
            ArrayType(StringType(), True),
            True,
        ),
        StructField("cross_bank_sum", DoubleType(), False),
        StructField("odd_hour_sum", DoubleType(), False),
        StructField(
            "payment_formats",
            ArrayType(StringType(), True),
            True,
        ),
        StructField(
            "last_ordered_event_time",
            TimestampType(),
            True,
        ),
    ]
)


ENTITY_FEATURE_OUTPUT_SCHEMA = StructType(
    [
        StructField("entity_id", StringType(), False),
        StructField("owned_account_count", DoubleType(), False),
        StructField("connected_bank_count", DoubleType(), False),
        StructField("outgoing_txn_count", DoubleType(), False),
        StructField("incoming_txn_count", DoubleType(), False),
        StructField("outgoing_total_paid", DoubleType(), False),
        StructField(
            "incoming_total_received",
            DoubleType(),
            False,
        ),
        StructField(
            "unique_counterparty_accounts",
            DoubleType(),
            False,
        ),
        StructField(
            "cross_bank_txn_ratio",
            DoubleType(),
            False,
        ),
        StructField("odd_hour_txn_ratio", DoubleType(), False),
        StructField(
            "payment_format_diversity",
            DoubleType(),
            False,
        ),
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


def unpack_entity_key(key) -> str:
    """Return an Entity ID from Spark or a wrapped test key."""
    entity_id = key[0]
    if isinstance(entity_id, tuple):
        if len(entity_id) != 1:
            raise ValueError("Expected one Entity ID in the state key.")
        entity_id = entity_id[0]
    return str(entity_id)


class EntityRunningTotalsProcessor(StatefulProcessor):
    """Maintain cumulative transaction-derived features per Entity."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        """Register the compact cumulative Entity state with Spark."""
        self.running_state = handle.getValueState(
            "entity_feature_state",
            ENTITY_FEATURE_STATE_SCHEMA,
        )

    def handleInputRows(
        self,
        key,
        rows,
        timer_values,
    ) -> Iterator[pd.DataFrame]:
        """Update one Entity from chronologically ordered event rows."""
        del timer_values

        events = combine_event_frames(rows)
        if events is None:
            return

        if self.running_state.exists():
            previous_state = self.running_state.get()
            owned_account_ids = set(previous_state[0] or [])
            connected_bank_ids = set(previous_state[1] or [])
            outgoing_txn_count = int(previous_state[2])
            incoming_txn_count = int(previous_state[3])
            outgoing_total_paid = float(previous_state[4])
            incoming_total_received = float(previous_state[5])
            counterparty_account_ids = set(previous_state[6] or [])
            cross_bank_sum = float(previous_state[7])
            odd_hour_sum = float(previous_state[8])
            payment_formats = set(previous_state[9] or [])
            last_ordered_event_time = previous_state[10]
        else:
            owned_account_ids = set()
            connected_bank_ids = set()
            outgoing_txn_count = 0
            incoming_txn_count = 0
            outgoing_total_paid = 0.0
            incoming_total_received = 0.0
            counterparty_account_ids = set()
            cross_bank_sum = 0.0
            odd_hour_sum = 0.0
            payment_formats = set()
            last_ordered_event_time = None

        batch_first_event_time, batch_last_event_time = event_time_bounds(events)
        if (
            last_ordered_event_time is not None
            and batch_first_event_time < last_ordered_event_time
        ):
            raise ValueError(
                "Stateful Entity features require chronological "
                "event-time input."
            )

        outgoing_events = events[
            events["direction"] == "outgoing"
        ]
        incoming_events = events[
            events["direction"] == "incoming"
        ]
        outgoing_amounts, incoming_amounts = directional_amounts(
            events, ["event_time","direction","account_id"],
        )

        owned_account_ids.update(string_values(events, "account_id"))
        connected_bank_ids.update(
            int(value)
            for value in events["bank_id"].dropna().tolist()
        )
        outgoing_txn_count += len(outgoing_events)
        incoming_txn_count += len(incoming_events)
        outgoing_total_paid += float(outgoing_amounts.sum())
        incoming_total_received += float(incoming_amounts.sum())
        counterparty_account_ids.update(
            string_values(events, "counterparty_account_id")
        )
        cross_bank_sum += numeric_sum(events, "is_cross_bank")
        odd_hour_sum += numeric_sum(events, "is_odd_hour")
        payment_formats.update(
            string_values(events, "payment_format")
        )
        last_ordered_event_time = batch_last_event_time

        self.running_state.update(
            (
                sorted(owned_account_ids),
                sorted(connected_bank_ids),
                outgoing_txn_count,
                incoming_txn_count,
                outgoing_total_paid,
                incoming_total_received,
                sorted(counterparty_account_ids),
                cross_bank_sum,
                odd_hour_sum,
                sorted(payment_formats),
                last_ordered_event_time,
            )
        )

        total_event_count = outgoing_txn_count + incoming_txn_count

        yield pd.DataFrame(
            [
                {
                    "entity_id": unpack_entity_key(key),
                    "owned_account_count": float(
                        len(owned_account_ids)
                    ),
                    "connected_bank_count": float(
                        len(connected_bank_ids)
                    ),
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
                    "unique_counterparty_accounts": float(
                        len(counterparty_account_ids)
                    ),
                    "cross_bank_txn_ratio": (
                        cross_bank_sum / total_event_count
                    ),
                    "odd_hour_txn_ratio": (
                        odd_hour_sum / total_event_count
                    ),
                    "payment_format_diversity": float(
                        len(payment_formats)
                    ),
                }
            ]
        )

    def close(self) -> None:
        """Spark calls this when the stateful processor stops."""
        pass


def build_stateful_entity_features(
    valid_account_events: DataFrame,
) -> DataFrame:
    """Build cumulative dynamic Entity updates from valid events."""
    entity_events = valid_account_events.select(
        "entity_id",
        "bank_id",
        "account_id",
        "counterparty_account_id",
        "event_time",
        "direction",
        "amount",
        "payment_format",
        "is_cross_bank",
        "is_odd_hour",
    )

    return (
        entity_events
        .groupBy("entity_id")
        .transformWithStateInPandas(
            statefulProcessor=EntityRunningTotalsProcessor(),
            outputStructType=ENTITY_FEATURE_OUTPUT_SCHEMA,
            outputMode="Update",
            timeMode="None",
        )
    )


def finalize_stateful_entity_feature_updates(
    entity_updates: DataFrame,
    entity_types: DataFrame,
) -> DataFrame:
    """Join fixed Entity types and return the exact feature contract."""
    type_defaults = {
        feature_name: 0.0
        for feature_name in ENTITY_TYPE_FEATURE_COLUMNS
    }

    return (
        entity_updates
        .join(
            entity_types.select(
                "entity_id",
                *ENTITY_TYPE_FEATURE_COLUMNS,
            ),
            on="entity_id",
            how="left",
        )
        .fillna(type_defaults)
        .select(
            "entity_id",
            *[
                F.col(feature_name)
                .cast("double")
                .alias(feature_name)
                for feature_name in ENTITY_FEATURE_COLUMNS
            ],
        )
    )
