"""Batch operations shared by the exact native stateful feature processors."""

from __future__ import annotations

import pandas as pd
import numpy as np


def combine_event_frames(rows):
    """Reuse Spark's single frame, concatenating only when it supplies several."""
    frames = [frame for frame in rows if not frame.empty]
    if not frames:
        return None
    return frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)


def event_time_bounds(events):
    """Find timestamp bounds without ordering an otherwise commutative batch."""
    times = events["event_time"]
    if len(times) == 1:
        timestamp = pd.Timestamp(times.iloc[0])
        return timestamp, timestamp
    return pd.Timestamp(times.min()), pd.Timestamp(times.max())


def ordered_amounts(events, sort_columns):
    """Order only the numeric vector, preserving the old floating-point sums.

    All sort fields are non-null in validated events. lexsort is stable and
    takes its primary key last, so reverse the existing pandas sort columns.
    Integer positions also work when an incoming frame has duplicate indices.
    """
    amounts = events["amount"]
    if not pd.api.types.is_numeric_dtype(amounts.dtype):
        amounts = pd.to_numeric(amounts, errors="coerce")
    amounts = amounts.fillna(0.0)
    if len(events) <= 1:
        return amounts, events["direction"].to_numpy() if "direction" in events else None
    positions = np.lexsort(tuple(events[column].to_numpy() for column in reversed(sort_columns)))
    directions = events["direction"].to_numpy()[positions] if "direction" in events else None
    return amounts.iloc[positions], directions


def directional_amounts(events, sort_columns):
    amounts, directions = ordered_amounts(events, sort_columns)
    return amounts[directions == "outgoing"], amounts[directions == "incoming"]


def sum_numeric_column(events, column):
    """Sum already numeric Spark columns directly; preserve coercion for objects."""
    values = events[column]
    if not pd.api.types.is_numeric_dtype(values.dtype):
        values = pd.to_numeric(values, errors="coerce")
    # pandas sum skips nulls and returns zero for an all-null/empty column,
    # equivalent to filling nulls with zero without allocating that copy.
    return float(values.sum())


def consecutive_gap_delta(first, last, count, previous):
    """Return the exact chronological gap contribution from batch boundaries.

    Adjacent differences telescope. With previous state there are count new
    gaps; without it there are count - 1. Equal timestamps still add gaps of
    zero length. Callers must reject first < previous before updating state.
    """
    start = pd.Timestamp(previous) if previous is not None else first
    return (last - start).total_seconds() / 3600.0, count - (previous is None)


def hourly_counts(events):
    """Count events per hour in bulk while retaining existing ISO state keys."""
    times = events["event_time"]
    if len(times) <= 4:
        # Constructing a vectorized grouping costs more for these tiny keys.
        counts = {}
        for timestamp in times:
            bucket = pd.Timestamp(timestamp).floor("h").isoformat()
            counts[bucket] = counts.get(bucket, 0) + 1
        return counts
    buckets = pd.DatetimeIndex(times).floor("h").value_counts(sort=False)
    return {bucket.isoformat(): int(count) for bucket, count in buckets.items()}
