"""Boundary arithmetic, hourly counts, and frame reuse regression checks."""

import pandas as pd
import pytest

from streaming.state._processor_batch import (
    combine_event_frames, consecutive_gap_delta, event_time_bounds, hourly_counts,
    directional_amounts,
    sum_numeric_column,
)


def test_single_frame_is_not_copied_and_multiple_frames_are_combined():
    first = pd.DataFrame({"amount": [1.0]}, index=[17])
    second = pd.DataFrame({"amount": [2.0]}, index=[17])
    assert combine_event_frames(iter([first])) is first
    assert combine_event_frames(iter([])) is None
    assert combine_event_frames(iter([first.iloc[:0]])) is None
    assert combine_event_frames(iter([first, second]))["amount"].tolist() == [1.0, 2.0]


@pytest.mark.parametrize("with_previous", [False, True])
@pytest.mark.parametrize("times", [
    ["2022-09-01 00:00:00.000001"],
    ["2022-09-01 02:00:00", "2022-09-01 00:00:00", "2022-09-01 02:00:00"],
    ["2022-09-01 01:00:00.000002", "2022-09-01 01:00:00.000001"],
])
def test_boundary_gaps_equal_consecutive_gaps(times, with_previous):
    events = pd.DataFrame({"event_time": pd.to_datetime(times, format="mixed")})
    before = events.copy(deep=True)
    first, last = event_time_bounds(events)
    previous = first - pd.Timedelta(hours=3) if with_previous else None
    sequence = sorted(events["event_time"].tolist())
    if previous is not None:
        sequence.insert(0, previous)
    expected = sum((right - left).total_seconds() / 3600 for left, right in zip(sequence, sequence[1:]))
    actual, count = consecutive_gap_delta(first, last, len(events), previous)
    assert actual == pytest.approx(expected, abs=1e-12)
    assert count == len(sequence) - 1
    counts = hourly_counts(events)
    assert sum(counts.values()) == len(events)
    expected_hours = {}
    for timestamp in events["event_time"]:
        key = timestamp.floor("h").isoformat()
        expected_hours[key] = expected_hours.get(key, 0) + 1
    assert counts == expected_hours
    pd.testing.assert_frame_equal(events, before)


def test_numeric_reduction_order_matches_previous_sort_with_duplicate_indices():
    events = pd.DataFrame({
        "event_time": pd.to_datetime(["2022-09-01"] * 4),
        "direction": ["outgoing", "incoming", "outgoing", "incoming"],
        "counterparty_account_id": ["B", "C", "A", "A"],
        "amount": [1e12, 3.0, 0.01, 4.0],
    }, index=[7, 7, 7, 7])
    columns = ["event_time", "direction", "counterparty_account_id"]
    expected = events.sort_values(columns, kind="stable")
    outgoing, incoming = directional_amounts(events, columns)
    assert outgoing.tolist() == expected.loc[expected["direction"] == "outgoing", "amount"].tolist()
    assert incoming.tolist() == expected.loc[expected["direction"] == "incoming", "amount"].tolist()


@pytest.mark.parametrize("values, expected", [
    ([1.0, None, 2.0], 3.0), (["1", "bad", None, "2"], 3.0),
    ([], 0.0), ([None, None], 0.0), ([True, False, True], 2.0),
])
def test_sum_preserves_null_and_coercion_semantics(values, expected):
    assert sum_numeric_column(pd.DataFrame({"value": values}), "value") == expected
