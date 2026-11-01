"""Bounded preparation tests with an unsorted miniature source."""

import csv

import pytest

from streaming.operations.prepare_replay_window import prepare_replay_window


def test_orders_and_bounds_duplicate_account_headers(tmp_path):
    source = tmp_path / "source.csv"
    target = tmp_path / "replay.csv"
    with source.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "Timestamp", "From Bank", "Account", "To Bank", "Account",
            "Amount Paid", "Payment Format",
        ])
        writer.writerows([
            ["2022/09/02 00:00", 1, "001", 1, "002", 1, "Wire"],
            ["2022/09/01 00:31", 1, "001", 1, "002", 2, "Wire"],
            ["2022/09/01 00:30", 10, "001", 1, "002", 3, "Wire"],
            ["2022/09/01 00:30", 2, "001", 1, "002", 4, "Wire"],
        ])
    assert prepare_replay_window(
        source, target, "2022/09/01 00:30", "2022/09/02 00:00", 2,
    ) == 2
    with target.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert [row["From Bank"] for row in rows] == ["2", "10"]
    assert rows[0]["Account"] == "001"
    assert rows[0]["Account.1"] == "002"
    with pytest.raises(FileExistsError):
        prepare_replay_window(source, target, "2022/09/01 00:30", "2022/09/02 00:00", 2)
    insufficient = tmp_path / "insufficient.csv"
    with pytest.raises(ValueError, match="Only 3"):
        prepare_replay_window(source, insufficient, "2022/09/01 00:30", "2022/09/02 00:00", 4)
    assert not insufficient.exists()
