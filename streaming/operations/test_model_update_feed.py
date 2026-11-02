"""The model feed must never expose partial or out-of-order packages."""

import json

import pytest

from streaming.operations.model_update_feed import ready_batches
from streaming.operations.write_stateful_run_report import EXPECTED_MODEL_TABLES


def audit(tmp_path, batch_id=0, valid=2, rejected=0):
    path = tmp_path / "metrics/run_id=run" / f"batch_id={batch_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    record = dict(run_id="run", batch_id=batch_id, validation_status="passed",
                  kafka_transaction_count=(valid + rejected) // 2,
                  expected_account_event_count=valid + rejected,
                  valid_account_event_count=valid, rejected_account_event_count=rejected)
    path.write_text(json.dumps(record))


def package(tmp_path, batch_id):
    directory = tmp_path / "outputs/run_id=run" / f"batch_id={batch_id}"
    directory.mkdir(parents=True, exist_ok=True)
    for table in EXPECTED_MODEL_TABLES:
        table_path = directory / f"{table}.parquet"
        table_path.mkdir()
        (table_path / "_SUCCESS").touch()
    (directory / "manifest.json").write_text(json.dumps(dict(
        run_id="run", batch_id=batch_id, status="ready",
        counts={name: 1 for name in EXPECTED_MODEL_TABLES},
        columns={name: ["key"] for name in EXPECTED_MODEL_TABLES},
    )))


def feed(tmp_path, after=-1):
    return list(ready_batches(tmp_path / "outputs", tmp_path / "metrics", "run", after))


def test_stops_at_incomplete_batch_even_when_later_package_exists(tmp_path):
    audit(tmp_path, 0)
    audit(tmp_path, 1)
    package(tmp_path, 1)
    assert feed(tmp_path) == []
    package(tmp_path, 0)
    assert [row["batch_id"] for row in feed(tmp_path)] == [0, 1]


def test_cursor_is_model_owned_and_failed_application_can_retry(tmp_path):
    audit(tmp_path)
    package(tmp_path, 0)
    assert feed(tmp_path) == feed(tmp_path)
    assert feed(tmp_path, after=0) == []


def test_rejected_batch_is_explicit_skip_followed_by_next_ready_batch(tmp_path):
    audit(tmp_path, 0, valid=0, rejected=2)
    audit(tmp_path, 1)
    package(tmp_path, 1)
    rows = feed(tmp_path)
    assert [row["status"] for row in rows] == ["skipped_no_valid_events", "ready"]
    assert sum(rows[0]["counts"].values()) == 0


def test_failed_audit_prevents_delivery(tmp_path):
    audit(tmp_path)
    package(tmp_path, 0)
    path = tmp_path / "metrics/run_id=run/batch_id=0.json"
    record = json.loads(path.read_text())
    record["validation_status"] = "failed"
    path.write_text(json.dumps(record))
    with pytest.raises(ValueError, match="did not pass"):
        feed(tmp_path)
