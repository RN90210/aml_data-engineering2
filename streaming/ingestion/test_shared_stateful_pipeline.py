"""Delivery/control regressions; actual native state is checked by replay tests."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace

import pytest

from streaming.ingestion import shared_stateful_streaming_pipeline as shared


def settings(tmp_path):
    return SimpleNamespace(
        topic="test-topic", bootstrap_servers="test:9092", run_id="test-run",
        max_offsets_per_trigger=500, concurrent_feature_jobs=2, feature_shuffle_partitions=1,
        **{name: tmp_path / name for name in (
            "account_entity_lookup_path", "static_metadata_dir", "output_dir",
            "metrics_output_dir", "rejected_output_dir", "checkpoint_dir",
        )},
    )


def staged_runner(tmp_path):
    args = settings(tmp_path)
    runner = shared.SharedBatchRunner(None, args, None, None, None)
    prepared = runner.events_root / "batch_id=0"
    prepared.mkdir()
    (prepared / "_SUCCESS").touch()
    (prepared / "part-0.parquet").touch()
    shared.atomic_json(shared.metrics_batch_output_path(args.metrics_output_dir, args.run_id, 0), {
        "validation_status": "passed", "valid_account_event_count": 2,
    })
    return runner


def test_identity_rejects_other_run_and_legacy_checkpoint(tmp_path):
    args = settings(tmp_path)
    shared.initialize_run(args)
    shared.initialize_run(args)
    args.topic = "different-topic"
    with pytest.raises(ValueError, match="identity differs"):
        shared.initialize_run(args)
    args.checkpoint_dir = tmp_path / "legacy"
    args.checkpoint_dir.mkdir()
    (args.checkpoint_dir / "metadata").touch()
    with pytest.raises(ValueError, match="fresh checkpoint"):
        shared.initialize_run(args)


def test_receipt_requires_state_commit_and_finished_output(tmp_path):
    runner = staged_runner(tmp_path)
    shared.atomic_json(runner.receipt_path(0, "account"), {
        "batch_id": 0, "component": "account", "query_batch_id": 7,
    })
    output = runner.batch_directory(0) / "account_features.parquet"
    output.mkdir(parents=True)
    (output / "_SUCCESS").touch()
    assert not runner.component_complete(0, "account")
    commit = runner.args.checkpoint_dir / "account/commits/7"
    commit.parent.mkdir(parents=True)
    commit.touch()
    assert runner.component_complete(0, "account")


def test_ready_manifest_waits_for_every_component(tmp_path, monkeypatch):
    runner = staged_runner(tmp_path)
    entered, release, packaged = Event(), Event(), Event()

    def component(name, batch_id):
        if name == "entity":
            entered.set()
            assert release.wait(5)

    monkeypatch.setattr(runner, "run_component", component)
    monkeypatch.setattr(shared, "package_batch", lambda *args: packaged.set())
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(runner, None, 0)
        try:
            assert entered.wait(5)
            assert not packaged.is_set()
        finally:
            release.set()
        future.result()
    assert packaged.is_set()


def test_component_failure_never_publishes_ready(tmp_path, monkeypatch):
    runner = staged_runner(tmp_path)
    packaged = []

    def fail(name, batch_id):
        if name == "bank":
            raise RuntimeError("injected component failure")

    monkeypatch.setattr(runner, "run_component", fail)
    monkeypatch.setattr(shared, "package_batch", lambda *args: packaged.append(args))
    with pytest.raises(RuntimeError, match="injected"):
        runner(None, 0)
    assert packaged == []
    assert not (runner.args.checkpoint_dir / "completed_batches/batch_id=0.json").exists()


def test_rejected_only_batch_skips_features_and_package(tmp_path, monkeypatch):
    runner = staged_runner(tmp_path)
    metrics = shared.metrics_batch_output_path(runner.args.metrics_output_dir, runner.args.run_id, 0)
    shared.atomic_json(metrics, {"validation_status": "passed", "valid_account_event_count": 0})
    monkeypatch.setattr(runner, "run_component", lambda *args: pytest.fail("must skip"))
    monkeypatch.setattr(shared, "package_batch", lambda *args: pytest.fail("must skip"))
    runner(None, 0)
    completion = runner.args.checkpoint_dir / "completed_batches/batch_id=0.json"
    assert shared.read_json(completion)["status"] == "skipped_no_valid_events"


def test_completed_retry_does_not_rewrite_package(tmp_path, monkeypatch):
    runner = staged_runner(tmp_path)
    manifest = runner.batch_directory(0) / "manifest.json"
    shared.atomic_json(manifest, {"status": "ready"})
    completion = runner.args.checkpoint_dir / "completed_batches/batch_id=0.json"
    shared.atomic_json(completion, {"batch_id": 0, "status": "ready"})
    monkeypatch.setattr(runner, "run_component", lambda *args: pytest.fail("must skip"))
    monkeypatch.setattr(shared, "package_batch", lambda *args: pytest.fail("must skip"))
    before = manifest.stat().st_mtime_ns
    runner(None, 0)
    assert manifest.stat().st_mtime_ns == before


def test_input_is_published_atomically_with_current_batch_identity(tmp_path, monkeypatch):
    args = settings(tmp_path)
    runner = shared.SharedBatchRunner(None, args, None, None, None)
    observed = []

    class Events:
        def persist(self, level):
            return self

        def unpersist(self):
            pass

        def coalesce(self, partitions):
            return self

        @property
        def write(self):
            return self

        def mode(self, mode):
            return self

        def parquet(self, location):
            # Simulate a writer still constructing its data. Nothing in this
            # location may be visible to the live native source queries yet.
            path = shared.Path(location)
            assert runner.pending_batch_id == 3
            assert runner.events_root not in path.parents
            assert not list(runner.events_root.glob("*/*.parquet"))
            path.mkdir(parents=True)
            (path / "part-0.parquet").touch()
            (path / "_SUCCESS").touch()

    def audit(*args, **kwargs):
        shared.atomic_json(shared.metrics_batch_output_path(
            runner.args.metrics_output_dir, runner.args.run_id, 3),
            {"validation_status": "passed", "valid_account_event_count": 2})

    def consume(name, batch_id):
        assert batch_id == runner.pending_batch_id == 3
        assert (runner.events_root / "batch_id=3/_SUCCESS").is_file()
        observed.append(name)

    events = Events()
    monkeypatch.setattr(shared, "write_audit_batch", audit)
    monkeypatch.setattr(shared, "split_valid_and_rejected_events", lambda frame: (frame, None))
    monkeypatch.setattr(runner, "run_component", consume)
    monkeypatch.setattr(shared, "package_batch", lambda *args: None)
    runner(events, 3)
    assert set(observed) == set(shared.COMPONENT_TABLES)
