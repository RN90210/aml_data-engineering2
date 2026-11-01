"""Tests for the MuleDetector replay DAG."""

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

from airflow.models import DagBag


EXPECTED_TASK_IDS = {
    "build_run_config",
    "prepare_replay_window",
    "publish_replay_to_kafka",
    "process_account_features",
    "process_account_edges",
    "process_bank_features",
    "process_entity_features",
    "process_account_relations",
    "audit_replay",
    "package_model_updates",
    "validate_replay",
    "write_run_report",
}


@contextmanager
def suppress_airflow_logs():
    """Keep normal DAG parsing logs out of pytest output."""
    logger = logging.getLogger("airflow")
    previous_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = previous_value


def load_dag_bag() -> DagBag:
    """Load the project DAGs in the same way Airflow discovers them."""
    with suppress_airflow_logs():
        return DagBag(include_examples=False)


def test_dags_import_without_errors():
    """Every project DAG must parse without an import error."""
    dag_bag = load_dag_bag()

    assert not dag_bag.import_errors, (
        f"DAG import errors: {dag_bag.import_errors}"
    )


def test_muledetector_replay_contract():
    """The replay DAG must retain its required serial orchestration shape."""
    dag = load_dag_bag().dags.get("muledetector_replay")

    assert dag is not None
    assert dag.max_active_runs == 1
    assert dag.default_args.get("retries") == 0
    assert set(dag.task_ids) == EXPECTED_TASK_IDS
    ordered_tasks = [
        "build_run_config",
        "prepare_replay_window",
        "publish_replay_to_kafka",
        "process_account_features",
        "process_account_edges",
        "process_bank_features",
        "process_entity_features",
        "process_account_relations",
        "audit_replay",
        "package_model_updates",
        "validate_replay",
        "write_run_report",
    ]
    for upstream, downstream in zip(ordered_tasks, ordered_tasks[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_native_tasks_launch_their_project_commands(monkeypatch):
    """A native Airflow task must execute its Spark command, not schedule a helper."""
    dag = load_dag_bag().dags["muledetector_replay"]
    commands = []
    account_task = dag.get_task("process_account_features")
    monkeypatch.setitem(
        account_task.python_callable.__globals__,
        "run_project_command",
        lambda name, command: commands.append((name, command)),
    )
    config = {
        "topic": "test-topic",
        "run_id": "test-run",
        "model_updates_dir": "/tmp/model-updates",
        "checkpoint_root": "/tmp/checkpoints/test-run",
        "max_offsets_per_trigger": "500",
    }
    expected_modules = {
        "process_account_features": (
            "account_features",
            "streaming.ingestion.stateful_account_streaming_pipeline",
        ),
        "process_account_edges": (
            "account_edges",
            "streaming.ingestion.stateful_edge_streaming_pipeline",
        ),
        "process_bank_features": (
            "bank_features",
            "streaming.ingestion.stateful_bank_streaming_pipeline",
        ),
        "process_entity_features": (
            "entity_features",
            "streaming.ingestion.stateful_entity_streaming_pipeline",
        ),
        "process_account_relations": (
            "account_relations",
            "streaming.ingestion.account_relation_streaming_pipeline",
        ),
    }

    for task_id, (component, module) in expected_modules.items():
        dag.get_task(task_id).python_callable(config)
        name, command = commands[-1]
        assert name == component
        assert command[command.index("-m") + 1] == module
        assert command[command.index("--max-offsets-per-trigger") + 1] == "500"
        assert command[command.index("--checkpoint-dir") + 1].endswith(
            f"/{component}"
        )
        assert ("--static-metadata-dir" in command) == (
            component in {"bank_features", "entity_features"}
        )

    assert len(commands) == len(expected_modules)


def test_larger_replay_config():
    """Manual runs without logical_date can request 5,000 rows in bounded batches."""
    dag = load_dag_bag().dags["muledetector_replay"]
    config = dag.get_task("build_run_config").python_callable(
        dag_run=SimpleNamespace(
            run_after=datetime(2026, 11, 1, tzinfo=timezone.utc),
            conf={"replay_row_count": 5000, "max_offsets_per_trigger": 500},
        )
    )
    assert config["replay_row_count"] == "5000"
    assert config["max_offsets_per_trigger"] == "500"
    assert config["run_id"] == "airflow_replay_20261101T000000"
