"""Tests for the MuleDetector replay DAG."""

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from airflow.models import DagBag
from airflow.providers.standard.operators.python import BranchPythonOperator


EXPECTED_TASK_IDS = {
    "build_run_config",
    "prepare_replay_window",
    "publish_replay_to_kafka",
    "choose_pipeline",
    "process_shared_updates",
    "choose_verification",
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
    """Output jobs fan out after publish and all must succeed before the audit."""
    dag = load_dag_bag().dags.get("muledetector_replay")

    assert dag is not None
    assert dag.max_active_runs == 1
    assert 1 <= dag.max_active_tasks <= 5
    assert dag.default_args.get("retries") == 0
    assert set(dag.task_ids) == EXPECTED_TASK_IDS
    output_tasks = {
        "process_account_features",
        "process_account_edges",
        "process_bank_features",
        "process_entity_features",
        "process_account_relations",
    }
    assert "publish_replay_to_kafka" in dag.get_task("prepare_replay_window").downstream_task_ids
    assert dag.get_task("publish_replay_to_kafka").downstream_task_ids == {"choose_pipeline"}
    assert dag.get_task("choose_pipeline").downstream_task_ids == output_tasks | {"process_shared_updates"}
    for task_id in output_tasks:
        task = dag.get_task(task_id)
        assert task.upstream_task_ids == {"build_run_config", "choose_pipeline"}
        assert task.downstream_task_ids == {"audit_replay"}
    assert output_tasks <= dag.get_task("audit_replay").upstream_task_ids
    assert "choose_verification" in dag.get_task("process_shared_updates").downstream_task_ids
    assert dag.get_task("choose_verification").trigger_rule == "none_failed_min_one_success"
    assert dag.get_task("write_run_report").trigger_rule == "none_failed_min_one_success"
    ordered_tasks = [
        "audit_replay",
        "package_model_updates",
        "choose_verification",
        "validate_replay",
        "write_run_report",
    ]
    for upstream, downstream in zip(ordered_tasks, ordered_tasks[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids


@pytest.mark.parametrize("limit", ["1", "2", "5"])
def test_configurable_concurrency(monkeypatch, limit):
    """The same independent branches support sequential or bounded parallel runs."""
    monkeypatch.setenv("MULEDETECTOR_MAX_ACTIVE_TASKS", limit)
    dag_bag = load_dag_bag()
    assert not dag_bag.import_errors
    assert dag_bag.dags["muledetector_replay"].max_active_tasks == int(limit)


@pytest.mark.parametrize("limit", ["0", "6", "invalid"])
def test_invalid_concurrency_rejected(monkeypatch, limit):
    """Do not silently enable unbounded Spark concurrency on a laptop."""
    dag = load_dag_bag().dags["muledetector_replay"]
    monkeypatch.setenv("MULEDETECTOR_MAX_ACTIVE_TASKS", limit)
    with pytest.raises(ValueError, match="integer from 1 to 5"):
        dag.get_task("build_run_config").python_callable.__globals__["replay_task_limit"]()


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
    assert config["pipeline_mode"] == "shared"
    assert config["verify_features"] == "true"
    assert config["run_id"] == "airflow_replay_20261101T000000"


def test_shared_pipeline_command_and_branch(monkeypatch):
    dag = load_dag_bag().dags["muledetector_replay"]
    config = dag.get_task("build_run_config").python_callable(
        dag_run=SimpleNamespace(run_after=datetime(2026, 11, 2, tzinfo=timezone.utc),
                               conf={"pipeline_mode": "shared", "concurrent_feature_jobs": 1})
    )
    assert dag.get_task("choose_pipeline").python_callable(config) == ["process_shared_updates"]
    commands = []
    shared_task = dag.get_task("process_shared_updates")
    monkeypatch.setitem(shared_task.python_callable.__globals__, "run_project_command",
                        lambda name, command: commands.append(command))
    shared_task.python_callable(config)
    command = commands[0]
    assert command[command.index("-m") + 1] == "streaming.ingestion.shared_stateful_streaming_pipeline"
    assert command[command.index("--concurrent-feature-jobs") + 1] == "1"
    assert command[command.index("--checkpoint-dir") + 1].endswith("/shared")
    config["pipeline_mode"] = "legacy"
    assert len(dag.get_task("choose_pipeline").python_callable(config)) == 5


def test_feature_verification_can_be_disabled_without_skipping_report():
    dag = load_dag_bag().dags["muledetector_replay"]
    choose = dag.get_task("choose_verification")
    assert isinstance(choose, BranchPythonOperator)
    assert choose.python_callable({"verify_features": "true"}) == "validate_replay"
    assert choose.python_callable({"verify_features": "false"}) == "write_run_report"
    assert choose.downstream_task_ids == {"validate_replay", "write_run_report"}
    assert dag.get_task("write_run_report").upstream_task_ids == {
        "build_run_config", "choose_verification", "validate_replay",
    }
