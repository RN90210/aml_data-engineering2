"""Tests for the MuleDetector replay DAG."""

import logging
from contextlib import contextmanager

from airflow.models import DagBag


EXPECTED_TASK_IDS = {
    "build_run_config",
    "prepare_replay_window",
    "publish_replay_to_kafka",
    "process_account_events",
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