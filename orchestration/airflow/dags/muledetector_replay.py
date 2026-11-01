"""
MuleDetector's first Airflow-orchestrated Kafka replay.

This DAG deliberately runs one task at a time:

prepare replay CSV
→ publish to Kafka
→ create native stateful Account, edge, Bank, and Entity updates
→ create Account relations
→ independently audit the Kafka replay
→ package the model-update manifests
→ validate the replay output
→ write a final run report

Each Airflow run uses a unique Kafka topic, replay file, and Spark
checkpoint directories. This prevents one manual replay from consuming
messages or checkpoints from another.
"""

from __future__ import annotations

import os
import csv
import shlex
import shutil
import subprocess
from pathlib import Path

from airflow.sdk import dag, task
from pendulum import datetime


# These are Linux paths inside the scheduler container, not Windows paths.
PROJECT_ROOT = Path("/opt/muledetector")
RAW_TRANSACTIONS_PATH = Path("/data/raw/HI-Medium_Trans.csv")

# Existing outputs created by your validated static pipeline.
ACCOUNT_ENTITY_LOOKUP_PATH = (
    PROJECT_ROOT / "data/processed/account_entity_lookup.parquet"
)
STATIC_OUTPUT_DIR = PROJECT_ROOT / "data/processed/spark_static_full"

# Default replay size; manual run configuration can request a larger window.
REPLAY_START_TIMESTAMP = "2022/09/01 00:30"
REPLAY_END_TIMESTAMP = "2022/09/02 00:30"
REPLAY_ROW_COUNT = 1000
MAX_OFFSETS_PER_TRIGGER = 500


def run_project_command(task_name: str, command: list[str]) -> None:
    """Run one project command and send its output to the Airflow task log."""
    if not PROJECT_ROOT.is_dir():
        raise FileNotFoundError(
            f"Mounted project directory not found: {PROJECT_ROOT}"
        )

    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"

    print(f"\nStarting task: {task_name}")
    print(f"Command: {shlex.join(command)}\n")

    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        check=True,
    )


@dag(
    dag_id="muledetector_replay",
    description="Replay bounded AML transactions through native Kafka/Spark pipelines.",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Ruhan",
        "retries": 0,
    },
    tags=["muledetector", "kafka", "spark", "replay"],
)
def muledetector_replay():
    @task
    def build_run_config(**context) -> dict[str, str]:
        """
        Create small run-specific settings for downstream tasks.

        This dictionary is safe for Airflow XCom because it contains only
        names and paths, never Parquet data or Spark DataFrames.
        """
        run_timestamp = context["dag_run"].run_after.strftime(
            "%Y%m%dT%H%M%S"
        )
        run_id = f"airflow_replay_{run_timestamp}"
        settings = context["dag_run"].conf or {}
        replay_row_count = int(settings.get("replay_row_count", REPLAY_ROW_COUNT))
        max_offsets = int(settings.get("max_offsets_per_trigger", MAX_OFFSETS_PER_TRIGGER))
        if replay_row_count <= 0 or max_offsets <= 0:
            raise ValueError("Replay row count and max offsets must be positive.")

        streaming_root = PROJECT_ROOT / "data/streaming"
        checkpoint_root = PROJECT_ROOT / "data/checkpoints/airflow"

        return {
            "run_id": run_id,
            "replay_row_count": str(replay_row_count),
            "max_offsets_per_trigger": str(max_offsets),
            "topic": f"aml-transactions-{run_timestamp}",
            "replay_path": str(
                streaming_root / "replay_windows" / f"{run_id}.csv"
            ),
            "prepared_replay_path": (
                context["dag_run"].conf or {}
            ).get("prepared_replay_path", ""),
            "model_updates_dir": str(
                streaming_root / "model_updates"
            ),
            "checkpoint_root": str(checkpoint_root / run_id),
            "audit_rejected_dir": str(
                streaming_root / "audit/rejected_account_events"
            ),
            "audit_metrics_dir": str(
                streaming_root / "audit/metrics"
            ),
            "report_path": str(
                streaming_root / "reports" / f"{run_id}.json"
            ),
        }

    @task
    def prepare_replay_window(config: dict[str, str]) -> None:
        """Create one ordered, bounded replay CSV from the mounted raw file."""
        if config["prepared_replay_path"]:
            source = Path(config["prepared_replay_path"])
            destination = Path(config["replay_path"])
            if not source.is_file():
                raise FileNotFoundError(
                    f"Prepared replay CSV not found: {source}"
                )
            with source.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = csv.reader(handle)
                next(rows, None)
                row_count = sum(1 for row in rows if row)
            if row_count != int(config["replay_row_count"]):
                raise ValueError(
                    f"Prepared replay has {row_count} rows; expected "
                    f"{config['replay_row_count']}."
                )
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            return
        run_project_command(
            "prepare_replay_window",
            [
                "python",
                "-m",
                "streaming.operations.prepare_replay_window",
                "--input-path",
                str(RAW_TRANSACTIONS_PATH),
                "--start-timestamp",
                REPLAY_START_TIMESTAMP,
                "--end-timestamp",
                REPLAY_END_TIMESTAMP,
                "--max-rows",
                config["replay_row_count"],
                "--output-path",
                config["replay_path"],
            ],
        )

    @task
    def publish_replay_to_kafka(config: dict[str, str]) -> None:
        """Publish the prepared replay file to this run's unique Kafka topic."""
        run_project_command(
            "publish_replay_to_kafka",
            [
                "python",
                "-m",
                "streaming.kafka.kafka_producer",
                "--input-path",
                config["replay_path"],
                "--max-rows",
                config["replay_row_count"],
                "--delay-seconds",
                "0",
                "--topic",
                config["topic"],
                "--bootstrap-servers",
                "kafka:9092",
            ],
        )

    def run_native_pipeline(
        config: dict[str, str],
        module: str,
        component: str,
        needs_static_metadata: bool = False,
    ) -> None:
        """Read this run's topic using one component's own checkpoint."""
        command = [
            "python",
            "-m",
            module,
            "--topic",
            config["topic"],
            "--bootstrap-servers",
            "kafka:9092",
            "--max-offsets-per-trigger",
            config["max_offsets_per_trigger"],
            "--run-id",
            config["run_id"],
            "--account-entity-lookup-path",
            str(ACCOUNT_ENTITY_LOOKUP_PATH),
            "--output-dir",
            config["model_updates_dir"],
            "--checkpoint-dir",
            str(Path(config["checkpoint_root"]) / component),
        ]
        if needs_static_metadata:
            command.extend([
                "--static-metadata-dir",
                str(STATIC_OUTPUT_DIR),
            ])
        run_project_command(
            component,
            command,
        )

    @task
    def process_account_features(config: dict[str, str]) -> None:
        run_native_pipeline(
            config,
            "streaming.ingestion.stateful_account_streaming_pipeline",
            "account_features",
        )

    @task
    def process_account_edges(config: dict[str, str]) -> None:
        run_native_pipeline(
            config,
            "streaming.ingestion.stateful_edge_streaming_pipeline",
            "account_edges",
        )

    @task
    def process_bank_features(config: dict[str, str]) -> None:
        run_native_pipeline(
            config,
            "streaming.ingestion.stateful_bank_streaming_pipeline",
            "bank_features",
            needs_static_metadata=True,
        )

    @task
    def process_entity_features(config: dict[str, str]) -> None:
        run_native_pipeline(
            config,
            "streaming.ingestion.stateful_entity_streaming_pipeline",
            "entity_features",
            needs_static_metadata=True,
        )

    @task
    def process_account_relations(config: dict[str, str]) -> None:
        run_native_pipeline(
            config,
            "streaming.ingestion.account_relation_streaming_pipeline",
            "account_relations",
        )

    @task
    def audit_replay(config: dict[str, str]) -> None:
        """Independently audit the same Kafka topic using its own checkpoint."""
        run_project_command(
            "audit_replay",
            [
                "python",
                "-m",
                "streaming.ingestion.streaming_audit_pipeline",
                "--topic",
                config["topic"],
                "--bootstrap-servers",
                "kafka:9092",
                "--max-offsets-per-trigger",
                config["max_offsets_per_trigger"],
                "--run-id",
                config["run_id"],
                "--account-entity-lookup-path",
                str(ACCOUNT_ENTITY_LOOKUP_PATH),
                "--rejected-output-dir",
                config["audit_rejected_dir"],
                "--metrics-output-dir",
                config["audit_metrics_dir"],
                "--checkpoint-dir",
                str(Path(config["checkpoint_root"]) / "audit"),
            ],
        )

    @task
    def package_model_updates(config: dict[str, str]) -> None:
        """Validate the six output tables and mark manifests ready for use."""
        run_project_command(
            "package_model_updates",
            [
                "python",
                "-m",
                "streaming.operations.package_incremental_model_updates",
                "--output-root",
                config["model_updates_dir"],
                "--run-id",
                config["run_id"],
            ],
        )

    @task
    def validate_replay(config: dict[str, str]) -> None:
        """Rebuild expected values from the replay CSV and compare outputs."""
        run_project_command(
            "validate_replay",
            [
                "python",
                "-m",
                "streaming.operations.validate_stateful_replay",
                "--replay-path",
                config["replay_path"],
                "--account-entity-lookup-path",
                str(ACCOUNT_ENTITY_LOOKUP_PATH),
                "--static-output-dir",
                str(STATIC_OUTPUT_DIR),
                "--model-updates-output-dir",
                config["model_updates_dir"],
                "--run-id",
                config["run_id"],
                "--max-offsets-per-trigger",
                config["max_offsets_per_trigger"],
            ],
        )

    @task
    def write_run_report(config: dict[str, str]) -> None:
        """Write one final JSON report only when the replay has no rejections."""
        run_project_command(
            "write_run_report",
            [
                "python",
                "-m",
                "streaming.operations.write_stateful_run_report",
                "--run-id",
                config["run_id"],
                "--metrics-output-dir",
                config["audit_metrics_dir"],
                "--model-updates-output-dir",
                config["model_updates_dir"],
                "--report-path",
                config["report_path"],
                "--require-no-rejections",
            ],
        )

    config = build_run_config()

    prepared = prepare_replay_window(config)
    published = publish_replay_to_kafka(config)
    accounts = process_account_features(config)
    edges = process_account_edges(config)
    banks = process_bank_features(config)
    entities = process_entity_features(config)
    relations = process_account_relations(config)
    audited = audit_replay(config)
    packaged = package_model_updates(config)
    validated = validate_replay(config)
    reported = write_run_report(config)

    prepared >> published >> accounts >> edges >> banks >> entities
    entities >> relations >> audited >> packaged >> validated >> reported


muledetector_replay()
