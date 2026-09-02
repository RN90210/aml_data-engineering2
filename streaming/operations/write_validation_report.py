from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.dataset as ds
from kafka import KafkaConsumer, TopicPartition


GRAPH_TABLES = [
    "node_account_features.parquet",
    "node_bank_features.parquet",
    "node_entity_features.parquet",
    "edge_account_to_account.parquet",
    "edge_account_to_bank.parquet",
    "edge_entity_to_account.parquet",
]


def count_csv_rows(csv_path: Path) -> int:
    """Count data rows without loading the complete CSV into memory."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Replay CSV not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file)

        try:
            next(reader)
        except StopIteration:
            return 0

        return sum(1 for _ in reader)


def count_parquet_rows(
    parquet_path: Path,
    missing_is_zero: bool = False,
) -> int:
    """Count rows in a Parquet directory without converting it to Pandas."""
    if not parquet_path.exists():
        if missing_is_zero:
            return 0

        raise FileNotFoundError(
            f"Parquet output not found: {parquet_path}"
        )

    return ds.dataset(
        str(parquet_path),
        format="parquet",
    ).count_rows()


def get_topic_offsets(
    bootstrap_servers: str,
    topic: str,
) -> tuple[dict[str, dict[str, int]], int]:
    """Read the beginning and end offset of every Kafka partition."""
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        request_timeout_ms=10_000,
    )

    try:
        partitions = consumer.partitions_for_topic(topic)

        if not partitions:
            raise ValueError(
                f"Kafka topic does not exist or has no partitions: {topic}"
            )

        topic_partitions = [
            TopicPartition(topic, partition)
            for partition in sorted(partitions)
        ]

        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        offsets = {}
        total_messages = 0

        for topic_partition in topic_partitions:
            start_offset = beginning_offsets[topic_partition]
            end_offset = end_offsets[topic_partition]
            available_messages = end_offset - start_offset

            offsets[str(topic_partition.partition)] = {
                "start_offset": start_offset,
                "end_offset": end_offset,
                "available_messages": available_messages,
            }

            total_messages += available_messages

        return offsets, total_messages
    finally:
        consumer.close()


def parse_started_at(value: str | None) -> datetime | None:
    """Parse an optional ISO timestamp supplied by a pipeline runner."""
    if value is None:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Write a validation report for one MuleDetector "
            "Kafka-to-graph pipeline run."
        )
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Unique human-readable name for this pipeline run.",
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        required=True,
        help="Chronologically ordered replay CSV sent to Kafka.",
    )
    parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic used for this run.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
    )
    parser.add_argument(
        "--account-events-dir",
        type=Path,
        required=True,
        help="Parquet folder containing valid Account events.",
    )
    parser.add_argument(
        "--rejected-events-dir",
        type=Path,
        required=True,
        help="Parquet folder containing rejected Account events.",
    )
    parser.add_argument(
        "--graph-snapshot-dir",
        type=Path,
        required=True,
        help="Final graph_snapshot package directory.",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        required=True,
        help="JSON validation report to create.",
    )
    parser.add_argument(
        "--pipeline-started-at",
        help=(
            "Optional ISO-8601 UTC start time. Airflow will provide "
            "this automatically later."
        ),
    )
    parser.add_argument(
        "--require-topic-count-match",
        action="store_true",
        help=(
            "Require Kafka's current message count to exactly match "
            "the replay CSV transaction count."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    reported_at = datetime.now(timezone.utc)
    started_at = parse_started_at(args.pipeline_started_at)

    input_transaction_count = count_csv_rows(args.input_csv)

    kafka_offsets, kafka_message_count = get_topic_offsets(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )

    valid_event_count = count_parquet_rows(
        args.account_events_dir
    )

    rejected_event_count = count_parquet_rows(
        args.rejected_events_dir,
        missing_is_zero=True,
    )

    expected_account_event_count = input_transaction_count * 2

    graph_counts = {}
    missing_graph_tables = []

    for table_name in GRAPH_TABLES:
        table_path = args.graph_snapshot_dir / table_name

        if not table_path.exists():
            missing_graph_tables.append(table_name)
            graph_counts[table_name] = 0
        else:
            graph_counts[table_name] = count_parquet_rows(table_path)

    checks = {
        "input_contains_transactions": input_transaction_count > 0,
        "kafka_contains_input_transactions": (
            kafka_message_count >= input_transaction_count
        ),
        "kafka_count_matches_input": (
            kafka_message_count == input_transaction_count
        ),
        "all_transaction_events_accounted_for": (
            valid_event_count + rejected_event_count
            == expected_account_event_count
        ),
        "no_events_rejected": rejected_event_count == 0,
        "all_graph_tables_exist": not missing_graph_tables,
        "all_graph_tables_contain_rows": all(
            count > 0 for count in graph_counts.values()
        ),
    }

    core_checks = [
        checks["input_contains_transactions"],
        checks["kafka_contains_input_transactions"],
        checks["all_transaction_events_accounted_for"],
        checks["all_graph_tables_exist"],
        checks["all_graph_tables_contain_rows"],
    ]

    if args.require_topic_count_match:
        core_checks.append(checks["kafka_count_matches_input"])

    if not all(core_checks):
        status = "failed"
    elif rejected_event_count > 0:
        status = "passed_with_rejections"
    else:
        status = "passed"

    duration_seconds = None

    if started_at is not None:
        duration_seconds = round(
            (reported_at - started_at).total_seconds(),
            3,
        )

    report = {
        "run_id": args.run_id,
        "status": status,
        "reported_at_utc": reported_at.isoformat(),
        "pipeline_started_at_utc": (
            started_at.isoformat()
            if started_at is not None
            else None
        ),
        "pipeline_duration_seconds": duration_seconds,
        "input": {
            "replay_csv": str(args.input_csv),
            "transaction_count": input_transaction_count,
        },
        "kafka": {
            "bootstrap_servers": args.bootstrap_servers,
            "topic": args.topic,
            "partition_offsets": kafka_offsets,
            "available_message_count": kafka_message_count,
        },
        "account_events": {
            "expected_count": expected_account_event_count,
            "valid_count": valid_event_count,
            "rejected_count": rejected_event_count,
        },
        "graph_snapshot": {
            "output_path": str(args.graph_snapshot_dir),
            "table_row_counts": graph_counts,
            "missing_tables": missing_graph_tables,
        },
        "checks": checks,
    }

    args.report_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    args.report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print(f"Validation status: {status}")
    print(f"Validation report written to: {args.report_path}")
    print(
        "Input transactions: "
        f"{input_transaction_count:,} | "
        f"Kafka messages: {kafka_message_count:,} | "
        f"Valid events: {valid_event_count:,} | "
        f"Rejected events: {rejected_event_count:,}"
    )

    if status == "failed":
        raise SystemExit(
            "Validation failed. Read the JSON report for details."
        )


if __name__ == "__main__":
    main()