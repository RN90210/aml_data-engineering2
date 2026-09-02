from __future__ import annotations

import argparse
import json
from pathlib import Path


EXPECTED_MODEL_TABLES = (
    "account_features",
    "bank_features",
    "entity_features",
    "account_to_account_edges",
    "account_to_bank",
    "entity_to_account",
)


def parse_args() -> argparse.Namespace:
    """Parse completed-run input locations and report policy."""
    parser = argparse.ArgumentParser(
        description=(
            "Write one validated report for a completed stateful replay."
        )
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--metrics-output-dir",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--model-updates-output-dir",
        type=Path,
        required=True,
    )
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--require-no-rejections", action="store_true")
    return parser.parse_args()


def read_json(path: Path, description: str) -> dict:
    """Read one JSON object with a clear path-specific error."""
    if not path.is_file():
        raise FileNotFoundError(f"{description} not found: {path}")

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Unable to read {description}: {path}") from error

    if not isinstance(value, dict):
        raise ValueError(f"{description} must contain a JSON object: {path}")
    return value


def require_integer(record: dict, field_name: str, context: str) -> int:
    """Return one required non-negative integer field."""
    value = record.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(
            f"{context} requires non-negative integer {field_name}."
        )
    return value


def require_number(record: dict, field_name: str, context: str) -> float:
    """Return one required non-negative numeric field."""
    value = record.get(field_name)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value < 0
    ):
        raise ValueError(
            f"{context} requires non-negative numeric {field_name}."
        )
    return float(value)


def load_audit_records(
    metrics_output_dir: Path,
    run_id: str,
) -> list[dict]:
    """Load, identify, and order every audit batch for one run."""
    run_directory = metrics_output_dir / f"run_id={run_id}"
    if not run_directory.is_dir():
        raise FileNotFoundError(
            f"Audit metrics run directory not found: {run_directory}"
        )

    audit_paths = sorted(run_directory.glob("batch_id=*.json"))
    if not audit_paths:
        raise FileNotFoundError(
            f"No audit batch records found in: {run_directory}"
        )

    records = []
    seen_batch_ids = set()
    for audit_path in audit_paths:
        audit = read_json(audit_path, "Audit batch record")
        context = f"Audit record {audit_path}"

        if audit.get("run_id") != run_id:
            raise ValueError(
                f"{context} has run_id {audit.get('run_id')!r}; "
                f"expected {run_id!r}."
            )

        batch_id = require_integer(audit, "batch_id", context)
        filename_batch_text = audit_path.stem.removeprefix("batch_id=")
        try:
            filename_batch_id = int(filename_batch_text)
        except ValueError as error:
            raise ValueError(
                f"Invalid audit batch filename: {audit_path.name}"
            ) from error

        if filename_batch_id != batch_id:
            raise ValueError(
                f"{context} batch_id does not match its filename."
            )
        if batch_id in seen_batch_ids:
            raise ValueError(f"Duplicate audit batch_id: {batch_id}")

        seen_batch_ids.add(batch_id)
        records.append(audit)

    return sorted(records, key=lambda record: record["batch_id"])


def validate_manifest(
    manifest: dict,
    manifest_path: Path,
    run_id: str,
    batch_id: int,
) -> dict[str, int]:
    """Validate one ready model package and return its six counts."""
    context = f"Model manifest {manifest_path}"
    if manifest.get("run_id") != run_id:
        raise ValueError(
            f"{context} has run_id {manifest.get('run_id')!r}; "
            f"expected {run_id!r}."
        )
    if manifest.get("batch_id") != batch_id:
        raise ValueError(
            f"{context} batch_id does not match audit batch {batch_id}."
        )
    if manifest.get("status") != "ready":
        raise ValueError(f"{context} status must be ready.")

    counts = manifest.get("counts")
    columns = manifest.get("columns")
    if not isinstance(counts, dict):
        raise ValueError(f"{context} requires a counts object.")
    if not isinstance(columns, dict):
        raise ValueError(f"{context} requires a columns object.")

    missing_count_tables = [
        table_name
        for table_name in EXPECTED_MODEL_TABLES
        if table_name not in counts
    ]
    missing_column_tables = [
        table_name
        for table_name in EXPECTED_MODEL_TABLES
        if table_name not in columns
    ]
    if missing_count_tables:
        raise ValueError(
            f"{context} is missing counts for: "
            f"{', '.join(missing_count_tables)}."
        )
    if missing_column_tables:
        raise ValueError(
            f"{context} is missing columns for: "
            f"{', '.join(missing_column_tables)}."
        )

    package_counts = {}
    for table_name in EXPECTED_MODEL_TABLES:
        table_count = counts[table_name]
        if (
            isinstance(table_count, bool)
            or not isinstance(table_count, int)
            or table_count < 0
        ):
            raise ValueError(
                f"{context} has invalid row count for {table_name}."
            )
        if not isinstance(columns[table_name], list):
            raise ValueError(
                f"{context} has invalid columns for {table_name}."
            )
        package_counts[table_name] = table_count

    return package_counts


def build_run_report(
    run_id: str,
    audit_records: list[dict],
    model_updates_output_dir: Path,
    require_no_rejections: bool,
) -> dict:
    """Validate all batches and combine their audit and model totals."""
    totals = {
        "kafka_transactions": 0,
        "expected_account_events": 0,
        "valid_account_events": 0,
        "rejected_account_events": 0,
        "batches": 0,
        "processing_seconds": 0.0,
        "ready_model_packages": 0,
        "skipped_no_valid_event_batches": 0,
    }
    combined_model_counts = {
        table_name: 0 for table_name in EXPECTED_MODEL_TABLES
    }
    audit_summaries = []
    model_packages = []
    validation_failed = False

    for audit in audit_records:
        batch_id = audit["batch_id"]
        context = f"Audit batch {batch_id}"
        kafka_count = require_integer(
            audit,
            "kafka_transaction_count",
            context,
        )
        expected_count = require_integer(
            audit,
            "expected_account_event_count",
            context,
        )
        valid_count = require_integer(
            audit,
            "valid_account_event_count",
            context,
        )
        rejected_count = require_integer(
            audit,
            "rejected_account_event_count",
            context,
        )
        processing_seconds = require_number(
            audit,
            "processing_seconds",
            context,
        )
        partition_offsets = audit.get("partition_offsets")
        if not isinstance(partition_offsets, list):
            raise ValueError(f"{context} requires partition_offsets.")

        count_validation_passed = (
            valid_count + rejected_count == expected_count
        )
        audit_validation_passed = (
            audit.get("validation_status") == "passed"
        )
        if not count_validation_passed or not audit_validation_passed:
            validation_failed = True

        if valid_count == 0 and rejected_count > 0:
            package_status = "skipped_no_valid_events"
            package_counts = {
                table_name: 0 for table_name in EXPECTED_MODEL_TABLES
            }
            totals["skipped_no_valid_event_batches"] += 1
        else:
            manifest_path = (
                model_updates_output_dir
                / f"run_id={run_id}"
                / f"batch_id={batch_id}"
                / "manifest.json"
            )
            manifest = read_json(manifest_path, "Ready model manifest")
            package_counts = validate_manifest(
                manifest,
                manifest_path,
                run_id,
                batch_id,
            )
            package_status = manifest["status"]
            totals["ready_model_packages"] += 1

        totals["kafka_transactions"] += kafka_count
        totals["expected_account_events"] += expected_count
        totals["valid_account_events"] += valid_count
        totals["rejected_account_events"] += rejected_count
        totals["batches"] += 1
        totals["processing_seconds"] += processing_seconds

        for table_name, row_count in package_counts.items():
            combined_model_counts[table_name] += row_count

        audit_summaries.append(
            {
                "batch_id": batch_id,
                "kafka_transaction_count": kafka_count,
                "expected_account_event_count": expected_count,
                "valid_account_event_count": valid_count,
                "rejected_account_event_count": rejected_count,
                "processing_seconds": processing_seconds,
                "validation_status": audit.get("validation_status"),
                "partition_offsets": partition_offsets,
            }
        )
        model_packages.append(
            {
                "batch_id": batch_id,
                "status": package_status,
                "counts": package_counts,
            }
        )

    totals["processing_seconds"] = round(
        totals["processing_seconds"],
        3,
    )

    has_rejections = totals["rejected_account_events"] > 0
    if validation_failed or (require_no_rejections and has_rejections):
        overall_status = "failed"
    elif has_rejections:
        overall_status = "passed_with_rejections"
    else:
        overall_status = "passed"

    return {
        "kind": "stateful_streaming_run_report",
        "version": 1,
        "run_id": run_id,
        "overall_status": overall_status,
        "totals": totals,
        "combined_model_table_row_counts": combined_model_counts,
        "audit_batches": audit_summaries,
        "model_packages": model_packages,
    }


def write_report_atomically(report: dict, report_path: Path) -> None:
    """Write a complete JSON report before replacing its final path."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = report_path.with_name(report_path.name + ".tmp")
    temporary_path.write_text(
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(report_path)


def main() -> None:
    """Validate a completed replay and write its consolidated report."""
    args = parse_args()
    audit_records = load_audit_records(
        args.metrics_output_dir,
        args.run_id,
    )
    report = build_run_report(
        args.run_id,
        audit_records,
        args.model_updates_output_dir,
        args.require_no_rejections,
    )
    write_report_atomically(report, args.report_path)

    print(f"Overall status: {report['overall_status']}")
    print(f"Report: {args.report_path}")


if __name__ == "__main__":
    main()
