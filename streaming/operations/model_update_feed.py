"""List ready model packages in order without waiting for an end-of-run report.

This module uses only the standard library. The model owns its durable cursor:
advance it only after a package has been applied successfully. Repeating a call
with the old cursor returns the same package again, allowing a failed application
to retry. Rejected-only batches are explicit skip records, not missing data.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from streaming.operations.write_stateful_run_report import (
    EXPECTED_MODEL_TABLES, read_json, require_integer, validate_manifest,
)


def ready_batches(output_root: Path, metrics_root: Path, run_id: str, after_batch_id: int = -1):
    """Yield consecutive audited packages, stopping at the first unfinished batch.

    Model callers should upsert by the documented keys and record (run_id,
    batch_id) in the same durable transaction as their model-side graph update.
    A ready package is immutable under normal checkpoint retry; consumers never
    read a table directory without its manifest and successful audit.
    """
    batch_id = after_batch_id + 1
    while True:
        audit_path = metrics_root / f"run_id={run_id}" / f"batch_id={batch_id}.json"
        if not audit_path.is_file():
            return
        audit = read_json(audit_path, "Audit batch record")
        if audit.get("run_id") != run_id or audit.get("batch_id") != batch_id:
            raise ValueError(f"Audit identity mismatch: {audit_path}")
        if audit.get("validation_status") != "passed":
            raise ValueError(f"Audit did not pass: {audit_path}")
        context = str(audit_path)
        valid = require_integer(audit, "valid_account_event_count", context)
        rejected = require_integer(audit, "rejected_account_event_count", context)
        expected = require_integer(audit, "expected_account_event_count", context)
        transactions = require_integer(audit, "kafka_transaction_count", context)
        if valid + rejected != expected or expected != transactions * 2:
            raise ValueError(f"Audit event counts disagree: {audit_path}")
        if valid == 0 and rejected > 0:
            yield {"run_id": run_id, "batch_id": batch_id, "status": "skipped_no_valid_events",
                   "counts": {name: 0 for name in EXPECTED_MODEL_TABLES}, "tables": {}, "audit": audit}
        else:
            directory = output_root / f"run_id={run_id}" / f"batch_id={batch_id}"
            manifest_path = directory / "manifest.json"
            if not manifest_path.is_file():
                return
            manifest = read_json(manifest_path, "Model manifest")
            counts = validate_manifest(manifest, manifest_path, run_id, batch_id)
            tables = {name: str(directory / f"{name}.parquet") for name in EXPECTED_MODEL_TABLES}
            for name, location in tables.items():
                if not (Path(location) / "_SUCCESS").is_file():
                    raise ValueError(f"Ready table is missing its completed write marker: {location}")
            yield {"run_id": run_id, "batch_id": batch_id, "status": "ready",
                   "counts": counts, "columns": manifest["columns"], "tables": tables, "audit": audit}
        batch_id += 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--metrics-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--after-batch-id", type=int, default=-1)
    parser.add_argument("--follow", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    args = parser.parse_args()
    if args.after_batch_id < -1 or args.poll_seconds <= 0:
        parser.error("--after-batch-id must be >= -1 and --poll-seconds must be positive")
    cursor = args.after_batch_id
    while True:
        for package in ready_batches(args.output_root, args.metrics_root, args.run_id, cursor):
            print(json.dumps(package), flush=True)
            cursor = package["batch_id"]
        if not args.follow:
            return
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
