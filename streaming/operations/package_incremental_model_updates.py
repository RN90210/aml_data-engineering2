from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq

from preprocessing.spark_static_pipeline import (
    ACCOUNT_FEATURE_COLUMNS,
    ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS,
    BANK_FEATURE_COLUMNS,
    ENTITY_FEATURE_COLUMNS,
)

EXPECTED_TABLE_COLUMNS = {
    "account_features": ["account_id", *ACCOUNT_FEATURE_COLUMNS],
    "bank_features": ["bank_id", *BANK_FEATURE_COLUMNS],
    "entity_features": ["entity_id", *ENTITY_FEATURE_COLUMNS],
    "account_to_account_edges": [
        "sender_account_id",
        "receiver_account_id",
        *ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS,
    ],
    "account_to_bank": ["account_id", "bank_id"],
    "entity_to_account": ["entity_id", "account_id"],
}


def parse_args() -> argparse.Namespace:
    """Parse the output location and requested run or batch."""
    parser = argparse.ArgumentParser(
        description=(
            "Validate completed incremental model-update tables and "
            "write ready manifests."
        )
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-id", type=int)
    return parser.parse_args()


def discover_batch_directories(
    output_root: Path,
    run_id: str,
    batch_id: int | None,
) -> list[tuple[int, Path]]:
    """Return requested batch directories in numeric batch order."""
    run_directory = output_root / f"run_id={run_id}"
    if not run_directory.is_dir():
        raise FileNotFoundError(
            f"Incremental run directory not found: {run_directory}"
        )

    if batch_id is not None:
        batch_directory = run_directory / f"batch_id={batch_id}"
        if not batch_directory.is_dir():
            raise FileNotFoundError(
                f"Incremental batch directory not found: {batch_directory}"
            )
        return [(batch_id, batch_directory)]

    discovered_batches = []
    for batch_directory in run_directory.glob("batch_id=*"):
        if not batch_directory.is_dir():
            continue

        batch_text = batch_directory.name.removeprefix("batch_id=")
        try:
            discovered_batch_id = int(batch_text)
        except ValueError as error:
            raise ValueError(
                f"Invalid batch directory name: {batch_directory.name}"
            ) from error

        discovered_batches.append(
            (discovered_batch_id, batch_directory)
        )

    if not discovered_batches:
        raise FileNotFoundError(
            f"No batch directories found in: {run_directory}"
        )

    return sorted(discovered_batches, key=lambda item: item[0])


def validate_table_and_count_rows(
    table_directory: Path,
    expected_columns: list[str],
) -> int:
    """Validate every Parquet footer and sum its metadata row count."""
    if not table_directory.is_dir():
        raise FileNotFoundError(
            f"Required table directory not found: {table_directory}"
        )

    parquet_files = sorted(table_directory.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(
            f"Required table contains no Parquet files: {table_directory}"
        )

    row_count = 0
    for parquet_path in parquet_files:
        try:
            parquet_file = pq.ParquetFile(parquet_path)
            actual_columns = parquet_file.schema_arrow.names
            row_count += parquet_file.metadata.num_rows
        except Exception as error:
            raise ValueError(
                f"Unable to read Parquet metadata: {parquet_path}"
            ) from error

        if actual_columns != expected_columns:
            raise ValueError(
                f"Schema mismatch for {parquet_path}. "
                f"Expected columns {expected_columns}, "
                f"found {actual_columns}."
            )

    return row_count


def package_batch(
    batch_directory: Path,
    run_id: str,
    batch_id: int,
) -> None:
    """Validate one complete batch and write its ready manifest."""
    counts = {}
    columns = {}

    for table_name, expected_columns in EXPECTED_TABLE_COLUMNS.items():
        table_directory = batch_directory / f"{table_name}.parquet"
        counts[table_name] = validate_table_and_count_rows(
            table_directory,
            expected_columns,
        )
        columns[table_name] = expected_columns

    manifest = {
        "kind": "incremental_model_update",
        "version": 1,
        "run_id": run_id,
        "batch_id": batch_id,
        "status": "ready",
        "description": (
            "These are only the rows affected by this Kafka micro-batch; "
            "feature values are cumulative through this batch."
        ),
        "counts": counts,
        "columns": columns,
    }

    manifest_path = batch_directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )

    print(
        f"Packaged run_id={run_id}, batch_id={batch_id}: "
        "6 tables ready."
    )


def main() -> None:
    """Package one requested batch or every discovered run batch."""
    args = parse_args()
    batches = discover_batch_directories(
        args.output_root,
        args.run_id,
        args.batch_id,
    )

    for batch_id, batch_directory in batches:
        package_batch(batch_directory, args.run_id, batch_id)


if __name__ == "__main__":
    main()
