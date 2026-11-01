"""Prepare an ordered replay with one source scan and bounded memory."""

from __future__ import annotations

import argparse
import csv
import heapq
import time
from datetime import datetime
from pathlib import Path

TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-path", type=Path, required=True)
    parser.add_argument("--start-timestamp", required=True)
    parser.add_argument("--end-timestamp", required=True)
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--output-path", type=Path, required=True)
    return parser.parse_args()


def prepare_replay_window(input_path, output_path, start_timestamp, end_timestamp, max_rows):
    """Keep the earliest max_rows using the project's existing seven sort keys.

    The input need not be sorted. nsmallest retains O(max_rows) candidates.
    Account values remain strings, including leading zeros.
    """
    if max_rows <= 0:
        raise ValueError("--max-rows must be greater than zero.")
    start = datetime.strptime(start_timestamp, TIMESTAMP_FORMAT)
    end = datetime.strptime(end_timestamp, TIMESTAMP_FORMAT)
    if start >= end:
        raise ValueError("Window start must be earlier than window end.")
    if output_path.exists():
        raise FileExistsError(f"Replay file already exists: {output_path}")
    attributes = getattr(input_path.stat(), "st_file_attributes", 0)
    if attributes & 0x1000:  # Windows FILE_ATTRIBUTE_OFFLINE
        raise OSError(
            f"Source CSV is online-only: {input_path}. In OneDrive choose "
            "'Always keep on this device' and wait for the download, or use a local copy."
        )

    with input_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        header = next(reader, None)
        if not header:
            raise ValueError(f"Empty transaction CSV: {input_path}")
        if header.count("Account") == 2:
            header[header.index("Account", header.index("Account") + 1)] = "Account.1"
        required = [
            "Timestamp", "From Bank", "Account", "To Bank", "Account.1",
            "Amount Paid", "Payment Format",
        ]
        missing = set(required).difference(header)
        if missing:
            raise ValueError(f"Missing source columns: {sorted(missing)}")
        positions = {name: header.index(name) for name in required}

        def eligible_rows():
            for row_number, row in enumerate(reader, start=2):
                if not row:
                    continue
                if len(row) != len(header):
                    raise ValueError(f"Malformed CSV row {row_number}: wrong field count")
                timestamp = row[positions["Timestamp"]]
                # Source timestamps have a fixed-width, chronologically sortable format.
                if start_timestamp <= timestamp < end_timestamp:
                    datetime.strptime(timestamp, TIMESTAMP_FORMAT)
                    yield row

        def sort_key(row):
            return (
                row[positions["Timestamp"]],
                int(row[positions["From Bank"]]),
                row[positions["Account"]],
                int(row[positions["To Bank"]]),
                row[positions["Account.1"]],
                float(row[positions["Amount Paid"]]),
                row[positions["Payment Format"]],
            )

        selected = heapq.nsmallest(max_rows, eligible_rows(), key=sort_key)

    if len(selected) != max_rows:
        raise ValueError(
            f"Only {len(selected):,} transactions were found in the requested "
            "window. Choose a wider time window."
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("x", encoding="utf-8", newline="") as destination:
        writer = csv.writer(destination)
        writer.writerow(header)
        writer.writerows(selected)
    return len(selected)


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    count = prepare_replay_window(
        args.input_path, args.output_path, args.start_timestamp,
        args.end_timestamp, args.max_rows,
    )
    print(f"Prepared {count:,} ordered transactions in {time.perf_counter() - started:.2f}s.")
    print(f"Replay window: [{args.start_timestamp}, {args.end_timestamp})")
    print(f"Saved replay file: {args.output_path}")


if __name__ == "__main__":
    main()
