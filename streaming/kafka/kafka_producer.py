from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "aml_transactions"
TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"
CSV_CHUNK_SIZE = 100_000


def json_serializer(value: dict) -> bytes:
    """Convert one transaction dictionary into Kafka message bytes."""
    return json.dumps(value, default=str).encode("utf-8")


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a connection from this Python program to Kafka."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
    )


def load_replay_transactions(
    input_path: Path,
    max_rows: int,
    start_timestamp: str | None,
) -> pd.DataFrame:
    """
    Read transactions for replay.

    Without a cutoff, read the first max_rows as before.
    With a cutoff, scan CSV chunks until max_rows transactions at or after
    the cutoff have been collected.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Transaction CSV not found: {input_path}")

    if max_rows <= 0:
        raise ValueError("--max-rows must be greater than zero.")

    if start_timestamp is None:
        transactions = pd.read_csv(input_path, nrows=max_rows)
    else:
        try:
            cutoff_time = pd.to_datetime(
                start_timestamp,
                format=TIMESTAMP_FORMAT,
                errors="raise",
            )
        except ValueError as error:
            raise ValueError(
                "--start-timestamp must use YYYY/MM/DD HH:MM format."
            ) from error

        selected_chunks: list[pd.DataFrame] = []
        remaining_rows = max_rows

        for chunk in pd.read_csv(input_path, chunksize=CSV_CHUNK_SIZE):
            if "Timestamp" not in chunk.columns:
                raise ValueError(
                    "Expected a 'Timestamp' column in the transaction CSV."
                )

            parsed_times = pd.to_datetime(
                chunk["Timestamp"],
                format=TIMESTAMP_FORMAT,
                errors="raise",
            )

            eligible_rows = chunk.loc[
                parsed_times >= cutoff_time
            ]

            if not eligible_rows.empty:
                selected_chunks.append(
                    eligible_rows.head(remaining_rows)
                )
                remaining_rows -= len(selected_chunks[-1])

            if remaining_rows == 0:
                break

        if not selected_chunks:
            raise ValueError(
                "No transactions were found at or after "
                f"{start_timestamp}."
            )

        transactions = pd.concat(
            selected_chunks,
            ignore_index=True,
        )

    if "Timestamp" not in transactions.columns:
        raise ValueError("Expected a 'Timestamp' column in the transaction CSV.")

    if transactions.empty:
        raise ValueError("No transactions were selected for replay.")

    return transactions.sort_values(
        "Timestamp",
        kind="stable",
    )


def replay_transactions(
    input_path: Path,
    producer: KafkaProducer,
    topic: str,
    max_rows: int,
    delay_seconds: float,
    start_timestamp: str | None,
) -> None:
    """Publish one timestamp-ordered transaction per Kafka message."""
    transactions = load_replay_transactions(
        input_path=input_path,
        max_rows=max_rows,
        start_timestamp=start_timestamp,
    )

    if start_timestamp is not None:
        print(
            "Replaying transactions at or after "
            f"{start_timestamp}..."
        )

    for sequence_number, (_, row) in enumerate(
        transactions.iterrows(),
        start=1,
    ):
        transaction = row.where(pd.notna(row), None).to_dict()

        # A stable key keeps events from the same sender account together
        # when the topic uses multiple partitions later.
        sender_key = (
            f"{transaction.get('From Bank')}::{transaction.get('Account')}"
        ).encode("utf-8")

        producer.send(
            topic,
            key=sender_key,
            value=json_serializer(transaction),
        )

        print(
            f"Sent {sequence_number}/{len(transactions)} | "
            f"timestamp={transaction['Timestamp']}"
        )

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    producer.flush()

    print(
        f"\nReplay complete: {len(transactions)} transactions sent "
        f"to '{topic}'."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay IBM AML transaction CSV rows into Kafka."
    )
    parser.add_argument("--input-path", type=Path, required=True)
    parser.add_argument("--max-rows", type=int, default=20)
    parser.add_argument("--delay-seconds", type=float, default=0.25)
    parser.add_argument(
        "--start-timestamp",
        help=(
            "Inclusive replay cutoff in YYYY/MM/DD HH:MM format. "
            "Only transactions at or after this time are sent."
        ),
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    producer = build_producer(args.bootstrap_servers)

    try:
        replay_transactions(
            input_path=args.input_path,
            producer=producer,
            topic=args.topic,
            max_rows=args.max_rows,
            delay_seconds=args.delay_seconds,
            start_timestamp=args.start_timestamp,
        )
    finally:
        producer.close()


if __name__ == "__main__":
    main()