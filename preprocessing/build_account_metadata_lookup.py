from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from preprocessing.paths import ACCOUNTS_FILE
from preprocessing.spark_static_pipeline import (
    ACCOUNT_SCHEMA,
    create_spark_session,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create the full Account-to-Entity metadata lookup required "
            "by the Kafka streaming pipeline."
        )
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.output_dir.exists():
        raise FileExistsError(
            f"Output directory already exists: {args.output_dir}\n"
            "Choose a new output path so account metadata is not overwritten."
        )

    spark = create_spark_session()

    try:
        accounts = (
            spark.read
            .option("header", "true")
            .schema(ACCOUNT_SCHEMA)
            .csv(str(ACCOUNTS_FILE))
        )

        account_lookup = (
            accounts
            .select(
                F.concat_ws(
                    "::",
                    F.col("Bank ID"),
                    F.col("Account Number"),
                ).alias("account_id"),
                F.col("Bank ID").alias("bank_id"),
                F.col("Entity ID").alias("entity_id"),
            )
            .dropDuplicates(["account_id"])
        )

        missing_entity_ids = account_lookup.filter(
            F.col("entity_id").isNull()
        ).count()

        if missing_entity_ids != 0:
            raise ValueError(
                "Account metadata lookup contains Accounts without Entity IDs."
            )

        account_count = account_lookup.count()

        (
            account_lookup.write
            .mode("errorifexists")
            .parquet(str(args.output_dir))
        )

        print("Completed. Full Account metadata lookup written to:")
        print(args.output_dir)
        print(f"Accounts in lookup: {account_count:,}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
