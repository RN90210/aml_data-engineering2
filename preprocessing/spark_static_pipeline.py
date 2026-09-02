import argparse
import os
from datetime import datetime
from pathlib import Path

if os.name == "nt":
    hadoop_home = os.path.join(
        os.path.expanduser("~"),
        "hadoop",
    )
    hadoop_bin = os.path.join(hadoop_home, "bin")

    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = (
        f"{hadoop_bin};{os.environ.get('PATH', '')}"
    )

from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark import StorageLevel
from pyspark.sql import Window
from preprocessing.paths import (
    ACCOUNTS_FILE,
    PROCESSED_DATA_DIR,
    TRANSACTIONS_FILE,
)


TRANSACTION_SCHEMA = T.StructType(
    [
        T.StructField("Timestamp", T.StringType(), nullable=False),
        T.StructField("From Bank", T.LongType(), nullable=False),
        T.StructField("Account", T.StringType(), nullable=False),
        T.StructField("To Bank", T.LongType(), nullable=False),
        T.StructField("Account.1", T.StringType(), nullable=False),
        T.StructField("Amount Received", T.DoubleType(), nullable=False),
        T.StructField("Receiving Currency", T.StringType(), nullable=False),
        T.StructField("Amount Paid", T.DoubleType(), nullable=False),
        T.StructField("Payment Currency", T.StringType(), nullable=False),
        T.StructField("Payment Format", T.StringType(), nullable=False),
        T.StructField("Is Laundering", T.IntegerType(), nullable=False),
    ]
)

ACCOUNT_SCHEMA = T.StructType(
    [
        T.StructField("Bank Name", T.StringType(), nullable=False),
        T.StructField("Bank ID", T.LongType(), nullable=False),
        T.StructField("Account Number", T.StringType(), nullable=False),
        T.StructField("Entity ID", T.StringType(), nullable=False),
        T.StructField("Entity Name", T.StringType(), nullable=False),
    ]
)

SAMPLE_ROW_LIMIT = 100_000

SPARK_OUTPUT_DIR = (
    PROCESSED_DATA_DIR / "spark_static_sample"
)

ACCOUNT_FEATURES_OUTPUT = (
    SPARK_OUTPUT_DIR / "node_account_features.parquet"
)

ACCOUNT_TO_ACCOUNT_EDGES_OUTPUT = (
    SPARK_OUTPUT_DIR / "edge_account_to_account.parquet"
)

BANK_FEATURES_OUTPUT = (
    SPARK_OUTPUT_DIR / "node_bank_features.parquet"
)

ENTITY_FEATURES_OUTPUT = (
    SPARK_OUTPUT_DIR / "node_entity_features.parquet"
)

ACCOUNT_TO_BANK_EDGES_OUTPUT = (
    SPARK_OUTPUT_DIR / "edge_account_to_bank.parquet"
)

ENTITY_TO_ACCOUNT_EDGES_OUTPUT = (
    SPARK_OUTPUT_DIR / "edge_entity_to_account.parquet"
)

ACCOUNT_LABELS_OUTPUT = (
    SPARK_OUTPUT_DIR / "account_labels.parquet"
)

ACCOUNT_FEATURE_COLUMNS = [
    "outgoing_txn_count",
    "incoming_txn_count",
    "outgoing_unique_counterparties",
    "incoming_unique_counterparties",
    "repeat_counterparty_ratio",
    "self_transfer_ratio",
    "outgoing_total_paid",
    "incoming_total_received",
    "outgoing_avg_paid",
    "incoming_avg_received",
    "outgoing_amount_std",
    "incoming_amount_std",
    "payment_currency_diversity",
    "receiving_currency_diversity",
    "currency_switch_ratio",
    "payment_format_diversity",
    "unique_connected_banks",
    "cross_bank_txn_ratio",
    "odd_hour_txn_ratio",
    "weekend_txn_ratio",
    "active_span_hours",
    "hours_since_last_txn",
    "max_txns_per_hour",
    "avg_inter_txn_gap_hours",
]

ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS = [
    "edge_txn_count",
    "edge_total_paid",
    "edge_avg_paid",
    "edge_amount_std",
    "edge_is_repeated",
    "edge_recency_hours",
    "edge_currency_switch_ratio",
    "edge_payment_format_diversity",
    "edge_inter_txn_gap_hours",
]

BANK_FEATURE_COLUMNS = [
    "bank_account_count",
    "outgoing_txn_count",
    "incoming_txn_count",
    "outgoing_total_paid",
    "incoming_total_received",
    "outgoing_avg_paid",
    "incoming_avg_received",
    "unique_sender_accounts",
    "unique_receiver_accounts",
    "counterparty_bank_count",
    "currency_diversity",
    "payment_format_diversity",
    "cross_bank_txn_ratio",
    "odd_hour_txn_ratio",
    "weekend_txn_ratio",
]

ENTITY_FEATURE_COLUMNS = [
    "entity_type_sole_proprietorship",
    "entity_type_partnership",
    "entity_type_corporation",
    "entity_type_country",
    "entity_type_other",
    "owned_account_count",
    "connected_bank_count",
    "outgoing_txn_count",
    "incoming_txn_count",
    "outgoing_total_paid",
    "incoming_total_received",
    "unique_counterparty_accounts",
    "cross_bank_txn_ratio",
    "odd_hour_txn_ratio",
    "payment_format_diversity",
]

def parse_run_config():
    """
    Read the requested static-pipeline mode and optional time cutoff.

    When end_timestamp is supplied, only transactions strictly before that
    timestamp are included in the static graph baseline.
    """
    parser = argparse.ArgumentParser(
        description="Build MuleDetector static Spark graph outputs."
    )

    mode = parser.add_mutually_exclusive_group()

    mode.add_argument(
        "--max-rows",
        type=int,
        default=SAMPLE_ROW_LIMIT,
        help=(
            "Run a bounded sample after applying any time cutoff. "
            "Default: 100000 rows."
        ),
    )

    mode.add_argument(
        "--full",
        action="store_true",
        help="Run every transaction that passes any time cutoff.",
    )

    parser.add_argument(
        "--end-timestamp",
        help=(
            "Exclusive static-baseline cutoff in YYYY/MM/DD HH:MM format. "
            "Only transactions before this time are included."
        ),
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Output directory for this run. Required when "
            "--end-timestamp is used, so spark_static_full is protected."
        ),
    )

    parser.add_argument(
        "--write-account-events",
        action="store_true",
        help=(
            "Also save the canonical Account-event table. "
            "Required when this static output will seed streaming."
        ),
    )

    args = parser.parse_args()

    if args.end_timestamp is not None:
        try:
            datetime.strptime(
                args.end_timestamp,
                "%Y/%m/%d %H:%M",
            )
        except ValueError as error:
            raise ValueError(
                "--end-timestamp must use YYYY/MM/DD HH:MM format."
            ) from error

        if args.output_dir is None:
            raise ValueError(
                "--output-dir is required with --end-timestamp. "
                "This prevents overwriting spark_static_full."
            )

    if args.full:
        max_rows = None
        default_output_dir = (
            PROCESSED_DATA_DIR / "spark_static_full"
        )
    else:
        if args.max_rows <= 0:
            raise ValueError("--max-rows must be greater than zero.")

        max_rows = args.max_rows
        default_output_dir = (
            PROCESSED_DATA_DIR
            / f"spark_static_sample_{args.max_rows}"
        )

    output_dir = args.output_dir or default_output_dir

    return (
        max_rows,
        output_dir,
        args.end_timestamp,
        args.write_account_events,
    )

def build_output_paths(output_dir):
    """
    Build all Parquet output paths for one pipeline run.
    """
    return {
        "account_events": (
            output_dir / "account_events.parquet"
        ),
        "account_features": (
            output_dir / "node_account_features.parquet"
        ),
        "account_to_account_edges": (
            output_dir / "edge_account_to_account.parquet"
        ),
        "bank_features": (
            output_dir / "node_bank_features.parquet"
        ),
        "entity_features": (
            output_dir / "node_entity_features.parquet"
        ),
        "account_to_bank_edges": (
            output_dir / "edge_account_to_bank.parquet"
        ),
        "entity_to_account_edges": (
            output_dir / "edge_entity_to_account.parquet"
        ),
        "account_labels": (
            output_dir / "account_labels.parquet"
        ),
    }


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("MuleDetector-Static-Feature-Pipeline")
        .master("local[2]")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.autoBroadcastJoinThreshold", "200m")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_source_data(spark: SparkSession):
    """
    Read raw CSV files using explicit schemas.

    Explicit schemas are important: Spark does not need a costly extra scan to
    guess every column's data type.
    """
    raw_transactions = (
        spark.read
        .option("header", "false")
        .schema(TRANSACTION_SCHEMA)
        .csv(str(TRANSACTIONS_FILE))
    )

    # The source file has two "Account" headers. Reading it as data lets our
    # explicit positional schema safely name the fifth column Account.1.
    transactions = raw_transactions.filter(
        F.col("Timestamp") != "Timestamp"
    )

    accounts = (
        spark.read
        .option("header", "true")
        .schema(ACCOUNT_SCHEMA)
        .csv(str(ACCOUNTS_FILE))
    )

    return transactions, accounts

def build_enriched_transactions(
    transactions,
    accounts,
):
    """
    Attach sender and receiver metadata, then create graph-ready columns.
    """

    sender_metadata = accounts.select(
        F.col("Bank ID").alias("sender_join_bank_id"),
        F.col("Account Number").alias("sender_join_account"),
        F.col("Bank Name").alias("sender_bank_name"),
        F.col("Entity ID").alias("sender_entity_id"),
        F.col("Entity Name").alias("sender_entity_name"),
    )

    receiver_metadata = accounts.select(
        F.col("Bank ID").alias("receiver_join_bank_id"),
        F.col("Account Number").alias("receiver_join_account"),
        F.col("Bank Name").alias("receiver_bank_name"),
        F.col("Entity ID").alias("receiver_entity_id"),
        F.col("Entity Name").alias("receiver_entity_name"),
    )

    sender_lookup = F.broadcast(sender_metadata)
    receiver_lookup = F.broadcast(receiver_metadata)

    enriched = (
        transactions
        .join(
            sender_lookup,
            (
                (transactions["From Bank"] == sender_lookup["sender_join_bank_id"])
                & (transactions["Account"] == sender_lookup["sender_join_account"])
            ),
            "left",
        )
        .drop(
            "sender_join_bank_id",
            "sender_join_account",
        )
    )

    enriched = (
        enriched
        .join(
            receiver_lookup,
            (
                (F.col("To Bank") == receiver_lookup["receiver_join_bank_id"])
                & (
                    F.col("`Account.1`")
                    == receiver_lookup["receiver_join_account"]
                )
            ),
            "left",
        )
        .drop(
            "receiver_join_bank_id",
            "receiver_join_account",
        )
    )

    enriched = (
        enriched
        .withColumn(
            "event_time",
            F.to_timestamp("Timestamp", "yyyy/MM/dd HH:mm"),
        )
        .withColumn(
            "sender_account_id",
            F.concat_ws(
                "::",
                F.col("From Bank").cast("string"),
                F.col("Account"),
            ),
        )
        .withColumn(
            "receiver_account_id",
            F.concat_ws(
                "::",
                F.col("To Bank").cast("string"),
                F.col("`Account.1`"),
            ),
        )
    )

    enriched = (
        enriched
        .withColumn(
            "is_currency_switch",
            (
                F.col("Payment Currency")
                != F.col("Receiving Currency")
            ).cast("int"),
        )
        .withColumn(
            "is_cross_bank",
            (
                F.col("From Bank") != F.col("To Bank")
            ).cast("int"),
        )
        .withColumn(
            "is_odd_hour",
            F.hour("event_time").between(0, 5).cast("int"),
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("event_time").isin([1, 7]).cast("int"),
        )
        .withColumn(
            "is_self_transfer",
            (
                F.col("sender_account_id")
                == F.col("receiver_account_id")
            ).cast("int"),
        )
    )

    return enriched

def build_account_events(enriched_transactions):
    """
    Convert each enriched transaction into:
    - one outgoing event for the sender
    - one incoming event for the receiver

    This is the shared source for Account node features.
    """
    outgoing_events = enriched_transactions.select(
        F.col("sender_account_id").alias("account_id"),
        F.col("receiver_account_id").alias("counterparty_account_id"),
        F.col("From Bank").alias("bank_id"),
        F.col("To Bank").alias("counterparty_bank_id"),
        F.col("sender_entity_id").alias("entity_id"),
        F.col("event_time"),
        F.lit("outgoing").alias("direction"),
        F.col("Amount Paid").cast("double").alias("amount"),
        F.col("Payment Currency").alias("currency"),
        F.col("Payment Format").alias("payment_format"),
        F.col("is_currency_switch"),
        F.col("is_cross_bank"),
        F.col("is_odd_hour"),
        F.col("is_weekend"),
        F.col("is_self_transfer"),
    )

    incoming_events = enriched_transactions.select(
        F.col("receiver_account_id").alias("account_id"),
        F.col("sender_account_id").alias("counterparty_account_id"),
        F.col("To Bank").alias("bank_id"),
        F.col("From Bank").alias("counterparty_bank_id"),
        F.col("receiver_entity_id").alias("entity_id"),
        F.col("event_time"),
        F.lit("incoming").alias("direction"),
        F.col("Amount Received").cast("double").alias("amount"),
        F.col("Receiving Currency").alias("currency"),
        F.col("Payment Format").alias("payment_format"),
        F.col("is_currency_switch"),
        F.col("is_cross_bank"),
        F.col("is_odd_hour"),
        F.col("is_weekend"),
        F.col("is_self_transfer"),
    )

    return outgoing_events.unionByName(incoming_events)

def build_bank_events(account_events):
    """
    Reuse Account events to create the canonical Bank-event table.

    One transaction contributes:
    - one outgoing event to the sender's bank
    - one incoming event to the receiver's bank
    """
    return account_events.select(
        F.col("bank_id"),
        F.col("counterparty_bank_id"),
        F.col("account_id").alias("participant_account_id"),
        F.col("event_time"),
        F.col("direction"),
        F.col("amount"),
        F.col("currency"),
        F.col("payment_format"),
        F.col("is_cross_bank"),
        F.col("is_odd_hour"),
        F.col("is_weekend"),
    )

def build_entity_events(account_events):
    """
    Reuse Account events to create the canonical Entity-event table.
    """
    return account_events.select(
        F.col("entity_id"),
        F.col("bank_id"),
        F.col("account_id"),
        F.col("counterparty_account_id"),
        F.col("event_time"),
        F.col("direction"),
        F.col("amount"),
        F.col("payment_format"),
        F.col("is_cross_bank"),
        F.col("is_odd_hour"),
    )

def compute_entity_features(accounts, entity_events):
    """
    Create one final feature row per Entity node.
    """
    account_metadata = accounts.select(
        F.col("Entity ID").alias("entity_id"),
        F.col("Entity Name").alias("entity_name"),
        F.col("Bank ID").alias("bank_id"),
        F.concat_ws(
            "::",
            F.col("Bank ID"),
            F.col("Account Number"),
        ).alias("account_id"),
    )

    entity_names = account_metadata.groupBy("entity_id").agg(
        F.min("entity_name").alias("entity_name")
    )

    entity_types = entity_names.select(
        "entity_id",
        F.when(
            F.lower("entity_name").contains("sole proprietorship"),
            1.0,
        ).otherwise(0.0).alias(
            "entity_type_sole_proprietorship"
        ),
        F.when(
            F.lower("entity_name").contains("partnership"),
            1.0,
        ).otherwise(0.0).alias("entity_type_partnership"),
        F.when(
            F.lower("entity_name").contains("corporation"),
            1.0,
        ).otherwise(0.0).alias("entity_type_corporation"),
        F.when(
            F.lower("entity_name").contains("country"),
            1.0,
        ).otherwise(0.0).alias("entity_type_country"),
        F.when(
            ~(
                F.lower("entity_name").contains("sole proprietorship")
                | F.lower("entity_name").contains("partnership")
                | F.lower("entity_name").contains("corporation")
                | F.lower("entity_name").contains("country")
            ),
            1.0,
        ).otherwise(0.0).alias("entity_type_other"),
    )

    active_entities = entity_events.select(
        "entity_id"
    ).distinct()

    ownership_features = entity_events.groupBy("entity_id").agg(
        F.countDistinct("account_id").alias("owned_account_count"),
        F.countDistinct("bank_id").alias("connected_bank_count"),
    )

    outgoing_features = (
        entity_events
        .filter(F.col("direction") == "outgoing")
        .groupBy("entity_id")
        .agg(
            F.count("*").alias("outgoing_txn_count"),
            F.sum("amount").alias("outgoing_total_paid"),
        )
    )

    incoming_features = (
        entity_events
        .filter(F.col("direction") == "incoming")
        .groupBy("entity_id")
        .agg(
            F.count("*").alias("incoming_txn_count"),
            F.sum("amount").alias("incoming_total_received"),
        )
    )

    shared_features = entity_events.groupBy("entity_id").agg(
        F.countDistinct("counterparty_account_id").alias(
            "unique_counterparty_accounts"
        ),
        F.avg("is_cross_bank").alias("cross_bank_txn_ratio"),
        F.avg("is_odd_hour").alias("odd_hour_txn_ratio"),
        F.countDistinct("payment_format").alias(
            "payment_format_diversity"
        ),
    )

    combined_features = (
        active_entities
        .join(entity_types, on="entity_id", how="left")
        .join(ownership_features, on="entity_id", how="left")
        .join(outgoing_features, on="entity_id", how="left")
        .join(incoming_features, on="entity_id", how="left")
        .join(shared_features, on="entity_id", how="left")
        .fillna(0)
    )

    return combined_features.select(
        F.col("entity_id"),
        *[
            F.col(feature_name).cast("double").alias(feature_name)
            for feature_name in ENTITY_FEATURE_COLUMNS
        ],
    )

def build_metadata_relations(account_events):
    """
    Build the two metadata-based graph relations for active Account nodes.
    """
    account_to_bank = account_events.select(
        "account_id",
        "bank_id",
    ).distinct()

    entity_to_account = account_events.select(
        "entity_id",
        "account_id",
    ).distinct()

    return account_to_bank, entity_to_account

def validate_metadata_relations(
    account_to_bank,
    entity_to_account,
):
    """
    Ensure every active Account maps to exactly one Bank and one Entity.
    """
    accounts_with_multiple_banks = (
        account_to_bank
        .groupBy("account_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    accounts_with_multiple_entities = (
        entity_to_account
        .groupBy("account_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if accounts_with_multiple_banks != 0:
        raise ValueError(
            "One or more Accounts map to multiple Banks."
        )

    if accounts_with_multiple_entities != 0:
        raise ValueError(
            "One or more Accounts map to multiple Entities."
        )

def build_account_labels(
    enriched_transactions,
    final_account_features,
):
    """
    Create offline Account labels only.

    Is Laundering is never used as a model feature.
    """
    laundering_transactions = enriched_transactions.filter(
        F.col("Is Laundering") == 1
    )

    positive_sender_accounts = laundering_transactions.select(
        F.col("sender_account_id").alias("account_id")
    )

    positive_receiver_accounts = laundering_transactions.select(
        F.col("receiver_account_id").alias("account_id")
    )

    positive_accounts = (
        positive_sender_accounts
        .unionByName(positive_receiver_accounts)
        .distinct()
        .withColumn("account_label", F.lit(1))
    )

    return (
        final_account_features
        .select("account_id")
        .join(
            positive_accounts,
            on="account_id",
            how="left",
        )
        .fillna(0, subset=["account_label"])
        .select(
            "account_id",
            F.col("account_label").cast("int"),
        )
    )

def validate_final_static_outputs(
    final_account_features,
    final_bank_features,
    final_entity_features,
    final_account_to_account_edges,
    account_to_bank,
    entity_to_account,
    account_labels,
):
    """
    Validate the static graph-output contract before handoff to the model.
    """
    expected_account_columns = (
        ["account_id"] + ACCOUNT_FEATURE_COLUMNS
    )

    expected_bank_columns = (
        ["bank_id"] + BANK_FEATURE_COLUMNS
    )

    expected_entity_columns = (
        ["entity_id"] + ENTITY_FEATURE_COLUMNS
    )

    expected_edge_columns = [
        "sender_account_id",
        "receiver_account_id",
    ] + ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS

    if final_account_features.columns != expected_account_columns:
        raise ValueError("Account feature column contract failed.")

    if final_bank_features.columns != expected_bank_columns:
        raise ValueError("Bank feature column contract failed.")

    if final_entity_features.columns != expected_entity_columns:
        raise ValueError("Entity feature column contract failed.")

    if final_account_to_account_edges.columns != expected_edge_columns:
        raise ValueError("Account edge column contract failed.")

    account_null_rows = final_account_features.filter(
        F.greatest(
            *[
                F.col(column_name).isNull().cast("int")
                for column_name in ACCOUNT_FEATURE_COLUMNS
            ]
        ) == 1
    ).count()

    bank_null_rows = final_bank_features.filter(
        F.greatest(
            *[
                F.col(column_name).isNull().cast("int")
                for column_name in BANK_FEATURE_COLUMNS
            ]
        ) == 1
    ).count()

    entity_null_rows = final_entity_features.filter(
        F.greatest(
            *[
                F.col(column_name).isNull().cast("int")
                for column_name in ENTITY_FEATURE_COLUMNS
            ]
        ) == 1
    ).count()

    edge_null_rows = final_account_to_account_edges.filter(
        F.greatest(
            *[
                F.col(column_name).isNull().cast("int")
                for column_name in ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS
            ]
        ) == 1
    ).count()

    account_node_ids = final_account_features.select("account_id")
    bank_node_ids = final_bank_features.select("bank_id")
    entity_node_ids = final_entity_features.select("entity_id")

    orphan_account_to_bank_accounts = (
        account_to_bank
        .join(account_node_ids, on="account_id", how="left_anti")
        .count()
    )

    orphan_account_to_bank_banks = (
        account_to_bank
        .join(bank_node_ids, on="bank_id", how="left_anti")
        .count()
    )

    orphan_entity_to_account_entities = (
        entity_to_account
        .join(entity_node_ids, on="entity_id", how="left_anti")
        .count()
    )

    orphan_entity_to_account_accounts = (
        entity_to_account
        .join(account_node_ids, on="account_id", how="left_anti")
        .count()
    )

    invalid_labels = account_labels.filter(
        ~F.col("account_label").isin(0, 1)
    ).count()

    missing_labels = (
        account_node_ids
        .join(
            account_labels.select("account_id"),
            on="account_id",
            how="left_anti",
        )
        .count()
    )

    failures = {
        "Account rows with missing features": account_null_rows,
        "Bank rows with missing features": bank_null_rows,
        "Entity rows with missing features": entity_null_rows,
        "Account edge rows with missing features": edge_null_rows,
        "Orphan Account to Bank account IDs": (
            orphan_account_to_bank_accounts
        ),
        "Orphan Account to Bank bank IDs": (
            orphan_account_to_bank_banks
        ),
        "Orphan Entity to Account entity IDs": (
            orphan_entity_to_account_entities
        ),
        "Orphan Entity to Account account IDs": (
            orphan_entity_to_account_accounts
        ),
        "Invalid labels": invalid_labels,
        "Accounts missing labels": missing_labels,
    }

    failed_checks = {
        name: value
        for name, value in failures.items()
        if value != 0
    }

    if failed_checks:
        raise ValueError(
            f"Static output validation failed: {failed_checks}"
        )

    print("\nStatic graph-output validation passed.")
    print(
        "Account nodes:",
        final_account_features.count(),
    )
    print(
        "Bank nodes:",
        final_bank_features.count(),
    )
    print(
        "Entity nodes:",
        final_entity_features.count(),
    )
    print(
        "Account to Account edges:",
        final_account_to_account_edges.count(),
    )
    print(
        "Account to Bank edges:",
        account_to_bank.count(),
    )
    print(
        "Entity to Account edges:",
        entity_to_account.count(),
    )
    print(
        "Positive Account labels:",
        account_labels.filter(
            F.col("account_label") == 1
        ).count(),
    )

def compute_bank_features(accounts, bank_events):
    """
    Create one final feature row per Bank node.
    """
    all_banks = accounts.select(
        F.col("Bank ID").alias("bank_id")
    ).distinct()

    bank_account_counts = accounts.groupBy(
        F.col("Bank ID").alias("bank_id")
    ).agg(
        F.countDistinct("Account Number").alias(
            "bank_account_count"
        )
    )

    outgoing = bank_events.filter(
        F.col("direction") == "outgoing"
    )

    outgoing_features = outgoing.groupBy("bank_id").agg(
        F.count("*").alias("outgoing_txn_count"),
        F.sum("amount").alias("outgoing_total_paid"),
        F.avg("amount").alias("outgoing_avg_paid"),
        F.countDistinct("participant_account_id").alias(
            "unique_sender_accounts"
        ),
    )

    incoming = bank_events.filter(
        F.col("direction") == "incoming"
    )

    incoming_features = incoming.groupBy("bank_id").agg(
        F.count("*").alias("incoming_txn_count"),
        F.sum("amount").alias("incoming_total_received"),
        F.avg("amount").alias("incoming_avg_received"),
        F.countDistinct("participant_account_id").alias(
            "unique_receiver_accounts"
        ),
    )

    shared_features = bank_events.groupBy("bank_id").agg(
        F.countDistinct("counterparty_bank_id").alias(
            "counterparty_bank_count"
        ),
        F.countDistinct("currency").alias("currency_diversity"),
        F.countDistinct("payment_format").alias(
            "payment_format_diversity"
        ),
        F.avg("is_cross_bank").alias("cross_bank_txn_ratio"),
        F.avg("is_odd_hour").alias("odd_hour_txn_ratio"),
        F.avg("is_weekend").alias("weekend_txn_ratio"),
    )

    combined_features = (
        all_banks
        .join(bank_account_counts, on="bank_id", how="left")
        .join(outgoing_features, on="bank_id", how="left")
        .join(incoming_features, on="bank_id", how="left")
        .join(shared_features, on="bank_id", how="left")
        .fillna(0)
    )

    return combined_features.select(
        F.col("bank_id"),
        *[
            F.col(feature_name).cast("double").alias(feature_name)
            for feature_name in BANK_FEATURE_COLUMNS
        ],
    )

def compute_directional_account_features(account_events):
    """
    Compute the outgoing and incoming Account features separately.

    Returns one row per account with 12 directional features.
    """
    all_accounts = account_events.select("account_id").distinct()

    outgoing = account_events.filter(F.col("direction") == "outgoing")
    incoming = account_events.filter(F.col("direction") == "incoming")

    outgoing_features = outgoing.groupBy("account_id").agg(
        F.count("*").alias("outgoing_txn_count"),
        F.countDistinct("counterparty_account_id").alias(
            "outgoing_unique_counterparties"
        ),
        F.sum("amount").alias("outgoing_total_paid"),
        F.avg("amount").alias("outgoing_avg_paid"),
        F.stddev_samp("amount").alias("outgoing_amount_std"),
        F.countDistinct("currency").alias("payment_currency_diversity"),
    )

    incoming_features = incoming.groupBy("account_id").agg(
        F.count("*").alias("incoming_txn_count"),
        F.countDistinct("counterparty_account_id").alias(
            "incoming_unique_counterparties"
        ),
        F.sum("amount").alias("incoming_total_received"),
        F.avg("amount").alias("incoming_avg_received"),
        F.stddev_samp("amount").alias("incoming_amount_std"),
        F.countDistinct("currency").alias("receiving_currency_diversity"),
    )

    return (
        all_accounts
        .join(outgoing_features, on="account_id", how="left")
        .join(incoming_features, on="account_id", how="left")
        .fillna(0)
    )

def compute_shared_account_features(account_events):
    """
    Compute Account features based on all incoming and outgoing events.

    Some helper columns are retained temporarily for later temporal features.
    """
    shared_features = account_events.groupBy("account_id").agg(
        F.count("*").alias("total_event_count"),
        F.countDistinct("counterparty_account_id").alias(
            "unique_counterparties"
        ),
        F.avg("is_self_transfer").alias("self_transfer_ratio"),
        F.avg("is_currency_switch").alias("currency_switch_ratio"),
        F.countDistinct("payment_format").alias(
            "payment_format_diversity"
        ),
        F.countDistinct("counterparty_bank_id").alias(
            "unique_connected_banks"
        ),
        F.avg("is_cross_bank").alias("cross_bank_txn_ratio"),
        F.avg("is_odd_hour").alias("odd_hour_txn_ratio"),
        F.avg("is_weekend").alias("weekend_txn_ratio"),
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("last_event_time"),
    )

    return shared_features.withColumn(
        "repeat_counterparty_ratio",
        F.col("total_event_count") / F.col("unique_counterparties"),
    )

def compute_temporal_account_features(
    account_events,
    snapshot_time=None,
):
    """
    Compute Account temporal and transaction-velocity features.

    When ``snapshot_time`` is supplied, it must be a one-row DataFrame with
    a ``snapshot_time`` column. This lets incremental updates use the global
    stream time while calculating features for only affected Accounts.
    """
    account_bounds = account_events.groupBy("account_id").agg(
        F.min("event_time").alias("first_event_time"),
        F.max("event_time").alias("last_event_time"),
    )

    if snapshot_time is None:
        snapshot_time = account_events.agg(
            F.max("event_time").alias("snapshot_time")
        )

    activity_span_and_recency = (
        account_bounds
        .crossJoin(snapshot_time)
        .select(
            "account_id",
            (
                F.col("last_event_time").cast("long")
                - F.col("first_event_time").cast("long")
            )
            .cast("double")
            .__truediv__(3600.0)
            .alias("active_span_hours"),
            (
                F.col("snapshot_time").cast("long")
                - F.col("last_event_time").cast("long")
            )
            .cast("double")
            .__truediv__(3600.0)
            .alias("hours_since_last_txn"),
        )
    )

    hourly_activity = (
        account_events
        .withColumn(
            "hour_bucket",
            F.date_trunc("hour", F.col("event_time")),
        )
        .groupBy("account_id", "hour_bucket")
        .agg(F.count("*").alias("txns_in_hour"))
        .groupBy("account_id")
        .agg(
            F.max("txns_in_hour").alias("max_txns_per_hour")
        )
    )

    event_order = Window.partitionBy("account_id").orderBy(
        F.col("event_time"),
        F.col("direction"),
        F.col("counterparty_account_id"),
    )

    average_gap = (
        account_events
        .withColumn(
            "previous_event_time",
            F.lag("event_time").over(event_order),
        )
        .withColumn(
            "inter_txn_gap_hours",
            (
                F.col("event_time").cast("long")
                - F.col("previous_event_time").cast("long")
            )
            .cast("double")
            .__truediv__(3600.0),
        )
        .groupBy("account_id")
        .agg(
            F.avg("inter_txn_gap_hours").alias(
                "avg_inter_txn_gap_hours"
            )
        )
    )

    return (
        activity_span_and_recency
        .join(hourly_activity, on="account_id", how="left")
        .join(average_gap, on="account_id", how="left")
        .fillna(0)
    )

def finalize_account_features(account_feature_stage):
    """
    Keep only account_id and the final 24 Account features,
    in the exact order required by the graph/model contract.
    """
    return account_feature_stage.select(
        F.col("account_id"),
        *[
            F.col(feature_name).cast("double").alias(feature_name)
            for feature_name in ACCOUNT_FEATURE_COLUMNS
        ],
    )

def compute_base_account_to_account_edges(enriched_transactions):
    """
    Create one directed Account -> Account relationship per sender/receiver pair.

    edge_last_event_time is a temporary helper for edge recency.
    """
    edges = enriched_transactions.groupBy(
        "sender_account_id",
        "receiver_account_id",
    ).agg(
        F.count("*").alias("edge_txn_count"),
        F.sum("Amount Paid").alias("edge_total_paid"),
        F.avg("Amount Paid").alias("edge_avg_paid"),
        F.stddev_samp("Amount Paid").alias("edge_amount_std"),
        F.max("event_time").alias("edge_last_event_time"),
        F.avg("is_currency_switch").alias(
            "edge_currency_switch_ratio"
        ),
        F.countDistinct("Payment Format").alias(
            "edge_payment_format_diversity"
        ),
    )

    return edges.withColumn(
        "edge_is_repeated",
        F.when(F.col("edge_txn_count") > 1, F.lit(1.0))
        .otherwise(F.lit(0.0)),
    )

def finalize_account_to_account_edges(
    base_edges,
    enriched_transactions,
    snapshot_time=None,
):
    """
    Add temporal edge features and keep the final nine edge features
    in the fixed graph/model order.
    """
    if snapshot_time is None:
        snapshot_time = enriched_transactions.agg(
            F.max("event_time").alias("snapshot_time")
        )

    edges_with_recency = (
        base_edges
        .crossJoin(snapshot_time)
        .withColumn(
            "edge_recency_hours",
            (
                F.col("snapshot_time").cast("long")
                - F.col("edge_last_event_time").cast("long")
            )
            / F.lit(3600.0),
        )
    )

    edge_order = Window.partitionBy(
        "sender_account_id",
        "receiver_account_id",
    ).orderBy(
        F.col("event_time"),
        F.col("Amount Paid"),
        F.col("Payment Format"),
    )

    average_gap = (
        enriched_transactions
        .withColumn(
            "previous_edge_event_time",
            F.lag("event_time").over(edge_order),
        )
        .withColumn(
            "edge_inter_txn_gap_hours",
            (
                F.col("event_time").cast("long")
                - F.col("previous_edge_event_time").cast("long")
            )
            / F.lit(3600.0),
        )
        .groupBy(
            "sender_account_id",
            "receiver_account_id",
        )
        .agg(
            F.avg("edge_inter_txn_gap_hours").alias(
                "edge_inter_txn_gap_hours"
            )
        )
    )

    combined_edges = (
        edges_with_recency
        .join(
            average_gap,
            on=[
                "sender_account_id",
                "receiver_account_id",
            ],
            how="left",
        )
        .fillna(0)
    )

    return combined_edges.select(
        "sender_account_id",
        "receiver_account_id",
        *[
            F.col(feature_name).cast("double").alias(feature_name)
            for feature_name in ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS
        ],
    )

def validate_enriched_transactions(enriched_transactions) -> None:
    """
    Validate that the two metadata joins and timestamp parsing succeeded.
    """
    validation_result = (
        enriched_transactions
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum(
                F.when(
                    F.col("sender_entity_id").isNull(),
                    1,
                ).otherwise(0)
            ).alias("missing_sender_metadata"),
            F.sum(
                F.when(
                    F.col("receiver_entity_id").isNull(),
                    1,
                ).otherwise(0)
            ).alias("missing_receiver_metadata"),
            F.sum(
                F.when(
                    F.col("event_time").isNull(),
                    1,
                ).otherwise(0)
            ).alias("invalid_timestamps"),
        )
        .first()
    )

    print("\nSpark validation results:")
    print(
        "Transactions:",
        validation_result["transaction_count"],
    )
    print(
        "Missing sender metadata:",
        validation_result["missing_sender_metadata"],
    )
    print(
        "Missing receiver metadata:",
        validation_result["missing_receiver_metadata"],
    )
    print(
        "Invalid timestamps:",
        validation_result["invalid_timestamps"],
    )

    if (
        validation_result["missing_sender_metadata"] != 0
        or validation_result["missing_receiver_metadata"] != 0
        or validation_result["invalid_timestamps"] != 0
    ):
        raise ValueError(
            "Enriched transaction validation failed. "
            "Do not continue to feature engineering."
        )




def main() -> None:
    (
        max_rows,
        output_dir,
        end_timestamp,
        write_account_events,
    ) = parse_run_config()
    output_paths = build_output_paths(output_dir)

    spark = create_spark_session()
    enriched_transactions = None

    try:
        print("Reading raw HI-Medium source files with Spark...")
        transactions, accounts = read_source_data(spark)
        if end_timestamp is not None:
            print(
                "\nBuilding static baseline from transactions before "
                f"{end_timestamp}..."
            )

            transactions = (
                transactions
                .withColumn(
                    "_cutoff_event_time",
                    F.to_timestamp(
                        "Timestamp",
                        "yyyy/MM/dd HH:mm",
                    ),
                )
                .filter(
                    F.col("_cutoff_event_time")
                    < F.to_timestamp(
                        F.lit(end_timestamp),
                        "yyyy/MM/dd HH:mm",
                    )
                )
                .drop("_cutoff_event_time")
            )

        if max_rows is None:
            print(
                "\nRunning the complete static historical dataset..."
            )
        else:
            print(
                f"\nLimiting this Spark run to "
                f"{max_rows:,} transaction rows..."
            )
            transactions = transactions.limit(max_rows)

        print("\nBuilding the two-sided enriched transaction DataFrame...")
        enriched_transactions = (
            build_enriched_transactions(
                transactions,
                accounts,
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        validate_enriched_transactions(enriched_transactions)

        print("\nCreating outgoing and incoming Account events...")

        account_events = build_account_events(enriched_transactions).persist(
            StorageLevel.MEMORY_AND_DISK
        )

        print(f"Account events: {account_events.count():,}")
        account_events.show(5, truncate=False)

        if write_account_events:
            print(
                "\nWriting canonical baseline Account events to Parquet..."
            )

            (
                account_events.write
                .mode("overwrite")
                .parquet(str(output_paths["account_events"]))
            )

            print(f"Saved: {output_paths['account_events']}")

        print("\nComputing directional Account features...")

        directional_account_features = compute_directional_account_features(
            account_events
        ).persist(StorageLevel.MEMORY_AND_DISK)

        print(
            "Accounts with directional features:",
            f"{directional_account_features.count():,}",
        )
        directional_account_features.show(5, truncate=False)

        print("\nComputing shared Account behaviour features...")

        shared_account_features = compute_shared_account_features(
            account_events
        ).persist(StorageLevel.MEMORY_AND_DISK)

        account_feature_stage = directional_account_features.join(
            shared_account_features,
            on="account_id",
            how="inner",
        )

        print(
            "Accounts after combining feature stages:",
            f"{account_feature_stage.count():,}",
        )
        account_feature_stage.show(5, truncate=False)

        print("\nComputing temporal Account features...")

        temporal_account_features = compute_temporal_account_features(
            account_events
        ).persist(StorageLevel.MEMORY_AND_DISK)

        account_feature_stage = account_feature_stage.join(
            temporal_account_features,
            on="account_id",
            how="inner",
        )

        print(
            "Accounts after temporal features:",
            f"{account_feature_stage.count():,}",
        )
        account_feature_stage.show(5, truncate=False)

        print("\nFinalizing the 24 Account node features...")

        final_account_features = finalize_account_features(
            account_feature_stage
        )

        print("Final Account feature columns:")
        print(final_account_features.columns)
        final_account_features.show(5, truncate=False)

        print("\nWriting final Account node features to Parquet...")

        (
            final_account_features.write
            .mode("overwrite")
            .parquet(str(output_paths["account_features"]))
        )

        print(f"Saved: {output_paths['account_features']}")

        print("\nComputing base Account to Account edge features...")

        base_account_to_account_edges = (
            compute_base_account_to_account_edges(
                enriched_transactions
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        print(
            "Directed Account to Account edges:",
            f"{base_account_to_account_edges.count():,}",
        )
        base_account_to_account_edges.show(5, truncate=False)

        print("\nFinalizing Account to Account edge features...")

        final_account_to_account_edges = (
            finalize_account_to_account_edges(
                base_account_to_account_edges,
                enriched_transactions,
            )
        )

        print("Final Account to Account edge columns:")
        print(final_account_to_account_edges.columns)
        final_account_to_account_edges.show(5, truncate=False)

        print(
            "\nWriting final Account to Account edges to Parquet..."
        )

        (
            final_account_to_account_edges.write
            .mode("overwrite")
            .parquet(
                str(output_paths["account_to_account_edges"])
            )
        )

        print(
            f"Saved: {output_paths['account_to_account_edges']}"
        )

        print("\nCreating Bank events from Account events...")

        bank_events = build_bank_events(account_events).persist(
            StorageLevel.MEMORY_AND_DISK
        )

        print(f"Bank events: {bank_events.count():,}")
        bank_events.show(5, truncate=False)

        print("\nComputing final Bank node features...")

        final_bank_features = compute_bank_features(
            accounts,
            bank_events,
        )

        print("Final Bank feature columns:")
        print(final_bank_features.columns)
        print(
            "Bank nodes:",
            f"{final_bank_features.count():,}",
        )
        final_bank_features.show(5, truncate=False)
        print("\nWriting final Bank node features to Parquet...")

        (
            final_bank_features.write
            .mode("overwrite")
            .parquet(str(output_paths["bank_features"]))
        )

        print(f"Saved: {output_paths['bank_features']}")

        print("\nCreating Entity events from Account events...")

        entity_events = build_entity_events(account_events).persist(
            StorageLevel.MEMORY_AND_DISK
        )

        print(f"Entity events: {entity_events.count():,}")
        entity_events.show(5, truncate=False)
        print("\nComputing final Entity node features...")

        final_entity_features = compute_entity_features(
            accounts,
            entity_events,
        )

        print("Final Entity feature columns:")
        print(final_entity_features.columns)
        print(
            "Entity nodes:",
            f"{final_entity_features.count():,}",
        )
        final_entity_features.show(5, truncate=False)
        print("\nWriting final Entity node features to Parquet...")

        (
            final_entity_features.write
            .mode("overwrite")
            .parquet(str(output_paths["entity_features"]))
        )

        print(f"Saved: {output_paths['entity_features']}")

        print("\nCreating Account to Bank and Entity to Account relations...")

        account_to_bank, entity_to_account = build_metadata_relations(
            account_events
        )

        validate_metadata_relations(
            account_to_bank,
            entity_to_account,
        )

        print(
            "Account to Bank relation edges:",
            f"{account_to_bank.count():,}",
        )
        print(
            "Entity to Account relation edges:",
            f"{entity_to_account.count():,}",
        )

        account_to_bank.show(5, truncate=False)
        entity_to_account.show(5, truncate=False)

        print("\nWriting metadata relations to Parquet...")

        (
            account_to_bank.write
            .mode("overwrite")
            .parquet(
                str(output_paths["account_to_bank_edges"])
            )
        )

        (
            entity_to_account.write
            .mode("overwrite")
            .parquet(
                str(output_paths["entity_to_account_edges"])
            )
        )

        print(
            f"Saved: {output_paths['account_to_bank_edges']}"
        )
        print(
            f"Saved: {output_paths['entity_to_account_edges']}"
        )

        print("\nCreating separate offline Account labels...")

        account_labels = build_account_labels(
            enriched_transactions,
            final_account_features,
        )

        positive_label_count = account_labels.filter(
            F.col("account_label") == 1
        ).count()

        print(
            "Positive Account labels:",
            positive_label_count,
        )

        (
            account_labels.write
            .mode("overwrite")
            .parquet(str(output_paths["account_labels"]))
        )

        print(f"Saved: {output_paths['account_labels']}")

        print("\nValidating final static graph outputs...")

        validate_final_static_outputs(
            final_account_features,
            final_bank_features,
            final_entity_features,
            final_account_to_account_edges,
            account_to_bank,
            entity_to_account,
            account_labels,
        )

        print("\nEnriched transaction preview:")
        enriched_transactions.select(
            "Timestamp",
            "sender_account_id",
            "sender_bank_name",
            "sender_entity_id",
            "receiver_account_id",
            "receiver_bank_name",
            "receiver_entity_id",
            "is_currency_switch",
            "is_cross_bank",
            "is_odd_hour",
            "is_weekend",
            "is_self_transfer",
        ).show(5, truncate=False)

        print("\nSpark two-sided join stage completed successfully.")

    finally:
        if enriched_transactions is not None:
            enriched_transactions.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
