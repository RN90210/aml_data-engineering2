"""Prepare each Kafka batch once, then publish a complete native model package.

One Spark application shares parsing, validation, enrichment, and fixed lookups.
Only stateless input events are cached. Native stateful queries read a durable
one-file event batch and retain their own RocksDB checkpoints. The next event
file is not published until all component queries have committed. This preserves
Kafka batch boundaries even after a partial failure, without sharing stateful
output or calling stateful operators on a static foreachBatch DataFrame.

Use a fresh checkpoint root when switching from the standalone Kafka pipelines:
these queries have a Parquet source, not a Kafka source. Keep this staging data
with the checkpoints; neither may be removed while a run can be resumed.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import sys
from threading import BoundedSemaphore, Event
import time

if sys.platform == "win32":
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark import StorageLevel
from pyspark.sql import functions as F

from streaming.ingestion.spark_streaming_pipeline import (
    add_transaction_flags, add_transaction_rejection_reason, build_account_events,
    enrich_transaction_entities, load_entity_lookup, split_valid_and_rejected_events,
)
from streaming.ingestion.streaming_audit_pipeline import (
    metrics_batch_output_path, write_audit_batch,
)
from streaming.ingestion.account_relation_streaming_pipeline import write_account_relation_batch
from streaming.ingestion.stateful_account_streaming_pipeline import write_account_feature_batch
from streaming.ingestion.stateful_edge_streaming_pipeline import write_account_edge_batch
from streaming.ingestion.stateful_bank_streaming_pipeline import write_bank_feature_batch
from streaming.ingestion.stateful_entity_streaming_pipeline import write_entity_feature_batch
from streaming.kafka.spark_kafka_consumer import build_spark_session, parse_transaction_stream
from streaming.operations.package_incremental_model_updates import package_batch
from streaming.state.stateful_account_processor import build_stateful_account_features
from streaming.state.stateful_edge_processor import build_stateful_account_edges
from streaming.state.stateful_bank_processor import build_stateful_bank_features
from streaming.state.stateful_entity_processor import (
    ENTITY_TYPE_FEATURE_COLUMNS, build_stateful_entity_features,
)


COMPONENT_TABLES = {
    "account": ("account_features",),
    "edge": ("account_to_account_edges",),
    "bank": ("bank_features",),
    "entity": ("entity_features",),
    "relations": ("account_to_bank", "entity_to_account"),
}


def atomic_json(path: Path, record: dict) -> None:
    """Expose a complete control record using a same-directory replacement."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def initialize_run(args) -> None:
    """Prevent accidental reuse of another source/run's component checkpoints."""
    identity = {
        "version": 1, "source": "shared_prepared_events",
        "topic": args.topic, "bootstrap_servers": args.bootstrap_servers,
        "run_id": args.run_id, "max_offsets_per_trigger": args.max_offsets_per_trigger,
        "feature_shuffle_partitions": args.feature_shuffle_partitions,
        **{name: str(getattr(args, name).resolve()) for name in (
            "account_entity_lookup_path", "static_metadata_dir", "output_dir",
            "rejected_output_dir", "metrics_output_dir",
        )},
    }
    identity_path = args.checkpoint_dir / "shared_run.json"
    if identity_path.exists():
        if read_json(identity_path) != identity:
            raise ValueError("Shared checkpoint identity differs; use the original run settings or a fresh root.")
    else:
        if args.checkpoint_dir.exists() and any(args.checkpoint_dir.iterdir()):
            raise ValueError("Shared pipeline requires its own fresh checkpoint root.")
        if (args.output_dir / f"run_id={args.run_id}").exists():
            raise ValueError("Output run already exists without shared checkpoints; use a fresh run ID.")
        atomic_json(identity_path, identity)


class SharedBatchRunner:
    """Coordinate one durable Kafka batch; a ready manifest is the delivery gate."""

    def __init__(self, spark, args, event_schema, bank_counts, entity_types, after_component=None):
        self.spark, self.args = spark, args
        self.event_schema = event_schema
        self.bank_counts, self.entity_types = bank_counts, entity_types
        self.after_component = after_component
        self.pending_batch_id = None
        self.queries = {}
        self.write_finished = {}
        self.write_slots = BoundedSemaphore(args.concurrent_feature_jobs)
        self.events_root = args.checkpoint_dir / "prepared_events"
        self.events_root.mkdir(parents=True, exist_ok=True)

    def close(self):
        """Stop idle native queries only after the outer query finishes/fails."""
        for query in self.queries.values():
            if query.isActive:
                query.stop()

    def receipt_path(self, batch_id, component):
        return self.args.checkpoint_dir / "receipts" / f"batch_id={batch_id}" / f"{component}.json"

    def batch_directory(self, batch_id):
        return self.args.output_dir / f"run_id={self.args.run_id}" / f"batch_id={batch_id}"

    def component_complete(self, batch_id, component):
        """A write receipt alone is insufficient: state must also be committed."""
        path = self.receipt_path(batch_id, component)
        if not path.is_file():
            return False
        receipt = read_json(path)
        if receipt.get("batch_id") != batch_id or receipt.get("component") != component:
            raise ValueError(f"Invalid component receipt: {path}")
        if component != "relations":
            committed = self.args.checkpoint_dir / component / "commits" / str(receipt["query_batch_id"])
            if not committed.is_file():
                return False
        return all((self.batch_directory(batch_id) / f"{table}.parquet" / "_SUCCESS").is_file()
                   for table in COMPONENT_TABLES[component])

    def run_component(self, component, batch_id):
        if self.component_complete(batch_id, component):
            return
        args = self.args
        if component == "relations":
            events = self.spark.read.schema(self.event_schema).parquet(
                str(self.events_root / f"batch_id={batch_id}")
            )
            with self.write_slots:
                write_account_relation_batch(events, batch_id, args.output_dir, args.run_id)
            receipt = {"batch_id": batch_id, "component": component}
            atomic_json(self.receipt_path(batch_id, component), receipt)
            if self.after_component is not None:
                self.after_component(component, batch_id)
        elif component not in self.queries:
            builders = {
                "account": build_stateful_account_features,
                "edge": build_stateful_account_edges,
                "bank": build_stateful_bank_features,
                "entity": build_stateful_entity_features,
            }
            # A single prepared file is exposed per outer batch. Successful
            # completion of every query is a barrier before exposing the next.
            # Separate SQL settings, shared Spark context and static cache.
            session = self.spark.newSession()
            partitions = args.feature_shuffle_partitions if component in {"account", "edge"} else 1
            session.conf.set("spark.sql.shuffle.partitions", str(partitions))
            session.conf.set("spark.sql.streaming.stateStore.providerClass",
                             "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            session.conf.set("spark.sql.session.timeZone", "UTC")
            session.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
            events = (session.readStream.schema(self.event_schema)
                      .option("recursiveFileLookup", "true")
                      .option("maxFilesPerTrigger", 1)
                      .parquet(str(self.events_root)))
            updates = builders[component](events)
            last_batch = None

            def write_updates(batch_updates, query_batch_id):
                nonlocal last_batch
                current_batch = self.pending_batch_id
                if current_batch is None or (last_batch is not None and current_batch <= last_batch):
                    raise RuntimeError(f"{component}: multiple input batches crossed the delivery barrier.")
                last_batch = current_batch
                common = dict(batch_id=current_batch, output_dir=args.output_dir, run_id=args.run_id)
                # The action is inside this slot: a stateful output is executed
                # once, with no caching or preliminary action on that output.
                with self.write_slots:
                    if component == "account":
                        write_account_feature_batch(batch_updates, **common)
                    elif component == "edge":
                        write_account_edge_batch(batch_updates, **common)
                    elif component == "bank":
                        write_bank_feature_batch(batch_updates, bank_account_counts=self.bank_counts, **common)
                    else:
                        write_entity_feature_batch(batch_updates, entity_types=self.entity_types, **common)
                    atomic_json(self.receipt_path(current_batch, component), {
                        "batch_id": current_batch, "component": component, "query_batch_id": query_batch_id,
                    })
                    if self.after_component is not None:
                        self.after_component(component, current_batch)
                    self.write_finished[(component, current_batch)].set()

            query = (updates.writeStream.outputMode("update").foreachBatch(write_updates)
                     .option("checkpointLocation", str(args.checkpoint_dir / component))
                     .trigger(processingTime="2 seconds").start())
            self.queries[component] = query
        if component != "relations":
            query = self.queries[component]
            deadline = time.monotonic() + args.batch_timeout_seconds
            while True:
                failure = query.exception()
                if failure is not None:
                    raise RuntimeError(f"{component}: native query failed for Kafka batch {batch_id}.") from failure
                if not query.isActive or time.monotonic() >= deadline:
                    raise RuntimeError(f"{component}: no committed output for Kafka batch {batch_id}.")
                # Avoid continuously polling the mounted checkpoint filesystem
                # during the write. The event is driver-local; the checkpoint
                # commit remains the authoritative durable completion check.
                if self.write_finished[(component, batch_id)].wait(0.5):
                    if self.component_complete(batch_id, component):
                        break
                    time.sleep(0.2)

    def __call__(self, batch_events, batch_id):
        """Write audit, drain native state, then atomically publish batch readiness."""
        args = self.args
        completed = args.checkpoint_dir / "completed_batches" / f"batch_id={batch_id}.json"
        if completed.is_file():
            completion = read_json(completed)
            if completion["status"] == "ready" and not (self.batch_directory(batch_id) / "manifest.json").is_file():
                raise RuntimeError(f"Ready manifest disappeared for batch {batch_id}.")
            return
        self.pending_batch_id = batch_id
        self.write_finished = {(component, batch_id): Event() for component in COMPONENT_TABLES}
        started = time.perf_counter()
        metrics_path = metrics_batch_output_path(args.metrics_output_dir, args.run_id, batch_id)
        prepared = self.events_root / f"batch_id={batch_id}"
        if not (prepared / "_SUCCESS").is_file():
            receipts = args.checkpoint_dir / "receipts" / f"batch_id={batch_id}"
            if receipts.exists() and any(receipts.glob("*.json")):
                raise RuntimeError(f"Prepared input disappeared after component writes for batch {batch_id}.")
            batch_events = batch_events.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                # No stateful operator precedes this cache. The audit and staged
                # events share the expensive upstream joins on its cached rows.
                totals = dict(kafka_transactions=0, valid_events=0, rejected_events=0,
                              batches=0, processing_seconds=0.0, validation_passed=True)
                write_audit_batch(batch_events, batch_id, args.run_id, args.rejected_output_dir,
                                  args.metrics_output_dir, totals, release_input=False)
                metrics = read_json(metrics_path)
                if metrics["validation_status"] != "passed":
                    raise ValueError(f"Account-event accounting failed for Kafka batch {batch_id}.")
                if metrics["valid_account_event_count"]:
                    valid, _ = split_valid_and_rejected_events(batch_events)
                    # Exactly one file is the durable boundary of this Kafka
                    # micro-batch; feature grouping still shuffles by its own key.
                    preparing = args.checkpoint_dir / "event_preparation" / f"batch_id={batch_id}"
                    valid.coalesce(1).write.mode("overwrite").parquet(str(preparing))
                    if prepared.exists():
                        raise RuntimeError(f"Incomplete published input for batch {batch_id}; retain it for diagnosis.")
                    # Keep in-progress files outside the watched source. Publish
                    # the complete directory with one same-filesystem rename.
                    preparing.rename(prepared)
            finally:
                batch_events.unpersist()
        else:
            metrics = read_json(metrics_path)
        if metrics["validation_status"] != "passed":
            raise ValueError(f"Audit failed for batch {batch_id}.")
        if metrics["valid_account_event_count"] == 0:
            atomic_json(completed, {"batch_id": batch_id, "status": "skipped_no_valid_events"})
            return
        files = list(prepared.glob("*.parquet"))
        if len(files) != 1:
            raise RuntimeError(f"Expected one prepared file for Kafka batch {batch_id}, found {len(files)}.")
        with ThreadPoolExecutor(max_workers=args.concurrent_feature_jobs) as executor:
            futures = [executor.submit(self.run_component, name, batch_id) for name in COMPONENT_TABLES]
            for future in futures:
                future.result()
        # Every state query has committed this batch. They stay alive and idle
        # until the next prepared file, avoiding query/worker restart per batch.
        # A consumer may now apply this package while the next Kafka batch runs.
        metrics["processing_seconds"] = round(time.perf_counter() - started, 3)
        atomic_json(metrics_path, metrics)
        package_batch(self.batch_directory(batch_id), args.run_id, batch_id)
        atomic_json(completed, {"batch_id": batch_id, "status": "ready"})


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=500)
    parser.add_argument("--run-id", required=True)
    for option in ("account-entity-lookup-path", "static-metadata-dir", "output-dir",
                   "rejected-output-dir", "metrics-output-dir", "checkpoint-dir"):
        parser.add_argument(f"--{option}", type=Path, required=True)
    parser.add_argument("--concurrent-feature-jobs", type=int, choices=range(1, 6), default=2)
    parser.add_argument("--feature-shuffle-partitions", type=int, default=8,
                        help="Account/edge partitions; Bank/Entity retain one local-test partition.")
    parser.add_argument("--batch-timeout-seconds", type=float, default=1800)
    args = parser.parse_args()
    if args.max_offsets_per_trigger <= 0:
        parser.error("--max-offsets-per-trigger must be positive")
    if args.feature_shuffle_partitions <= 0 or args.batch_timeout_seconds <= 0:
        parser.error("Feature partitions and batch timeout must be positive")
    return args


def run_pipeline(args, after_component=None):
    """Run the bounded shared application; the hook supports recovery verification."""
    initialize_run(args)
    spark = build_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.streaming.stateStore.providerClass",
                   "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    spark.sparkContext.setLogLevel("WARN")
    cached = []
    try:
        lookup = load_entity_lookup(spark, args.account_entity_lookup_path)
        cached.append(lookup)
        bank_counts = spark.read.parquet(str(args.static_metadata_dir / "node_bank_features.parquet"))
        bank_counts = bank_counts.select("bank_id", "bank_account_count").persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(bank_counts)
        bank_counts.count()
        entity_types = spark.read.parquet(str(args.static_metadata_dir / "node_entity_features.parquet"))
        entity_types = entity_types.select("entity_id", *ENTITY_TYPE_FEATURE_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(entity_types)
        entity_types.count()
        kafka = (spark.readStream.format("kafka")
                 .option("kafka.bootstrap.servers", args.bootstrap_servers)
                 .option("subscribe", args.topic).option("startingOffsets", "earliest")
                 .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
                 .option("failOnDataLoss", "true").load())
        transactions = parse_transaction_stream(kafka).withColumn("kafka_topic", F.lit(args.topic))
        enriched = enrich_transaction_entities(add_transaction_flags(transactions), lookup)
        events = build_account_events(add_transaction_rejection_reason(enriched))
        valid, _ = split_valid_and_rejected_events(events)
        runner = SharedBatchRunner(spark, args, valid.schema, F.broadcast(bank_counts),
                                   F.broadcast(entity_types), after_component)
        query = (events.writeStream.outputMode("append").foreachBatch(runner)
                 .option("checkpointLocation", str(args.checkpoint_dir / "kafka"))
                 .trigger(availableNow=True).start())
        try:
            query.awaitTermination()
        finally:
            if query.isActive:
                query.stop()
    finally:
        if "runner" in locals():
            runner.close()
        for frame in reversed(cached):
            frame.unpersist()
        spark.stop()
    print(f"Shared replay complete: run_id={args.run_id} output={args.output_dir / f'run_id={args.run_id}'}")


def main():
    run_pipeline(parse_args())


if __name__ == "__main__":
    main()
