# MuleDetector — Static and Streaming Data Engineering Pipeline

This repository contains the Data Engineering pipeline for MuleDetector, a graph-based AML money-mule detection project built on the IBM HI-Medium synthetic transaction dataset.

## Architecture

```text
Historical transactions before a cutoff
→ PySpark static pipeline
→ static graph baseline

Transactions after the cutoff
→ Kafka topic
→ Spark Structured Streaming micro-batches with native RocksDB state
→ affected Account, Bank, Entity, and directed edge feature updates
→ six-table incremental model package and audit report
→ model-side graph upserts and inference
```

The model-training and inference implementation is owned separately. This repository produces the validated graph-ready data that the RGCN/GAT model consumes.

## Static pipeline

The static PySpark pipeline:

* Reads the transaction and Account metadata CSV files.
* Enriches every transaction with sender and receiver Account, Bank, and Entity metadata.
* Produces 24 Account features, 15 Bank features, 15 Entity features, and 9 Account-to-Account edge features.
* Creates Account-to-Bank and Entity-to-Account relations.
* Creates offline Account labels from `Is Laundering`; this field is never used as a model input feature.

Run a bounded validation sample:

```powershell
python -m preprocessing.spark_static_pipeline --max-rows 100000
```

Run the complete historical dataset:

```powershell
python -m preprocessing.spark_static_pipeline --full
```

## Time-separated streaming replay

The final demonstration uses a fixed event-time cutoff:

* Transactions before the cutoff form the static baseline.
* Transactions after the cutoff are replayed through Kafka.
* Spark consumes the Kafka events in micro-batches and creates two Account events per transaction: one outgoing and one incoming.
* Native stateful processors maintain cumulative features and write only rows affected by each batch.
* An independent audit, package validator, replay comparator, and run report check the completed updates.
* The Airflow DAG runs a 1,000-transaction demonstration in two batches of 500 transactions.
* An Airflow run can reuse an existing prepared replay CSV through `dag_run.conf.prepared_replay_path`; otherwise it scans the raw CSV to prepare a new window.
* Replay preparation uses a single CSV scan and retains only the requested earliest rows in memory. It does not start Spark. OneDrive source files must be downloaded locally first.
* Airflow run configuration can set `replay_row_count` and `max_offsets_per_trigger` (defaults: 1,000 and 500). A prepared CSV must contain exactly `replay_row_count` data rows.

On Windows, set the local Hadoop utilities for each new PowerShell terminal:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
$env:JAVA_TOOL_OPTIONS = "-Djava.library.path=C:\hadoop\bin"
```

The Kafka broker runs locally in Docker on `localhost:9092`.

## Final graph data contract

A graph snapshot contains:

```text
node_account_features.parquet
node_bank_features.parquet
node_entity_features.parquet
edge_account_to_account.parquet
edge_account_to_bank.parquet
edge_entity_to_account.parquet
manifest.json
```

The older `current_graph_snapshot` package remains available. The native workflow now writes incremental model-update packages; see [MODEL_HANDOFF.md](MODEL_HANDOFF.md) for their paths, keys, and application rules.

See `FEATURE_DESIGN.md` for the fixed feature definitions and ordering contract.

## Key modules

- `preprocessing/spark_static_pipeline.py` builds the historical graph baseline and defines the feature contracts.
- `streaming/operations/prepare_replay_window.py` creates the ordered replay CSV.
- `streaming/kafka/kafka_producer.py` publishes transactions to Kafka.
- `streaming/ingestion/stateful_*_streaming_pipeline.py` writes native Account, edge, Bank, and Entity updates.
- `streaming/ingestion/account_relation_streaming_pipeline.py` writes the two mapping tables.
- `streaming/ingestion/streaming_audit_pipeline.py` audits the same Kafka replay independently.
- `streaming/operations/package_incremental_model_updates.py`, `validate_stateful_replay.py`, and `write_stateful_run_report.py` validate and report completed batches.
- `orchestration/airflow/dags/muledetector_replay.py` runs the replay stages in order.

## Data policy

Raw CSV files, generated Parquet outputs, Kafka/Spark checkpoints, and PyTorch artifacts are intentionally excluded from Git. They are generated locally or shared separately when needed.

## Local recovery verification

`streaming.operations.verify_stateful_recovery` is an opt-in integration check for a running Kafka/Spark environment. Use a fresh topic, run ID, checkpoint root, output namespace, and report path with the two-row `stateful_carry_test_2.csv` fixture. It executes one Spark job at a time for Accounts, edges, Banks, Entities, and mappings. For each component it fails batch 1 after the Parquet write but before Spark's commit, verifies batch 0 was committed, restarts with the same checkpoint, and verifies a further restart with no new messages leaves Parquet bytes unchanged. It then compares both recovered batches against the existing incremental feature builders. Logs and a JSON report are retained; checkpoints are never removed by this utility.

The local recovery run `laptop_recovery_20261101` passed for all five components. The larger Airflow run `airflow_replay_20261101T075252` also passed: 5,000 transactions in ten batches, zero rejected events, ten ready model packages, and all sixty independent table comparisons passed. The full 3 GB local CSV preparation took 308.99 seconds with the bounded scanner. See [MODEL_HANDOFF.md](MODEL_HANDOFF.md) for evidence paths and the model interface.
