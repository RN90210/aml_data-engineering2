# MuleDetector — Static and Streaming Data Engineering Pipeline

This repository contains the Data Engineering pipeline for MuleDetector, a graph-based AML money-mule detection project built on the IBM HI-Medium synthetic transaction dataset.

## Architecture

```text
Historical transactions before a cutoff
→ PySpark static pipeline
→ static graph baseline

Transactions after the cutoff
→ Kafka topic
→ Spark Structured Streaming micro-batches
→ validated Account events
→ baseline + streamed event history
→ current graph snapshot for model inference
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
* The combined event history is materialized into a complete current graph snapshot.

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

The final verified demonstration package is a `current_graph_snapshot`: it combines the static baseline before `2022/09/01 00:30` with 1,000 chronologically replayed post-cutoff transactions.

See `FEATURE_DESIGN.md` for the fixed feature definitions and ordering contract.

## Key modules

```text
preprocessing/spark_static_pipeline.py
    Builds static graph outputs and the historical baseline.

preprocessing/build_account_metadata_lookup.py
    Builds the stable Account → Bank/Entity metadata lookup.

streaming/prepare_replay_window.py
    Creates a globally time-ordered post-cutoff replay CSV.

streaming/kafka_producer.py
    Publishes replay transactions as JSON messages to Kafka.

streaming/spark_streaming_pipeline.py
    Consumes Kafka events, validates them, joins metadata, and writes Account events.

streaming/build_current_graph_snapshot.py
    Combines static baseline Account events with post-cutoff streamed events.

streaming/materialize_*.py
    Recomputes the exact Account, Bank, Entity, and transaction-edge feature contracts.

streaming/build_graph_delta.py
    Validates and packages either a replay delta or a full current graph snapshot.
```

## Data policy

Raw CSV files, generated Parquet outputs, Kafka/Spark checkpoints, and PyTorch artifacts are intentionally excluded from Git. They are generated locally or shared separately when needed.
