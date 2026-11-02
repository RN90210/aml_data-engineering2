# MuleDetector data-to-model handoff

The data pipeline supplies graph features for model training and inference. It does not train or serve the model.

## Replay size and timing

The current Airflow demonstration publishes 1,000 post-cutoff transactions to a run-specific Kafka topic. Each transaction produces one outgoing and one incoming Account event. The default shared application reads Kafka once with `maxOffsetsPerTrigger=500`, prepares the events once, and feeds the native processors from durable prepared batches. The clean demonstration produces two batches of 500 transactions and 1,000 Account events each. The legacy standalone pipelines remain available via `pipeline_mode: legacy`. These are demonstration limits, not a production throughput guarantee. Invalid transactions contribute two rejected Account events and no feature updates.

The historical static baseline uses transactions before `2022/09/01 00:30`. The replay window starts at that timestamp. Training and evaluation must preserve this time boundary: features available at a prediction time cannot include later transactions.

## Incremental output contract

Each run writes six Parquet tables under `data/streaming/model_updates/run_id=<run_id>/batch_id=<batch_id>/`:

| Table | Upsert key | Contents |
| --- | --- | --- |
| `account_features.parquet` | `account_id` | 24 cumulative Account features |
| `bank_features.parquet` | `bank_id` | 15 Bank features, including static account count |
| `entity_features.parquet` | `entity_id` | 15 Entity features, including static type indicators |
| `account_to_account_edges.parquet` | `sender_account_id`, `receiver_account_id` | 9 cumulative directed edge features |
| `account_to_bank.parquet` | `account_id`, `bank_id` | Stable Account-to-Bank mappings seen in this batch |
| `entity_to_account.parquet` | `entity_id`, `account_id` | Stable Entity-to-Account mappings seen in this batch |

The exact feature names and column order are defined by `ACCOUNT_FEATURE_COLUMNS`, `BANK_FEATURE_COLUMNS`, `ENTITY_FEATURE_COLUMNS`, and `ACCOUNT_TO_ACCOUNT_EDGE_FEATURE_COLUMNS` in `preprocessing/spark_static_pipeline.py`. Model code should validate the table schemas against the batch `manifest.json` before applying updates.

Each batch contains only affected nodes, edges, and mappings. Feature values on those rows include valid transactions from the beginning of this replay through that batch. Apply batches in increasing `batch_id` order, upserting by the keys above. Do not replace the whole graph with one batch's rows, or sum feature rows across batches. Temporal features are calculated as of each batch. Start from the static graph baseline if inference requires historical context; the incremental packages alone are not a complete graph snapshot.

`manifest.json` marks a completed six-table batch as `ready` and records its row counts and columns. The audit writes per-batch Kafka offset ranges, accepted/rejected event counts, and validation status. Live batch consumers may apply a ready package after its matching audit passes their rejection policy; they do not need to wait for the final report. The run report summarizes the completed run. Offline training/evaluation can additionally require that final report and independent feature comparison before accepting the entire run.

The shared ingestion runner prepares each Kafka batch once in one Spark application
and publishes its manifest before preparing the next batch. The legacy standalone
pipelines remain available. See [SHARED_PIPELINE.md](SHARED_PIPELINE.md) for the
shared command, checkpoint requirements, and `model_update_feed.ready_batches`
consumer interface. The feed returns package paths in batch order and explicitly
identifies rejected-only batches. The model owns its durable last-applied cursor;
feature rows must be upserted, never added to previous feature values.

## Model-side decisions to agree on

- Which Account(s) receive a score after each batch: every affected Account or the whole graph.
- Whether inference is triggered per batch or on a separate schedule.
- The label source and prediction horizon for training. `Is Laundering` is for offline labels only and is not an input feature.
- Where the model keeps its current graph and how it records the last applied `(run_id, batch_id)` so retries are idempotent.
- Expected production arrival rate, latency target, and retention period. The 1,000-transaction demonstration does not establish these values.

The saved native 1,000-transaction, two-batch replay passed independent comparison of all six output tables against the incremental recomputation path with a `1e-6` absolute tolerance. The native Airflow workflow also completed successfully in the local containers as run `airflow_replay_20261101T053727`: 1,000 Kafka transactions, 2,000 valid Account events, zero rejections, two ready model packages, and all twelve batch/table comparisons passed. Its run report is `data/streaming/reports/airflow_replay_20261101T053727.json`.

For that container verification, Airflow used the optional `prepared_replay_path` run configuration to copy an already prepared 1,000-row replay CSV. Without that option, the DAG prepares a fresh replay from the 3 GB raw CSV. Preparation now uses one bounded-memory CSV scan, retaining only the requested earliest rows. The original source was a OneDrive offline placeholder; it must be fully downloaded before either host or Docker preparation can read it. Airflow can also set `replay_row_count` and `max_offsets_per_trigger` in manual run configuration.

Checkpoint recovery was verified for Account, edge, Bank, Entity, and mapping pipelines in `laptop_recovery_20261101`. Each writer deliberately failed after writing batch 1 and before Spark committed it, resumed using the same checkpoint, and then restarted with no new messages without changing Parquet files. Both recovered batches matched all six expected tables. Evidence is in `data/streaming/reports/laptop_recovery_20261101.json` and its companion log directory. This covers controlled task failure and replay of an uncommitted batch; it is not a power-loss or storage-corruption test.

Production deployment, model consumption, and a realistic throughput target remain separate integration work.

The larger replay also passed as `airflow_replay_20261101T075252`: 5,000 real source transactions, 10,000 valid Account events, zero rejections, ten 500-transaction batches, and ten ready model packages. All sixty batch/table comparisons passed at the existing `1e-6` tolerance. The downloaded local source was `C:\Users\ruhan\Downloads\HI-Medium_Trans (1).csv`; a full bounded-memory scan prepared the 5,000-row replay in 308.99 seconds. Airflow then used that prepared CSV for the sequential native pipeline run. Evidence is in `data/streaming/reports/airflow_replay_20261101T075252.json` and the `manual__laptop_5000_20261101` Airflow task logs. The report's `processing_seconds` measures audit-batch processing only, not the whole Airflow workflow or raw-file preparation.
