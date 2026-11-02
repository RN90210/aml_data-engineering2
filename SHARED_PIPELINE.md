# Shared ingestion and per-batch model delivery

The shared runner uses one Spark application. For each Kafka micro-batch it:

1. Parses, flags, enriches both transaction sides, and validates once.
2. Caches only those stateless Account-event rows while writing audit data and
   preparing valid events. The fixed static lookups are loaded once per application.
   The Account-to-Entity lookup is explicitly partitioned and sorted by Account ID
   before caching, so matching sort-merge joins reuse its fixed ordering. An opt-in
   Spark plan test verifies that neither lookup side is sorted again per query.
3. Writes one bounded Parquet event file outside the watched source directory,
   then publishes the completed directory by a same-filesystem rename.
4. Runs the existing Account, edge, Bank, and Entity native processors against
   this shared input. They remain separate keyed RocksDB states. Queries stay
   alive between batches; their final Parquet writes are their only output actions.
   Stateless mappings use the same prepared events.
5. Waits for each state checkpoint commit and all six successful table writes,
   validates their schemas using Parquet metadata, then atomically publishes
   `manifest.json`. Only then does it prepare the next Kafka batch.

The event-file barrier preserves the original Kafka micro-batch boundary and
batch-local temporal features. A model can consume batch 0 while batch 1 is
running. It does not have to wait for the independent feature comparator or final
run report. The six-table contract, exact feature builders, and cumulative feature
meaning are unchanged; no feature or distinct-history state is removed.

## Running

The standalone shared command performs audit and packaging itself; it does not
start the expensive independent recomputation check:

```text
python -m streaming.ingestion.shared_stateful_streaming_pipeline --topic YOUR_TOPIC --run-id YOUR_RUN --account-entity-lookup-path data/processed/account_entity_lookup.parquet --static-metadata-dir data/processed/spark_static_full --output-dir data/streaming/model_updates --rejected-output-dir data/streaming/audit/rejected_account_events --metrics-output-dir data/streaming/audit/metrics --checkpoint-dir data/checkpoints/YOUR_SHARED_RUN --bootstrap-servers localhost:9092 --max-offsets-per-trigger 500
```

This is still an available-now replay runner. It processes Kafka data available
when the outer query starts, then stops. Continuously arriving production traffic
needs a deployment with an appropriate outer trigger and service supervision.

`--concurrent-feature-jobs` defaults to 2 and bounds simultaneous output actions
within the one application. `--feature-shuffle-partitions` defaults to 8 for
Account/edge; Bank/Entity retain one partition. The shared preparation session
retains its own eight shuffle partitions. Each feature session explicitly sets
RocksDB, UTC, and disabled automatic broadcasting. Only small Bank/Entity static
feature lookups retain their existing explicit broadcast joins.

Airflow defaults to shared processing and accepts `pipeline_mode: shared` or `pipeline_mode: legacy` in run
configuration. The shared branch performs audit and packaging during the stream;
the legacy branch retains the five standalone jobs and subsequent audit/package
steps. Both branches retain independent verification and the final report for
acceptance testing by default. Set `verify_features: false` in Airflow run
configuration to omit independent recomputation on normal delivery runs; audit,
per-batch packaging, and the final report still run.
`concurrent_feature_jobs` controls the shared runner's action
limit separately from Airflow's active-task limit.

## Checkpoints and retries

Start with a fresh checkpoint root when switching from the standalone pipelines.
The shared native queries read Parquet instead of Kafka, so existing Kafka-source
checkpoints must not be reused. The old pipelines and checkpoints remain intact.

Keep `prepared_events`, component checkpoints, write receipts, completion records,
and the outer Kafka checkpoint together. The saved run identity rejects changes
to source/run paths, batch size, or feature partition settings on restart. Do not
launch two drivers against the same shared checkpoint root.

A component receipt is written after its Parquet action, but the coordinator
requires the matching Spark commit before accepting it. On partial failure,
committed components are reused; an uncommitted component replays its original
event file and overwrites its output deterministically. The next batch cannot
enter state until this batch is complete. Already-ready batches are retained.
Rejected-only batches produce audit records and explicit skip entries in the
model feed and final report, without feature-state updates or a ready manifest.

The coordinator uses filesystem paths and same-filesystem renames. Its staging,
checkpoint, and output paths must be mounted consistently for the driver and
Spark executors; an object-store deployment requires a compatible publication
protocol rather than substituting an S3 URI into `pathlib` paths.

## Model integration

`streaming.operations.model_update_feed.ready_batches` uses only the Python
standard library. It yields consecutive batches after a model-owned cursor,
stopping at the first unfinished batch. It checks the audit identity/counts/status,
manifest readiness, all six tables, and successful write markers. Returned items
include table paths, counts, feature column lists, and the batch audit. A rejected-
only batch is returned as `skipped_no_valid_events`, so it does not look like a
missing batch.

Use the feed from the model's consumer:

```python
from pathlib import Path
from streaming.operations.model_update_feed import ready_batches

# Model-side functions below must be supplied by the model integration.
cursor = load_last_applied_batch(run_id)  # -1 before the first batch
for package in ready_batches(Path(output_root), Path(metrics_root), run_id, cursor):
    if package["status"] == "ready":
        apply_graph_updates_and_save_cursor(package)
    else:
        save_skipped_batch_cursor(package)
```

The model must read its expected schemas, upsert cumulative rows by the keys in
MODEL_HANDOFF.md, and durably record `(run_id, batch_id)` with the graph update.
Advance the cursor only after successful application. Repeating a feed call with
an old cursor intentionally returns the same package again. The feed does not
train, score, mutate the graph, or claim exactly-once model-side execution.
Apply the desired rejection policy using the returned audit record. A final run
report is a retrospective summary; offline training/evaluation may additionally
wait for the report and independent comparison before accepting the full run.

For inspection only, the CLI prints one JSON item per completed batch:

```text
python -m streaming.operations.model_update_feed --output-root data/streaming/model_updates --metrics-root data/streaming/audit/metrics --run-id YOUR_RUN --follow
```

Its cursor is in memory. Restart with `--after-batch-id` to inspect later batches;
the model integration should use its own durable cursor rather than treating CLI
stdout delivery as acknowledgement that a model update succeeded.
