# Native stateful replay performance

## Scope and invariants

The November 2, 2026 refactor preserves all 24 Account, 15 Bank, 15 Entity,
and 9 directed-edge features, both mapping tables, exact distinct counts,
cumulative history, transaction-atomic validation, and the six-table output
contract. State variable names, field order, and Spark state/output schemas
match baseline commit `1e22ebf5e860cee8f58408bd25dbfb188f4d4e63`.

This phase does not introduce approximate features, trim history, migrate to
MapState, change the two-Account-event representation, or cache stateful output.

## Work removed

- Reuse a single incoming pandas frame rather than copying it through concat.
- Read timestamp bounds without sorting an entire event DataFrame.
- Keep the original floating-point reduction order by sorting just amount-vector
  positions with a stable NumPy ordering. This avoids full-frame sorts while
  preserving the old totals and squared-amount sums.
- Calculate consecutive gap contributions from first/last timestamps and the
  previous boundary. Adjacent differences telescope; repeated timestamps still
  contribute zero-length gaps and increment the gap count.
- Count hourly activity in bulk for larger groups, retaining a cheaper path for
  groups of up to four events.
- Sum already numeric flags directly, without repeated coercion and null-filled
  copies. Object-valued inputs retain the previous null/coercion behavior.
- Deduplicate and materialize the fixed Account-to-Entity lookup once per Spark
  application using MEMORY_AND_DISK. Both stream-static joins reuse this lookup.
  Query completion/failure releases the cached lookup. Automatic broadcasts
  remain disabled; no new explicit broadcast hint is added.

The proposed single `ValueState.get()` read was tested and discarded: Spark
logged a warning for every new key and real replay timings regressed. The final
code retains the original `exists()`/`get()` pattern.

## Phase 1 measurements

The callback benchmark runs old and current callbacks in the same process,
alternates timing order, and reports medians of five rounds of 200 updates.
It uses in-memory fake state. It therefore excludes Spark scheduling, Python/JVM
state-store communication, shuffles, checkpoint IO, and output writes.

| Processor | 1 row/key | 4 rows/key | 128 rows/key |
| --- | ---: | ---: | ---: |
| Account | 1.51x | 1.50x | 3.23x |
| Edge | 1.88x | 1.53x | 2.03x |
| Bank | 2.29x | 1.82x | 2.19x |
| Entity | 2.75x | 1.33x | 1.92x |

These are measured callback speed ratios, not end-to-end throughput claims.
The benchmark also compares outputs and saved-state values across singleton,
unsorted, multi-frame, and larger batches, restores state between updates, and
checks late-event rejection and schema equality.

Two final Account Spark trials used identical 1,000-transaction Kafka input,
separate fresh checkpoints, container-local test storage, and opposite run
orders. The baseline used the old processor and uncached lookup; the new version
used the refactored processor and the materialized static lookup. Both trials
compared their actual Parquet outputs within an absolute tolerance of `1e-6`.

| Trial | Metric | Before | After |
| --- | --- | ---: | ---: |
| Pair 4, new first | First trigger | 31.313 s | 20.261 s |
| Pair 4, new first | Second trigger | 15.325 s | 7.133 s |
| Pair 4, new first | Whole application | 70.736 s | 64.888 s |
| Pair 5, old first | First trigger | 26.250 s | 18.113 s |
| Pair 5, old first | Second trigger | 9.890 s | 8.021 s |
| Pair 5, old first | Whole application | 52.167 s | 54.745 s |

Trigger times improved in both final trials. Whole-application time for only two
batches was mixed because startup and one-time lookup materialization are a
substantial fixed cost. These results do not establish a universal speedup or
a production latency guarantee. Earlier exploratory pairs 1-3 were not the
final implementation and must not be substituted for these final trials.

The complete 5,000-transaction replay also passed all sixty batch/table
comparisons, with zero rejections and ten ready packages. The runs below used
the same prepared input and 500 transactions per batch, but occurred on different
days; their timings are historical observations, not controlled trial results.

| Stage | November 1 run | November 2 refactored run |
| --- | ---: | ---: |
| Five output tasks, summed | about 30.7 min | about 16.7 min |
| Independent recomputation/comparison | about 12.4 min | about 6.0 min |
| Complete Airflow workflow | about 46.2 min | about 25.5 min |

The measured Airflow workflows above ran jobs sequentially. The DAG now makes
the five output jobs independent branches after publication, capped at two
active tasks by default. `MULEDETECTOR_MAX_ACTIVE_TASKS` accepts 1 through 5
in the Airflow DAG-processor environment. A limit of 1 restores sequential
scheduling; 5 allows all five output jobs to overlap if executor resources permit.
The jobs retain independent checkpoints and separate table paths. Audit and
packaging wait for all five jobs to succeed, so this change alone does not
provide per-batch model delivery while the replay is running. Concurrent jobs
compete for CPU, memory, and disk; the sequential timing table is not a prediction
of their runtime.

The bounded-concurrency acceptance run `manual__concurrent_1000_20261102`
used two active task slots and produced project run
`airflow_replay_20261102T073632`. Bank and Entity started together; later output
tasks overlapped as slots became available. All five output tasks passed, followed
by audit, packaging, and all twelve independent table comparisons across two
500-transaction batches. There were 2,000 valid Account events, zero rejections,
and two ready packages. The output stage spanned about 6.5 minutes. There is no
matching sequential trial under the same conditions, so this verifies correctness
and overlap, not a measured concurrency speedup. The report is
`data/streaming/reports/airflow_replay_20261102T073632.json`. Ten DAG tests passed,
including branch/barrier dependencies and valid/invalid task limits.

This remains a bounded replay workflow, not a low-latency inference service. The final report's
`processing_seconds` measures audit batch processing only; it is not the full
workflow time. Raw CSV preparation is separate from these replay timings.

Checkpoint recovery also passed for Account, Edge, Bank, Entity, and relations.
The failure workers used the baseline processors, committed batch 0, and failed
after writing batch 1 but before its commit. The current code resumed those
checkpoints, recovered batch 1, and left output files unchanged on a subsequent
restart without new input. Independent feature comparisons passed for both
recovered batches. This verifies compatibility for the exercised state and
failure scenario; it does not establish recovery behavior for every failure mode.

## Local evidence and reproduction

Generated evidence is intentionally excluded from Git:

- `data/streaming/reports/processor_refactor_20261102_container_callbacks.json`
- `data/streaming/reports/processor_refactor_20261102_spark_pair4/summary.json`
- `data/streaming/reports/processor_refactor_20261102_spark_pair5/summary.json`
- `data/streaming/reports/airflow_replay_20261102T063449.json`
- `data/streaming/reports/processor_refactor_recovery_20261102.json`
- Airflow run `manual__processor_refactor_5000_20261102`, including all sixty
  comparison PASS records in the `validate_replay` task log.

Focused regression tests:

```text
python -m pytest streaming/state/test_processor_batch.py streaming/ingestion/test_entity_lookup_lifecycle.py -q
```

Run callback equivalence/timings on a host with Git:

```text
python -m streaming.operations.benchmark_stateful_processors --baseline-ref 1e22ebf --report-path data/streaming/reports/processor_callbacks.json
```

For a container without Git, export the baseline first with `--export-baseline`
and pass the resulting JSON using `--baseline-bundle`. The `_compare_spark`
entry point runs sequential Account Spark trials; it requires a fresh test
topic, run ID, report directory, and checkpoint/output working directory.

The shared runner described in [SHARED_PIPELINE.md](SHARED_PIPELINE.md) now
handles ingestion/enrichment reuse and per-batch delivery in one Spark
application. It keeps native queries alive between batches and bounds simultaneous
output actions. Fixed lookup partitioning/ordering is prepared once; an opt-in
Spark plan test verifies that both cached lookup sides avoid per-query sorts.
Only stateless input rows and static metadata are cached. The existing native
state schemas and finalizers are retained. Its file-source checkpoints need a
fresh root rather than reuse of the standalone Kafka-source checkpoints.

The shared 1,000-transaction Airflow run `airflow_replay_20261102T101747` passed
all twelve table comparisons. Its combined output/audit/package task ran from
10:20:32 to 10:27:00 UTC (about 6.5 minutes); batch 0 was ready at 10:25:02 while
batch 1 was still running. The earlier standalone concurrent run spent about
7.9 minutes on its output/audit/package stages. These are same-input local
observations, not controlled throughput guarantees. This shared trial preceded
the final explicit ordering/partitioning of the static lookup cache.

`shared_recovery_20261102_v1` passed failure-after-write/before-commit recovery,
withheld the incomplete batch's manifest, preserved the already-ready batch, and
left package files unchanged on a restart with no new input. Both recovered
batches passed all twelve independent table comparisons. Evidence is in
`data/streaming/reports/shared_recovery_20261102_v1.json` and companion logs.

The three-transaction `shared_mixed_20261102` test placed a malformed transaction
between two valid ones. It produced four valid Account events, two rejections,
two ready packages, and one explicit skip. All eighteen batch/table checks passed,
including independently verifying that the rejected batch had no valid events
and no package. The feed returned `ready`, `skipped_no_valid_events`, `ready` in
order. The report passed with rejections and failed under the strict rejection
policy. This test also corrected the replay validator's nullable-input schema and
header handling, so malformed rows retain their original offsets for validation.

Airflow retains the legacy branch via `pipeline_mode: legacy`. Normal delivery
can set `verify_features: false`; audit, packaging, and reporting still run.
Acceptance runs keep independent recomputation enabled.

The final shared 5,000-transaction run `airflow_replay_20261102T104709` passed
all sixty independent comparisons: 10,000 valid Account events, zero rejections,
and ten ready packages. The model feed also returned all ten packages in order.
The output/audit/package task ran from 10:47:44 to 11:01:39 UTC, about 13.9 minutes,
versus about 19.1 minutes for those stages in the earlier sequential 5,000-row run.
Batch 0 was ready at 10:51:25, about 3.7 minutes after the shared task started;
later batches were delivered before the run ended. The complete acceptance
workflow, including about eight minutes of independent comparison, took about
22.5 minutes. These are historical same-input observations, not a controlled
hardware-independent speed guarantee. Evidence is in
`data/streaming/reports/airflow_replay_20261102T104709.json` and Airflow run
`manual__shared_5000_20261102_final`. Shared processing is now the default; the
legacy branch and all prior checkpoints remain available.

Entry-based collection state requires a
separate checkpoint migration design. JVM state-update code should be evaluated
only with measurements showing the remaining Python/state-store boundary cost.
