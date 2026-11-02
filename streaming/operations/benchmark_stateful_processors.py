"""Compare processor outputs/state and callback CPU time against a Git revision.

No Spark session is started. This isolates Python callback work; it does not
measure shuffles, state-store RPCs, checkpoint IO, or end-to-end batch latency.
"""

from __future__ import annotations

import argparse
import copy
import importlib
import hashlib
import json
import math
import os
from pathlib import Path
import pickle
import statistics
import subprocess
import sys
import time
import types

import numpy as np
import pandas as pd


PROCESSORS = {
    "account": ("AccountRunningTotalsProcessor", "ACCOUNT_FEATURE", ("1::A",)),
    "edge": ("EdgeRunningTotalsProcessor", "EDGE_FEATURE", ("1::A", "2::B")),
    "bank": ("BankRunningTotalsProcessor", "BANK_FEATURE", (1,)),
    "entity": ("EntityRunningTotalsProcessor", "ENTITY_FEATURE", ("E1",)),
}


class MemoryValueState:
    """Model tuple persistence; pickle round trips model process restoration."""

    def __init__(self):
        self.value = None
        self.get_calls = 0
        self.exists_calls = 0

    def exists(self):
        self.exists_calls += 1
        return self.value is not None

    def get(self):
        self.get_calls += 1
        return copy.deepcopy(self.value)

    def update(self, value):
        self.value = copy.deepcopy(value)


class MemoryHandle:
    def __init__(self):
        self.state = MemoryValueState()
        self.registration = None

    def getValueState(self, name, schema):
        self.registration = (name, schema.json())
        return self.state


def make_events(count, seed, batch=0):
    """Unsorted events with ties, hour crossings, amounts, and exact distinct IDs."""
    rng = np.random.default_rng(seed)
    times = pd.Timestamp("2022-09-01") + pd.to_timedelta(
        batch * 86400000000 + np.sort(rng.integers(0, 7200000000, count)), unit="us"
    )
    if count > 2:
        times = times.to_numpy(copy=True)
        times[1] = times[0]
    frame = pd.DataFrame({
        "event_time": times,
        "direction": rng.choice(["incoming", "outgoing"], count),
        "amount": np.round(rng.uniform(0, 100000, count), 2),
        "account_id": [f"1::A{i % 9}" for i in range(count)],
        "participant_account_id": [f"1::A{i % 9}" for i in range(count)],
        "counterparty_account_id": [f"2::B{i % 11}" for i in range(count)],
        "bank_id": rng.integers(1, 4, count),
        "counterparty_bank_id": rng.integers(1, 7, count),
        "currency": rng.choice(["USD", "EUR", "INR"], count),
        "payment_format": rng.choice(["Wire", "ACH", "Cash"], count),
        "is_self_transfer": rng.integers(0, 2, count),
        "is_currency_switch": rng.integers(0, 2, count),
        "is_cross_bank": rng.integers(0, 2, count),
        "is_odd_hour": rng.integers(0, 2, count),
        "is_weekend": rng.integers(0, 2, count),
    })
    return frame.sample(frac=1, random_state=seed)


def assert_state_equal(left, right):
    if isinstance(left, (float, np.floating)):
        assert math.isclose(left, right, rel_tol=0, abs_tol=1e-6), (left, right)
    elif isinstance(left, dict):
        assert left.keys() == right.keys()
        for key in left:
            assert_state_equal(left[key], right[key])
    elif isinstance(left, (tuple, list)):
        assert len(left) == len(right)
        for first, second in zip(left, right):
            assert_state_equal(first, second)
    else:
        assert left == right, (left, right)


def initialized(module, class_name):
    handle = MemoryHandle()
    processor = getattr(module, class_name)()
    processor.init(handle)
    return processor, handle


def check_equivalence(old, new, spec):
    """Check unsorted chunks, singleton keys, state restoration, and late rejection."""
    class_name, prefix, key = spec
    for suffix in ("STATE_SCHEMA", "OUTPUT_SCHEMA"):
        assert getattr(old, f"{prefix}_{suffix}").json() == getattr(new, f"{prefix}_{suffix}").json()
    checked = 0
    for sizes in ((1, 1, 1), (2, 5, 9), (128, 257, 64)):
        first, first_handle = initialized(old, class_name)
        second, second_handle = initialized(new, class_name)
        assert first_handle.registration == second_handle.registration
        assert list(first.handleInputRows(key, iter([]), None)) == []
        assert list(second.handleInputRows(key, iter([]), None)) == []
        for batch, size in enumerate(sizes):
            events = make_events(size, 37 + batch, batch)
            chunks = [events] if batch == 0 else [
                events.iloc[:max(1, size // 2)], events.iloc[max(1, size // 2):]
            ]
            chunks = [chunk for chunk in chunks if not chunk.empty]
            before = events.copy(deep=True)
            expected = list(first.handleInputRows(key, iter(chunks), None))
            actual = list(second.handleInputRows(key, iter(chunks), None))
            assert len(expected) == len(actual) == 1
            pd.testing.assert_frame_equal(expected[0], actual[0], check_exact=False, rtol=0, atol=1e-6)
            pd.testing.assert_frame_equal(events, before)
            assert_state_equal(first_handle.state.value, second_handle.state.value)
            # Restart callbacks with their existing tuple state, not empty state.
            for module, handle, which in ((old, first_handle, 0), (new, second_handle, 1)):
                restored, restored_handle = initialized(module, class_name)
                restored_handle.state.value = pickle.loads(pickle.dumps(handle.state.value))
                if which == 0:
                    first, first_handle = restored, restored_handle
                else:
                    second, second_handle = restored, restored_handle
            checked += 1
        late = make_events(1, 3, batch=0)
        for processor, handle in ((first, first_handle), (second, second_handle)):
            before_state = copy.deepcopy(handle.state.value)
            try:
                list(processor.handleInputRows(key, iter([late]), None))
            except ValueError as error:
                assert "chronological" in str(error)
            else:
                raise AssertionError("Late input was not rejected")
            assert_state_equal(before_state, handle.state.value)
    return checked


def callback_seconds(module, spec, events, iterations):
    processor, _ = initialized(module, spec[0])
    started = time.perf_counter()
    for _ in range(iterations):
        list(processor.handleInputRows(spec[2], iter([events]), None))
    return time.perf_counter() - started


def baseline_sources(root, revision=None, bundle=None):
    """Export Git sources on the host; containers need not install Git."""
    if bundle is not None:
        return json.loads(bundle.read_text(encoding="utf-8"))
    commit = subprocess.check_output([
        "git", "-c", f"safe.directory={root}", "rev-parse", "--verify", f"{revision}^{{commit}}",
    ], cwd=root, text=True).strip()
    sources = {}
    for name in PROCESSORS:
        path = f"streaming/state/stateful_{name}_processor.py"
        sources[name] = subprocess.check_output([
            "git", "-c", f"safe.directory={root}", "show", f"{commit}:{path}",
        ], cwd=root, text=True)
    return {"baseline_commit": commit, "sources": sources}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    baseline = parser.add_mutually_exclusive_group(required=True)
    baseline.add_argument("--baseline-ref")
    baseline.add_argument("--baseline-bundle", type=Path)
    parser.add_argument("--export-baseline", type=Path)
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if args.iterations <= 0 or args.rounds <= 0:
        parser.error("iterations and rounds must be positive")
    root = Path(__file__).resolve().parents[2]
    bundle = baseline_sources(root, args.baseline_ref, args.baseline_bundle)
    revision = bundle["baseline_commit"]
    if args.export_baseline is not None:
        args.export_baseline.parent.mkdir(parents=True, exist_ok=True)
        args.export_baseline.write_text(json.dumps(bundle), encoding="utf-8")
        print(f"Exported baseline {revision} to {args.export_baseline}")
        return
    if args.report_path is None:
        parser.error("--report-path is required for benchmarks")
    report = {"kind": "stateful_processor_callback_benchmark", "baseline_commit": revision,
              "scope": "Python callbacks with in-memory state; not end-to-end Spark latency",
              "iterations": args.iterations, "rounds": args.rounds, "processors": {}}
    for name, spec in PROCESSORS.items():
        path = f"streaming/state/stateful_{name}_processor.py"
        source = bundle["sources"][name]
        baseline = types.ModuleType(f"baseline_{name}")
        exec(compile(source, f"{revision}:{path}", "exec"), baseline.__dict__)
        current = importlib.import_module(f"streaming.state.stateful_{name}_processor")
        checks = check_equivalence(baseline, current, spec)
        results = {}
        for size in (1, 4, 128):
            events = make_events(size, 59)
            # Repeated keys with equal timestamps are valid across benchmark updates.
            events["event_time"] = pd.Timestamp("2022-09-01 01:00")
            timings = {"before": [], "after": []}
            for trial in range(args.rounds):
                order = (("before", baseline), ("after", current))
                if trial % 2:
                    order = order[::-1]
                for label, module in order:
                    timings[label].append(callback_seconds(module, spec, events, args.iterations))
            before, after = (statistics.median(timings[label]) for label in ("before", "after"))
            results[str(size)] = {"before_seconds": before, "after_seconds": after,
                                  "speedup": before / after}
        report["processors"][name] = {"equivalence_batches_passed": checks, "by_rows_per_key": results}
        print(f"processor={name} equivalence=PASS timings={results}", flush=True)
    report["status"] = "passed"
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Report: {args.report_path}")


def spark_worker():
    """Profile one existing pipeline; optionally load its old processor by value.

    All pipeline options follow the profiler's options. Use distinct, fresh
    checkpoints/outputs for before and after; publish the same Kafka input once.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--component", choices=PROCESSORS, required=True)
    parser.add_argument("--baseline-ref")
    parser.add_argument("--baseline-bundle", type=Path)
    parser.add_argument("--profile-path", type=Path, required=True)
    args, pipeline_options = parser.parse_known_args(sys.argv[2:])
    root = Path(__file__).resolve().parents[2]
    baseline_commit = None
    if args.profile_path.exists():
        raise FileExistsError(args.profile_path)
    if args.baseline_ref or args.baseline_bundle:
        module_name = f"streaming.state.stateful_{args.component}_processor"
        baseline = baseline_sources(root, args.baseline_ref, args.baseline_bundle)
        source = baseline["sources"][args.component]
        baseline_commit = baseline["baseline_commit"]
        module = types.ModuleType(module_name)
        exec(compile(source, module_name, "exec"), module.__dict__)
        sys.modules[module_name] = module
        # Ensure executor workers execute the baseline rather than import the
        # optimized file currently on disk under the same module name.
        from pyspark import cloudpickle
        cloudpickle.register_pickle_by_value(module)
    else:
        source = (root / f"streaming/state/stateful_{args.component}_processor.py").read_text(encoding="utf-8")

    from pyspark.sql.streaming import DataStreamWriter
    from streaming.ingestion import spark_streaming_pipeline as ingestion
    original_lookup = ingestion.load_entity_lookup
    lookup_details = {}

    def capture_lookup(spark, path):
        # The old loader performed exactly this read/select/dedup without a cache.
        lookup = original_lookup(spark, path, cache_lookup=not bool(args.baseline_ref or args.baseline_bundle))
        lookup_details["cached_at_load"] = lookup.is_cached
        return lookup

    ingestion.load_entity_lookup = capture_lookup
    original_start = DataStreamWriter.start
    queries = []

    def capture_start(writer, *options, **kwargs):
        query = original_start(writer, *options, **kwargs)
        queries.append(query)
        return query

    DataStreamWriter.start = capture_start
    profile = {"component": args.component, "baseline_ref": args.baseline_ref,
               "baseline_commit": baseline_commit,
               "processor_source_sha256": hashlib.sha256(source.encode()).hexdigest(),
               "status": "failed", "query_progress": []}
    started = time.perf_counter()
    try:
        suffix = "edge" if args.component == "edge" else args.component
        pipeline = importlib.import_module(f"streaming.ingestion.stateful_{suffix}_streaming_pipeline")
        sys.argv = [pipeline.__name__, *pipeline_options]
        pipeline.main()
        profile["status"] = "passed"
    finally:
        DataStreamWriter.start = original_start
        ingestion.load_entity_lookup = original_lookup
        profile["wall_seconds"] = time.perf_counter() - started
        profile["static_lookup"] = lookup_details
        for query in queries:
            profile["query_progress"].extend(query.recentProgress)
        from pyspark.sql import SparkSession
        session = SparkSession.getActiveSession()
        if session is not None:
            session.stop()
        args.profile_path.parent.mkdir(parents=True, exist_ok=True)
        args.profile_path.write_text(json.dumps(profile, indent=2, default=str) + "\n", encoding="utf-8")
        print(f"Profile: {args.profile_path}", flush=True)


def compare_spark():
    """Run Account before/after sequentially on identical Kafka input."""
    parser = argparse.ArgumentParser(description=compare_spark.__doc__)
    parser.add_argument("--baseline-bundle", type=Path, required=True)
    parser.add_argument("--replay-path", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--working-dir", type=Path)
    parser.add_argument("--after-first", action="store_true")
    parser.add_argument("--account-entity-lookup-path", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    args = parser.parse_args(sys.argv[2:])
    args.report_dir.mkdir(parents=True, exist_ok=False)
    work = args.working_dir or args.report_dir
    if args.working_dir is not None:
        work.mkdir(parents=True, exist_ok=False)
    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"

    def run(label, options):
        with (args.report_dir / f"{label}.log").open("w", encoding="utf-8") as output:
            subprocess.run([sys.executable, "-u", "-m", *options], env=environment,
                           stdout=output, stderr=subprocess.STDOUT, check=True)

    run("publish", ["streaming.kafka.kafka_producer", "--input-path", str(args.replay_path),
        "--max-rows", "1000", "--delay-seconds", "0", "--topic", args.topic,
        "--bootstrap-servers", args.bootstrap_servers])
    summary = {"kind": "account_spark_before_after_benchmark", "run_id": args.run_id,
               "scope": "One sequential before/after pair on the same 1,000-row Kafka replay",
               "profiles": {}}
    for label in (("after", "before") if args.after_first else ("before", "after")):
        profile_path = args.report_dir / f"{label}.json"
        options = ["streaming.operations.benchmark_stateful_processors", "_spark_worker",
            "--component", "account", "--profile-path", str(profile_path)]
        if label == "before":
            options += ["--baseline-bundle", str(args.baseline_bundle)]
        options += ["--topic", args.topic, "--bootstrap-servers", args.bootstrap_servers,
            "--max-offsets-per-trigger", "500", "--run-id", f"{args.run_id}_{label}",
            "--account-entity-lookup-path", str(args.account_entity_lookup_path),
            "--output-dir", str(work / "outputs"),
            "--checkpoint-dir", str(work / "checkpoints" / label)]
        run(label, options)
        profile = json.loads(profile_path.read_text())
        batches = [{"batch_id": progress["batchId"],
                    "input_rows": progress["numInputRows"],
                    "duration_ms": progress["durationMs"],
                    "state_operators": progress.get("stateOperators", [])}
                   for progress in profile["query_progress"] if progress["numInputRows"] > 0]
        if [batch["batch_id"] for batch in batches] != [0, 1]:
            raise AssertionError(f"{label}: expected exactly two populated batches")
        summary["profiles"][label] = {"wall_seconds": profile["wall_seconds"], "batches": batches}
        print(f"Spark {label}: {profile['wall_seconds']:.2f}s", flush=True)

    import pyarrow.parquet as pq
    for batch_id in (0, 1):
        frames = []
        for label in ("before", "after"):
            directory = (work / "outputs" / f"run_id={args.run_id}_{label}"
                         / f"batch_id={batch_id}" / "account_features.parquet")
            parts = [pq.ParquetFile(path).read().to_pandas() for path in sorted(directory.glob("*.parquet"))]
            frame = pd.concat(parts, ignore_index=True).sort_values("account_id").reset_index(drop=True)
            assert not frame["account_id"].duplicated().any()
            frames.append(frame)
        assert list(frames[0].columns) == list(frames[1].columns)
        pd.testing.assert_frame_equal(frames[0], frames[1], check_exact=False, rtol=0, atol=1e-6)
    summary["output_equivalence"] = "passed"
    summary["wall_speedup"] = (summary["profiles"]["before"]["wall_seconds"]
                               / summary["profiles"]["after"]["wall_seconds"])
    (args.report_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n")
    print(f"Spark output equivalence=PASS wall_speedup={summary['wall_speedup']:.3f}", flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_spark_worker":
        spark_worker()
    elif len(sys.argv) > 1 and sys.argv[1] == "_compare_spark":
        compare_spark()
    else:
        main()
