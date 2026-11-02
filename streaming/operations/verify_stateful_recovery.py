"""Exercise real Kafka checkpoints by failing after a batch write, then resuming.

Run only against a fresh, disposable topic/checkpoint/output namespace. The
worker wraps existing foreachBatch writers; feature formulas are unchanged.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import types


COMPONENTS = {
    "account": ("stateful_account_streaming_pipeline", "write_account_feature_batch"),
    "edge": ("stateful_edge_streaming_pipeline", "write_account_edge_batch"),
    "bank": ("stateful_bank_streaming_pipeline", "write_bank_feature_batch"),
    "entity": ("stateful_entity_streaming_pipeline", "write_entity_feature_batch"),
    "relations": ("account_relation_streaming_pipeline", "write_account_relation_batch"),
}


def fail_worker() -> None:
    """Write batch 1 normally, then fail before Spark can commit that batch."""
    component, marker = sys.argv[2:4]
    pipeline_options = sys.argv[4:]
    if pipeline_options[:1] == ["--baseline-bundle"]:
        bundle = json.loads(Path(pipeline_options[1]).read_text(encoding="utf-8"))
        pipeline_options = pipeline_options[2:]
        if component in bundle["sources"]:
            name = f"streaming.state.stateful_{component}_processor"
            baseline = types.ModuleType(name)
            exec(compile(bundle["sources"][component], name, "exec"), baseline.__dict__)
            sys.modules[name] = baseline
            from pyspark import cloudpickle
            cloudpickle.register_pickle_by_value(baseline)
    module_name, writer_name = COMPONENTS[component]
    module = importlib.import_module(f"streaming.ingestion.{module_name}")
    original = getattr(module, writer_name)

    def write_then_fail(*args, **kwargs):
        original(*args, **kwargs)
        batch_id = kwargs.get("batch_id", args[1] if len(args) > 1 else None)
        if batch_id == 1:
            Path(marker).write_text("injected_after_batch_1_write\n", encoding="utf-8")
            raise RuntimeError("INTENTIONAL_RECOVERY_TEST_FAILURE_AFTER_WRITE")

    setattr(module, writer_name, write_then_fail)
    sys.argv = [module_name, *pipeline_options]
    module.main()


def parquet_hashes(directory: Path) -> dict[str, str]:
    """Hash output bytes so a no-new-data restart must leave them untouched."""
    return {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.glob("batch_id=*/*.parquet/*.parquet"))
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--replay-path", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--account-entity-lookup-path", type=Path, required=True)
    parser.add_argument("--static-output-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--checkpoint-root", type=Path, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--baseline-bundle", type=Path,
                        help="Use old processors for the failure phase, then resume with current code.")
    args = parser.parse_args()
    run_dir = args.output_dir / f"run_id={args.run_id}"
    if run_dir.exists() or args.checkpoint_root.exists() or args.report_path.exists():
        raise FileExistsError("Recovery test requires fresh output, checkpoint, and report paths.")
    logs = args.report_path.parent / f"{args.run_id}_logs"
    logs.mkdir(parents=True, exist_ok=False)
    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"
    report = {"kind": "stateful_checkpoint_recovery_verification", "run_id": args.run_id,
              "status": "running", "components": {}, "feature_comparison": "pending"}
    if args.baseline_bundle is not None:
        report["failure_worker_baseline_commit"] = json.loads(
            args.baseline_bundle.read_text(encoding="utf-8")
        )["baseline_commit"]
        report["resume_worker_version"] = "current_working_tree"
    started = time.perf_counter()

    def run(label, command):
        with (logs / f"{label}.log").open("w", encoding="utf-8") as handle:
            return subprocess.run(command, env=environment, stdout=handle,
                                  stderr=subprocess.STDOUT, check=False).returncode

    try:
        code = run("publish", [sys.executable, "-m", "streaming.kafka.kafka_producer",
            "--input-path", str(args.replay_path), "--max-rows", "2",
            "--delay-seconds", "0", "--topic", args.topic,
            "--bootstrap-servers", args.bootstrap_servers])
        if code:
            raise RuntimeError("Kafka publication failed; see publish.log")
        for component, (module_name, _) in COMPONENTS.items():
            checkpoint = args.checkpoint_root / component
            marker = logs / f"{component}_failure_marker.txt"
            options = ["--topic", args.topic, "--bootstrap-servers", args.bootstrap_servers,
                "--max-offsets-per-trigger", "1", "--run-id", args.run_id,
                "--account-entity-lookup-path", str(args.account_entity_lookup_path),
                "--output-dir", str(args.output_dir), "--checkpoint-dir", str(checkpoint)]
            if component in {"bank", "entity"}:
                options += ["--static-metadata-dir", str(args.static_output_dir)]
            failure_command = [sys.executable, "-m",
                "streaming.operations.verify_stateful_recovery", "_worker", component,
                str(marker)]
            if args.baseline_bundle is not None:
                failure_command += ["--baseline-bundle", str(args.baseline_bundle)]
            failed_code = run(f"{component}_injected_failure", [*failure_command, *options])
            if not failed_code or not marker.exists():
                raise RuntimeError(f"{component}: expected after-write failure was not observed")
            if not (checkpoint / "commits/0").exists() or (checkpoint / "commits/1").exists():
                raise RuntimeError(f"{component}: unexpected commit state after injected failure")
            command = [sys.executable, "-m", f"streaming.ingestion.{module_name}", *options]
            if run(f"{component}_resume", command) or not (checkpoint / "commits/1").exists():
                raise RuntimeError(f"{component}: checkpoint resume failed")
            before = parquet_hashes(run_dir)
            if not before:
                raise RuntimeError(f"{component}: no Parquet rows were written")
            if run(f"{component}_no_new_data", command):
                raise RuntimeError(f"{component}: no-new-data restart failed")
            if before != parquet_hashes(run_dir):
                raise RuntimeError(f"{component}: no-new-data restart modified package files")
            report["components"][component] = {
                "after_write_failure_observed": True, "batch_0_committed_before_failure": True,
                "batch_1_recovered": True, "no_new_data_output_unchanged": True,
            }
            print(f"recovery component={component} PASS", flush=True)
        comparison = run("feature_comparison", [sys.executable, "-m",
            "streaming.operations.validate_stateful_replay", "--replay-path", str(args.replay_path),
            "--account-entity-lookup-path", str(args.account_entity_lookup_path),
            "--static-output-dir", str(args.static_output_dir),
            "--model-updates-output-dir", str(args.output_dir), "--run-id", args.run_id,
            "--max-offsets-per-trigger", "1"])
        if comparison:
            raise RuntimeError("Recovered feature comparison failed; see feature_comparison.log")
        report["feature_comparison"] = "passed"
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
        raise
    finally:
        report["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        args.report_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.report_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        temporary.replace(args.report_path)
        print(f"recovery overall={report['status']} report={args.report_path}", flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_worker":
        fail_worker()
    else:
        main()
