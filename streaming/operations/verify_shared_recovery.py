"""Verify partial shared-batch failure, readiness, checkpoint replay, and retry.

Use a fresh topic, run ID, checkpoint root, and output namespace. This is an
opt-in integration utility; it never removes checkpoints or generated evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def worker():
    from streaming.ingestion.shared_stateful_streaming_pipeline import parse_args, run_pipeline
    marker = Path(sys.argv[2])
    sys.argv = [sys.argv[0], *sys.argv[3:]]

    def fail(component, batch_id):
        if component == "edge" and batch_id == 1:
            marker.write_text("failed after edge write before state commit\n", encoding="utf-8")
            raise RuntimeError("INTENTIONAL_SHARED_RECOVERY_FAILURE")

    run_pipeline(parse_args(), after_component=fail)


def file_hashes(directory):
    return {str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(directory.rglob("*")) if path.is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--replay-path", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--feature-shuffle-partitions", type=int, default=1)
    for name in ("account-entity-lookup-path", "static-metadata-dir", "output-dir",
                 "rejected-output-dir", "metrics-output-dir", "checkpoint-dir", "report-path"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    run_dir = args.output_dir / f"run_id={args.run_id}"
    if run_dir.exists() or args.checkpoint_dir.exists() or args.report_path.exists():
        raise FileExistsError("Shared recovery verification requires fresh output, checkpoint, and report paths.")
    logs = args.report_path.parent / f"{args.run_id}_logs"
    logs.mkdir(parents=True, exist_ok=False)
    environment = {**os.environ, "PYTHONUNBUFFERED": "1"}

    def run(label, command):
        with (logs / f"{label}.log").open("w", encoding="utf-8") as handle:
            return subprocess.run(command, env=environment, stdout=handle,
                                  stderr=subprocess.STDOUT, check=False).returncode

    options = ["--topic", args.topic, "--run-id", args.run_id,
               "--bootstrap-servers", args.bootstrap_servers, "--max-offsets-per-trigger", "1",
               "--feature-shuffle-partitions", str(args.feature_shuffle_partitions)]
    for name in ("account_entity_lookup_path", "static_metadata_dir", "output_dir",
                 "rejected_output_dir", "metrics_output_dir", "checkpoint_dir"):
        options.extend(["--" + name.replace("_", "-"), str(getattr(args, name))])
    command = [sys.executable, "-m", "streaming.ingestion.shared_stateful_streaming_pipeline", *options]
    report = {"kind": "shared_pipeline_recovery_verification", "run_id": args.run_id, "status": "running"}
    started = time.perf_counter()
    try:
        if run("publish", [sys.executable, "-m", "streaming.kafka.kafka_producer",
                           "--input-path", str(args.replay_path), "--max-rows", "2", "--delay-seconds", "0",
                           "--topic", args.topic, "--bootstrap-servers", args.bootstrap_servers]):
            raise RuntimeError("Publication failed; see publish.log")
        marker = logs / "failure.txt"
        code = run("injected_failure", [sys.executable, "-m", "streaming.operations.verify_shared_recovery",
                                         "_worker", str(marker), *options])
        if not code or not marker.is_file():
            raise RuntimeError("The intended after-write failure was not observed.")
        batch_0 = run_dir / "batch_id=0"
        if not (batch_0 / "manifest.json").is_file():
            raise RuntimeError("Batch 0 was not ready before batch 1's failure.")
        if (run_dir / "batch_id=1/manifest.json").exists():
            raise RuntimeError("Incomplete batch 1 was exposed as ready.")
        receipt = json.loads((args.checkpoint_dir / "receipts/batch_id=1/edge.json").read_text())
        if (args.checkpoint_dir / "edge/commits" / str(receipt["query_batch_id"])).exists():
            raise RuntimeError("The failure did not occur before the Edge commit.")
        report["batch_0_ready_before_failure"] = True
        report["incomplete_batch_1_not_ready"] = True
        before = file_hashes(batch_0)
        if run("resume", command):
            raise RuntimeError("Checkpoint resume failed; see resume.log")
        if before != file_hashes(batch_0):
            raise RuntimeError("Resume changed the already-ready batch 0.")
        if not (run_dir / "batch_id=1/manifest.json").is_file():
            raise RuntimeError("Recovered batch 1 has no ready manifest.")
        report["partial_batch_recovered"] = True
        before = file_hashes(run_dir)
        audit_directory = args.metrics_output_dir / f"run_id={args.run_id}"
        before_audit = file_hashes(audit_directory)
        if (run("no_new_data", command) or before != file_hashes(run_dir)
                or before_audit != file_hashes(audit_directory)):
            raise RuntimeError("No-new-data restart failed or changed model packages.")
        report["no_new_data_output_unchanged"] = True
        if run("feature_comparison", [sys.executable, "-m", "streaming.operations.validate_stateful_replay",
                                       "--replay-path", str(args.replay_path),
                                       "--account-entity-lookup-path", str(args.account_entity_lookup_path),
                                       "--static-output-dir", str(args.static_metadata_dir),
                                       "--model-updates-output-dir", str(args.output_dir),
                                       "--run-id", args.run_id, "--max-offsets-per-trigger", "1"]):
            raise RuntimeError("Recovered feature values differ; see feature_comparison.log")
        report["feature_comparison"] = "passed"
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
        raise
    finally:
        report["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        args.report_path.parent.mkdir(parents=True, exist_ok=True)
        args.report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"shared recovery overall={report['status']} report={args.report_path}", flush=True)


if __name__ == "__main__":
    if sys.argv[1:2] == ["_worker"]:
        worker()
    else:
        main()
