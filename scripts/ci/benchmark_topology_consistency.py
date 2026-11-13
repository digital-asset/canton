"""Draft-only, paired timing experiment for the ordinary PV36 test selection."""

import argparse
import hashlib
import importlib.util
import json
import os
import re
import resource
import shutil
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

import yaml
from parse_sbt_cmd import parse_cmd
from verify_topology_integration_proof import verify

ROOT = Path(__file__).resolve().parents[2]
PROOF = (
    "com.digitalasset.canton.integration.tests.manual.TopologyConsistencyIntegrationTestPostgres"
)
EXCLUDED = "com.digitalasset.canton.integration.tests.version.ProtobufCompatibilityReaderTest"
SHARDS = 12


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def action_output(script, extra_env, key):
    """Execute the production action's own command/selection code, without duplicating it."""
    with tempfile.NamedTemporaryFile() as output:
        subprocess.run(
            ["bash", "-e", "-o", "pipefail", "-c", script],
            env={**os.environ, **extra_env, "GITHUB_OUTPUT": output.name},
            check=True,
            cwd=ROOT,
        )
        lines = Path(output.name).read_text().splitlines()
    for index, line in enumerate(lines):
        if line.startswith(key + "="):
            return line.split("=", 1)[1]
        if line.startswith(key + "<<"):
            end = lines.index(line.split("<<", 1)[1], index + 1)
            return "\n".join(lines[index + 1 : end])
    raise ValueError(f"Production action did not emit {key}")


def prepare(catalog, timings, output):
    action = ROOT / ".github/actions/test/run_ordinary_tests/action.yml"
    steps = yaml.safe_load(action.read_text())["runs"]["steps"]
    scripts = {step["id"]: step["run"] for step in steps if "run" in step}
    exclusions = action_output(scripts["resolve-exclude-patterns"], {}, "exclude_patterns")
    command = action_output(
        scripts["build-sbt-command"],
        {"EXCLUDE_UNSTABLE_TESTS": "true", "INCLUDE_ONLY_REQUIRES_EXTERNAL_KMS_TESTS": "false"},
        "command",
    )
    spec = importlib.util.spec_from_file_location(
        "production_splitter", ROOT / "scripts/ci/split-tests/split_tests.py"
    )
    splitter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(splitter)
    names = splitter.load_test_names(str(catalog))
    require(
        names.count(PROOF) == names.count(EXCLUDED) == 1,
        "Expected unique compiled proof/compatibility suite",
    )
    selected = splitter.filter_tests(names, exclusions.splitlines())
    require(EXCLUDED in selected and PROOF not in selected, "Ordinary selection changed")
    selected.remove(EXCLUDED)
    buckets, _ = splitter.assign_to_shards(
        splitter.build_weighted_items(selected, splitter.load_timings(str(timings)), 300), SHARDS
    )
    require(all(buckets), "Every benchmark shard must have real ordinary tests")
    plan = {
        "commit": os.environ["GITHUB_SHA"],
        "run_id": os.environ["GITHUB_RUN_ID"],
        "protocol_version": "36",
        "baseline_policy": "Preserve the existing nightly settings, including performance opt-outs",
        "catalog_sha256": digest(catalog.read_bytes()),
        # Preserve production's cross-project duplicate entries in both modes.
        "catalog_duplicate_entries": {
            name: count for name, count in Counter(names).items() if count > 1
        },
        "timings_sha256": digest(timings.read_bytes()),
        "selection_action_sha256": digest(action.read_bytes()),
        "excluded_suites": [EXCLUDED],
        "proof_suite": PROOF,
        "command_template": command,
        "buckets": buckets,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(plan, indent=2))
    print(f"Froze {len(selected)} ordinary suites into {SHARDS} shards; excluded only {EXCLUDED}")


def order(shard):
    return [True, False] if shard % 2 == 0 else [False, True]


def reset_postgres(container, shard):
    require(os.environ.get("CI") == "true", "This benchmark only resets a dedicated CI container")
    expected = f"canton-postgres-{os.environ['GITHUB_RUN_ID']}-{shard}"
    require(container == expected, "Refusing to reset an unexpected Postgres container")
    # Only databases in this job's disposable container are reset. Roles/config remain identical.
    subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container,
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "test",
            "-d",
            "postgres",
        ],
        input=(
            "SELECT format('DROP DATABASE %I WITH (FORCE);', datname) FROM pg_database "
            "WHERE datname NOT IN ('postgres', 'template0', 'template1')\n\\gexec\n"
            "CREATE DATABASE test;\n"
        ),
        text=True,
        check=True,
    )
    subprocess.run(["docker", "restart", container], check=True)
    for _ in range(60):
        ready = subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", "test", "-d", "test"],
            stdout=subprocess.DEVNULL,
            check=False,
        )
        if ready.returncode == 0:
            return
        time.sleep(1)
    raise ValueError("Benchmark Postgres did not become ready")


def junit_snapshot(destination):
    root = ET.Element("testsuite", name="topology-benchmark")
    for report in sorted(set(ROOT.glob("**/target/**/TEST-*.xml"))):
        for case in ET.parse(report).iter("testcase"):
            root.append(case)
    ET.ElementTree(root).write(destination, encoding="utf-8", xml_declaration=True)
    return root


def clean_runtime_files(plan_path, output):
    require(
        os.environ.get("GITHUB_ACTIONS") == "true", "Fresh checkouts are only cleaned in Actions"
    )
    require(Path(os.environ["GITHUB_WORKSPACE"]).resolve() == ROOT, "Unexpected benchmark checkout")
    # Preserve compiled targets and raw results; reset generated test files between invocations.
    subprocess.run(
        [
            "git",
            "clean",
            "-ffdxq",
            "-e",
            "target/",
            "-e",
            output.relative_to(ROOT).as_posix() + "/",
            "-e",
            plan_path.resolve().relative_to(ROOT).as_posix(),
        ],
        cwd=ROOT,
        check=True,
    )


def case_signature(root):
    return Counter(
        (case.get("classname", ""), case.get("name", ""), case.find("skipped") is not None)
        for case in root.iter("testcase")
    )


def run(plan_path, shard, container, output):
    plan = json.loads(plan_path.read_text())
    for field, env in [("commit", "GITHUB_SHA"), ("run_id", "GITHUB_RUN_ID")]:
        require(plan[field] == os.environ[env], f"Stale benchmark plan: {field}")
    require(len(plan["buckets"]) == SHARDS and 0 <= shard < SHARDS, "Invalid shard plan")
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    command = plan["command_template"].replace(
        "$RUN_SPLITTED_TESTS_CMD", "testOnly " + " ".join([PROOF, *plan["buckets"][shard]])
    )
    args = parse_cmd(command)
    signatures = []
    samples = []
    for index, enabled in enumerate(order(shard)):
        sample_dir = output / f"sample-{index}"
        sample_dir.mkdir()
        reset_postgres(container, shard)
        clean_runtime_files(plan_path, output)
        # Cached compilation can include reports. Never accept a previous invocation's JUnit.
        for report in set(ROOT.glob("**/target/**/TEST-*.xml")):
            report.unlink()
        for name in ["log", "track"]:
            shutil.rmtree(ROOT / name, ignore_errors=True)
            (ROOT / name).mkdir()
        proof = sample_dir / "node-proof.json"
        mode = "enabled" if enabled else "disabled"
        env = {
            **os.environ,
            "CANTON_NIGHTLY_TEST_RUN": str(enabled).lower(),
            "CANTON_TOPOLOGY_VALIDATION_EXPECTED": mode,
            "CANTON_TOPOLOGY_PROOF_EVIDENCE": str(proof),
            "CANTON_PROTOCOL_VERSION": "36",
            "CANTON_CRASH_ON_PRETTY_PRINTING_ERRORS": "true",
            "SBT_OUTPUT_FILE": str(sample_dir / "sbt.log"),
            "SBT_BOOTSTRAP_RETRY_ATTEMPTS": "1",
            "REPORT_TO_DATADOG": "false",
            "FAIL_ON_ERROR_IN_OUTPUT": "true",
            "SUCCEED_ON_ERROR": "false",
            "EXECUTION_CONTEXT_SIZE": "8",
            "MAX_CONCURRENT_SBT_TEST_TASKS": "4",
            "EXTRA_PARAMETERS": "-J-Xlog:gc=info,safepoint=info:file=track/GC.log:time",
            "LOG_IMMEDIATE_FLUSH": "true",
            "TIMEOUT": "40m",
            "MAX_SINGLE_TEST_MINUTES": "30",
        }
        print(f"BENCHMARK shard={shard} sample={index} topology_validation={mode}", flush=True)
        cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        start = time.monotonic()
        result = subprocess.run(
            ["./scripts/ci/sbt-ci-wrapper.sh", *args], env=env, cwd=ROOT, check=False
        )
        elapsed = time.monotonic() - start
        cpu_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        record = {
            "index": index,
            "validation_enabled": enabled,
            "elapsed_seconds": elapsed,
            "cpu_seconds": cpu_after.ru_utime
            + cpu_after.ru_stime
            - cpu_before.ru_utime
            - cpu_before.ru_stime,
            "exit_code": result.returncode,
            "valid": False,
        }
        try:
            junit = sample_dir / "junit.xml"
            root = junit_snapshot(junit)
            signature = case_signature(root)
            require(result.returncode == 0, "SBT failed; this timing cannot establish savings")
            require(
                not re.search(
                    r"^\[info\] compiling \d+ (?:Scala|Java) sources? to ",
                    (sample_dir / "sbt.log").read_text(),
                    re.MULTILINE,
                ),
                "Unexpected compilation inside the measured interval",
            )
            require(
                not list(root.iter("failure")) and not list(root.iter("error")),
                "Failing JUnit result",
            )
            require(
                sum(count for (_, _, skipped), count in signature.items() if not skipped) > 1,
                "Ordinary tests did not execute",
            )
            verify(
                proof,
                junit,
                PROOF,
                mode,
                plan["commit"],
                plan["run_id"],
                os.environ["GITHUB_RUN_ATTEMPT"],
            )
            signatures.append(signature)
            record["executed_tests"] = sum(
                count for (_, _, skipped), count in signature.items() if not skipped
            )
            record["case_signature_sha256"] = digest(json.dumps(sorted(signature.items())).encode())
            record["valid"] = True
        except (ValueError, OSError, ET.ParseError) as error:
            # failure-surfaced: printed invalid sample; final require and summarizer reject it.
            record["error"] = str(error)
        samples.append(record)
        # Keep the complete raw record even if a later invocation fails or is cancelled.
        (output / "result.json").write_text(
            json.dumps(
                {
                    "commit": plan["commit"],
                    "run_id": plan["run_id"],
                    "run_attempt": os.environ["GITHUB_RUN_ATTEMPT"],
                    "shard": shard,
                    "runner": os.environ.get("RUNNER_NAME"),
                    "plan_sha256": digest(plan_path.read_bytes()),
                    "command": args,
                    "samples": samples,
                },
                indent=2,
            )
        )
        print(json.dumps(record), flush=True)
    require(
        len(signatures) == 2 and all(x == signatures[0] for x in signatures),
        "Both invocations must pass and execute identical test cases",
    )


def summarize(directory, output):
    files = sorted(directory.rglob("result.json"))
    all_results = [json.loads(path.read_text()) for path in files]
    require(
        len({(x["shard"], x["run_attempt"]) for x in all_results}) == len(all_results),
        "Duplicate shard attempt",
    )
    latest = {}
    for result in sorted(all_results, key=lambda x: int(x["run_attempt"])):
        latest[result["shard"]] = result
    results = list(latest.values())
    require(set(latest) == set(range(SHARDS)), "Missing benchmark shards")
    for field in ["commit", "run_id", "plan_sha256"]:
        require(len({x[field] for x in results}) == 1, f"Mixed benchmark {field}")
    for field, env in [("commit", "GITHUB_SHA"), ("run_id", "GITHUB_RUN_ID")]:
        if os.environ.get(env):
            require(results[0][field] == os.environ[env], f"Stale benchmark {field}")
    totals = {True: 0.0, False: 0.0}
    shard_deltas = []
    for result in results:
        samples = result["samples"]
        require(
            len(samples) == 2 and all(s["valid"] and s["exit_code"] == 0 for s in samples),
            "Incomplete/failed sample",
        )
        require(
            [s["validation_enabled"] for s in samples] == order(result["shard"]),
            "Wrong sample order",
        )
        require([s["index"] for s in samples] == list(range(2)), "Wrong sample indices")
        require(
            len({s["case_signature_sha256"] for s in samples}) == 1,
            "Different tests executed between modes",
        )
        on = next(s["elapsed_seconds"] for s in samples if s["validation_enabled"])
        off = next(s["elapsed_seconds"] for s in samples if not s["validation_enabled"])
        require(on > 0 and off > 0, "Invalid duration")
        totals[True] += on
        totals[False] += off
        shard_deltas.append(on - off)
    saving = sum(shard_deltas)
    summary = {
        "commit": results[0]["commit"],
        "run_id": results[0]["run_id"],
        "scope": "ordinary PV36, non-external-KMS; excludes protobuf compatibility reader; includes per-invocation four-node proof",
        "validation_on_runner_hours": totals[True] / 3600,
        "validation_off_runner_hours": totals[False] / 3600,
        "saved_runner_hours_per_selection": saving / 3600,
        "saved_percent": 100 * saving / totals[True],
        "shards_faster_with_validation_off": sum(x > 0 for x in shard_deltas),
        "shards": SHARDS,
        "invocations": SHARDS * 2,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2))
    text = (
        f"Ordinary PV36 topology benchmark: {summary['saved_runner_hours_per_selection']:.3f} "
        f"runner-hours saved ({summary['saved_percent']:.2f}%).\n\n"
        f"Existing baseline checks: {totals[True] / 3600:.3f} runner-hours; disabled: {totals[False] / 3600:.3f}.\n\n"
        f"One on/off comparison per shard; "
        f"off was faster on {summary['shards_faster_with_validation_off']}/{SHARDS} shards.\n\n"
        "Scope: ordinary PV36 without external KMS. Excludes the known protobuf compatibility "
        "reader failure. Every invocation includes a passing four-node Postgres validator proof. "
        "Setup/cache restore/database reset/artifact upload are outside the measured SBT interval. "
        "This is a directional estimate from one comparison, not a long-term performance guarantee. "
        "No failed/retried/skipped invocations enter the estimate.\n"
    )
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as report:
            report.write(text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["prepare", "run", "summarize"])
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--timings", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--shard", type=int)
    parser.add_argument("--container")
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.operation == "prepare":
        prepare(args.catalog, args.timings, args.output)
    elif args.operation == "run":
        run(args.plan, args.shard, args.container, args.output)
    else:
        summarize(args.directory, args.output)
