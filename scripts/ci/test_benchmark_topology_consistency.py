"""Reject misleading performance results and exercise the production selector/command."""

import json
import subprocess

import pytest

import benchmark_topology_consistency as benchmark


def test_prepare_executes_the_production_selection_and_command(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_SHA", "commit")
    monkeypatch.setenv("GITHUB_RUN_ID", "run")
    ordinary = [f"com.digitalasset.canton.integration.tests.Example{i}Test" for i in range(30)]
    excluded = [
        "com.digitalasset.canton.integration.tests.docs.snippet.ExampleTest",
        "com.digitalasset.canton.integration.tests.manual.ExampleTest",
        "com.digitalasset.canton.integration.tests.crashrecovery.ExampleTest",
    ]
    catalog = tmp_path / "catalog"
    catalog.write_text("\n".join([*ordinary, ordinary[0], *excluded, benchmark.PROOF, benchmark.EXCLUDED]))
    timings = tmp_path / "timings.json"
    timings.write_text("{}")
    output = tmp_path / "plan.json"
    benchmark.prepare(catalog, timings, output)
    plan = json.loads(output.read_text())
    assert {suite for bucket in plan["buckets"] for suite in bucket} == set(ordinary)
    assert len(plan["buckets"]) == 12
    assert sum(bucket.count(ordinary[0]) for bucket in plan["buckets"]) == 2
    command = plan["command_template"].replace("$RUN_SPLITTED_TESTS_CMD", "testOnly ExampleTest")
    assert benchmark.parse_cmd(command) == [
        "dumpClassPath",
        "testOnly ExampleTest -- -l UnstableTest -l com.digitalasset.canton.annotations.UnstableTest "
        "-l RequiresExternalKms -l com.digitalasset.canton.annotations.RequiresExternalKms",
        "checkErrors",
    ]


def result(shard, on=100, off=80, attempt="1"):
    return {
        "shard": shard,
        "commit": "commit",
        "run_id": "run",
        "run_attempt": attempt,
        "plan_sha256": "plan",
        "samples": [
            {
                "index": index,
                "validation_enabled": enabled,
                "elapsed_seconds": on if enabled else off,
                "valid": True,
                "exit_code": 0,
                "case_signature_sha256": "identical-tests",
            }
            for index, enabled in enumerate(benchmark.order(shard))
        ],
    }


def records(tmp_path):
    directory = tmp_path / "raw"
    for shard in range(12):
        sample = directory / f"shard-{shard}"
        sample.mkdir(parents=True)
        (sample / "result.json").write_text(json.dumps(result(shard)))
    return directory


def test_summarize_counts_runner_time_and_preserves_negative_savings(tmp_path):
    directory = records(tmp_path)
    output = tmp_path / "summary.json"
    benchmark.summarize(directory, output)
    summary = json.loads(output.read_text())
    assert summary["validation_on_runner_hours"] == pytest.approx(1200 / 3600)
    assert summary["saved_runner_hours_per_selection"] == pytest.approx(240 / 3600)
    assert summary["saved_percent"] == 20
    assert summary["invocations"] == 48
    for path in directory.rglob("result.json"):
        value = json.loads(path.read_text())
        path.write_text(json.dumps(result(value["shard"], on=80, off=100)))
    benchmark.summarize(directory, output)
    assert json.loads(output.read_text())["saved_runner_hours_per_selection"] < 0


@pytest.mark.parametrize("corruption", ["failed", "retry", "different_tests", "order", "indices", "duration", "commit", "plan"])
def test_summarize_rejects_invalid_or_incomparable_samples(tmp_path, corruption):
    directory = records(tmp_path)
    path = directory / "shard-0/result.json"
    value = json.loads(path.read_text())
    if corruption == "failed":
        value["samples"][0]["valid"] = False
    elif corruption == "retry":
        value["samples"][0]["exit_code"] = 1
    elif corruption == "different_tests":
        value["samples"][0]["case_signature_sha256"] = "different"
    elif corruption == "order":
        value["samples"][0]["validation_enabled"] = False
    elif corruption == "indices":
        value["samples"][0]["index"] = 1
    elif corruption == "duration":
        value["samples"][0]["elapsed_seconds"] = -1
    else:
        value["commit" if corruption == "commit" else "plan_sha256"] = "different"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        benchmark.summarize(directory, tmp_path / "summary.json")


def test_missing_shard_cannot_produce_a_savings_estimate(tmp_path):
    directory = records(tmp_path)
    (directory / "shard-0/result.json").unlink()
    with pytest.raises(ValueError, match="Missing"):
        benchmark.summarize(directory, tmp_path / "summary.json")


def test_latest_failed_attempt_cannot_fall_back_to_an_earlier_pass(tmp_path):
    directory = records(tmp_path)
    latest = directory / "retry"
    latest.mkdir()
    value = result(0, attempt="2")
    value["samples"][0]["valid"] = False
    (latest / "result.json").write_text(json.dumps(value))
    with pytest.raises(ValueError, match="failed"):
        benchmark.summarize(directory, tmp_path / "summary.json")


def test_database_reset_refuses_any_other_container(monkeypatch):
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    with pytest.raises(ValueError, match="unexpected"):
        benchmark.reset_postgres("anything-else", 0)


def test_fresh_invocations_preserve_nested_compilation_targets_and_evidence(tmp_path, monkeypatch):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    source = tmp_path / "source.scala"
    source.write_text("tracked")
    subprocess.run(["git", "-C", str(tmp_path), "add", "source.scala"], check=True)
    compiled = tmp_path / "community/app/target/classes/Example.class"
    compiled.parent.mkdir(parents=True)
    compiled.write_text("compiled")
    plan = tmp_path / "plan.json"
    plan.write_text("plan")
    output = tmp_path / "results"
    output.mkdir()
    (output / "raw.json").write_text("evidence")
    generated = tmp_path / "generated-runtime-file"
    generated.write_text("stale")
    monkeypatch.setattr(benchmark, "ROOT", tmp_path)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_WORKSPACE", str(tmp_path))
    benchmark.clean_runtime_files(plan, output)
    assert not generated.exists()
    assert source.exists() and compiled.exists() and plan.exists() and (output / "raw.json").exists()
