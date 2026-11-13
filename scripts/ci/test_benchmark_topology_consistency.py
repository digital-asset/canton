"""Reject misleading performance results and exercise the production selector/command."""

import json
import subprocess
import textwrap

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


@pytest.mark.parametrize("fail_disabled", [False, True])
def test_run_uses_fresh_process_env_reports_and_node_proof(tmp_path, monkeypatch, fail_disabled):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    wrapper = tmp_path / "scripts/ci/sbt-ci-wrapper.sh"
    wrapper.parent.mkdir(parents=True)
    wrapper.write_text(textwrap.dedent("""\
        #!/usr/bin/env python3
        import json, os
        from pathlib import Path
        enabled = os.environ['CANTON_NIGHTLY_TEST_RUN'] == 'true'
        assert os.environ['CANTON_TOPOLOGY_VALIDATION_EXPECTED'] == ('enabled' if enabled else 'disabled')
        report = Path('community/app/target/test-reports/TEST-fresh.xml')
        assert not report.exists()
        report.parent.mkdir(parents=True, exist_ok=True)
        suite = 'com.digitalasset.canton.integration.tests.manual.TopologyConsistencyIntegrationTestPostgres'
        status = '<failure/>' if os.environ.get('FAKE_FAIL_DISABLED') == 'true' and not enabled else ''
        report.write_text(f'<testsuite><testcase classname="{suite}" name="proof"/>'
                          f'<testcase classname="OrdinaryTest" name="ordinary">{status}</testcase></testsuite>')
        evidence = {
            'suite': suite, 'commit': os.environ['GITHUB_SHA'], 'run_id': os.environ['GITHUB_RUN_ID'],
            'run_attempt': os.environ['GITHUB_RUN_ATTEMPT'], 'nightly_marker': str(enabled).lower(),
            'expected_validation': enabled, 'general_checks_enabled': True, 'ping_succeeded': True,
            'nodes': [{'node': node, 'config_override': enabled, 'validator': enabled,
                       'lookup_succeeded': True, 'storage_class': 'Postgres',
                       'snapshot_class': 'ValidatingTopologySnapshot' if enabled else 'CachingTopologySnapshot'}
                      for node in ['participant1', 'participant2', 'sequencer1', 'mediator1']],
        }
        Path(os.environ['CANTON_TOPOLOGY_PROOF_EVIDENCE']).write_text(json.dumps(evidence))
        Path(os.environ['SBT_OUTPUT_FILE']).write_text('completed without compilation')
        """))
    wrapper.chmod(0o755)
    subprocess.run(["git", "-C", str(tmp_path), "add", "scripts"], check=True)
    monkeypatch.setattr(benchmark, "ROOT", tmp_path)
    resets = []
    monkeypatch.setattr(benchmark, "reset_postgres", lambda container, shard: resets.append(shard))
    for name, value in {
        "GITHUB_ACTIONS": "true", "GITHUB_WORKSPACE": str(tmp_path), "GITHUB_SHA": "commit",
        "GITHUB_RUN_ID": "run", "GITHUB_RUN_ATTEMPT": "2", "FAKE_FAIL_DISABLED": str(fail_disabled).lower(),
    }.items():
        monkeypatch.setenv(name, value)
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({
        "commit": "commit", "run_id": "run", "command_template": '"$RUN_SPLITTED_TESTS_CMD"',
        "buckets": [["OrdinaryTest"] for _ in range(12)],
    }))
    output = tmp_path / "results"
    if fail_disabled:
        with pytest.raises(ValueError, match="must pass"):
            benchmark.run(plan, 0, "fake-container", output)
    else:
        benchmark.run(plan, 0, "fake-container", output)
    saved = json.loads((output / "result.json").read_text())
    assert resets == [0, 0, 0, 0]
    assert saved["run_attempt"] == "2"
    assert [s["validation_enabled"] for s in saved["samples"]] == [True, False, False, True]
    assert [s["valid"] for s in saved["samples"]] == ([True, False, False, True] if fail_disabled else [True] * 4)
