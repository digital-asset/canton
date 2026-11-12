import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from exact_test_suite import exact_pattern
from verify_topology_integration_proof import NODES, verify

SUITE = (
    "com.digitalasset.canton.integration.tests.manual.TopologyConsistencyIntegrationTestPostgres"
)


def write_proof(tmp_path, enabled=True):
    evidence = {
        "suite": SUITE,
        "commit": "current-commit",
        "run_id": "123",
        "run_attempt": "2",
        "expected_validation": enabled,
        "nightly_marker": str(enabled).lower(),
        "general_checks_enabled": True,
        "ping_succeeded": True,
        "nodes": [
            {
                "node": name,
                "config_override": enabled,
                "validator": enabled,
                "snapshot_class": (
                    "ValidatingTopologySnapshot" if enabled else "CachingTopologySnapshot"
                ),
                "storage_class": "Postgres",
                "lookup_succeeded": True,
            }
            for name in sorted(NODES)
        ],
    }
    proof = tmp_path / "proof.json"
    proof.write_text(json.dumps(evidence))
    junit = tmp_path / "TEST-merged.xml"
    junit.write_text(
        f'<testsuites><testsuite><testcase classname="{SUITE}" name="proof"/></testsuite></testsuites>'
    )
    return evidence, proof, junit


def check(proof, junit, mode="enabled"):
    return verify(proof, junit, SUITE, mode, "current-commit", "123", "2")


@pytest.mark.parametrize("enabled", [False, True])
def test_accepts_both_real_proof_modes(tmp_path, enabled):
    _, proof, junit = write_proof(tmp_path, enabled)
    summary = check(proof, junit, "enabled" if enabled else "disabled")
    assert "ping and JUnit passed" in summary
    for node in NODES:
        assert f"| {node} |" in summary


def test_expected_mode_is_independent_of_the_production_marker(tmp_path):
    _, proof, junit = write_proof(tmp_path, enabled=False)
    with pytest.raises(ValueError, match="nightly_marker"):
        check(proof, junit, "enabled")


@pytest.mark.parametrize("field", ["commit", "run_id", "run_attempt"])
def test_rejects_stale_evidence(tmp_path, field):
    evidence, proof, junit = write_proof(tmp_path)
    evidence[field] = "old"
    proof.write_text(json.dumps(evidence))
    with pytest.raises(ValueError, match=field):
        check(proof, junit)


@pytest.mark.parametrize("status", ["skipped", "failure", "error"])
def test_rejects_a_test_that_did_not_pass(tmp_path, status):
    _, proof, junit = write_proof(tmp_path)
    junit.write_text(
        f'<testsuite><testcase classname="{SUITE}" name="proof"><{status}/></testcase></testsuite>'
    )
    with pytest.raises(ValueError, match="must pass"):
        check(proof, junit)


def test_rejects_empty_test_selection_and_missing_evidence(tmp_path):
    _, proof, junit = write_proof(tmp_path)
    junit.write_text('<testsuite tests="0"/>')
    with pytest.raises(ValueError, match="actually execute"):
        check(proof, junit)
    proof.unlink()
    with pytest.raises(FileNotFoundError):
        check(proof, junit)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("config_override", False),
        ("validator", False),
        ("lookup_succeeded", False),
        ("storage_class", "Memory"),
        ("snapshot_class", "ForwardingTopologySnapshot"),
    ],
)
def test_requires_runtime_evidence_from_each_real_node(tmp_path, field, value):
    evidence, proof, junit = write_proof(tmp_path)
    evidence["nodes"][0][field] = value
    proof.write_text(json.dumps(evidence))
    with pytest.raises(ValueError):
        check(proof, junit)


def test_rejects_missing_nodes_or_a_failed_ping(tmp_path):
    evidence, proof, junit = write_proof(tmp_path)
    evidence["nodes"].pop()
    proof.write_text(json.dumps(evidence))
    with pytest.raises(ValueError, match="Missing node"):
        check(proof, junit)
    evidence, proof, junit = write_proof(tmp_path)
    evidence["ping_succeeded"] = False
    proof.write_text(json.dumps(evidence))
    with pytest.raises(ValueError, match="ping"):
        check(proof, junit)


def test_exact_suite_selection_escapes_regex_and_requires_a_compiled_suite(tmp_path):
    catalogue = tmp_path / "tests.log"
    catalogue.write_text(SUITE + "\ncom.example.OtherSuite\n")
    pattern = exact_pattern(SUITE, catalogue)
    assert re.search(pattern, SUITE)
    assert not re.search(pattern, SUITE.replace(".", "X", 1))
    assert not re.search(pattern, SUITE + "Extra")
    with pytest.raises(ValueError, match="exactly one"):
        exact_pattern("com.example.Missing", catalogue)
    catalogue.write_text(SUITE + "\n" + SUITE + "\n")
    with pytest.raises(ValueError, match="exactly one"):
        exact_pattern(SUITE, catalogue)


@pytest.mark.parametrize("explicit_suite", [False, True])
def test_real_split_action_selects_one_suite_with_mutually_exclusive_filters(
    tmp_path, explicit_suite
):
    root = Path(__file__).resolve().parents[2]
    action = yaml.load(
        (root / ".github/actions/test/split_tests/action.yml").read_text(), Loader=yaml.BaseLoader
    )
    script = action["runs"]["steps"][0]["with"]["command"]
    catalogue = tmp_path / "catalogue.log"
    ordinary = "com.example.OrdinarySuite"
    catalogue.write_text(f"{SUITE}\n{ordinary}\n")
    output = tmp_path / "github-output"
    result = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", script],
        env={
            **os.environ,
            "PATH": f"{Path(sys.executable).parent}{os.pathsep}{os.environ['PATH']}",
            "GITHUB_WORKSPACE": str(root),
            "GITHUB_OUTPUT": str(output),
            "TMPDIR": str(tmp_path),
            "TEST_FILE": str(catalogue),
            "TIMINGS_FILE": "",
            "TOTAL_SHARDS": "1",
            "SHARD_INDEX": "0",
            "FALLBACK_TEST_TIME": "300",
            "DEBUG_SPLIT_TESTS": "false",
            "EXCLUDE_PATTERNS": "" if explicit_suite else r"\.integration\.tests\.manual\.",
            "INCLUDE_PATTERNS": exact_pattern(SUITE, catalogue) if explicit_suite else "",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    result_file = Path(output.read_text().strip().removeprefix("result="))
    fields = dict(line.split("=", 1) for line in result_file.read_text().splitlines())
    assert fields["tests_per_shard"] == fields["total_tests"] == "1"
    assert fields["selected_tests"] == (SUITE if explicit_suite else ordinary)
