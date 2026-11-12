"""Keep nightly topology validation coverage when test workflows are shared with PR CI."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def load_workflow(path: Path) -> dict:
    # Preserve on/true/false as strings rather than YAML 1.1 boolean aliases.
    return yaml.load(path.read_text(), Loader=yaml.BaseLoader)


def running_test_jobs(workflow: dict) -> list[dict]:
    return [
        job
        for job in workflow["jobs"].values()
        if any(
            step.get("uses", "").startswith("./.github/actions/test/run_")
            for step in job.get("steps", [])
        )
    ]


# Include every test workflow called by the nightly, rather than a fixed allowlist.
NIGHTLY_CALLS = [
    (name, job, load_workflow(WORKFLOWS / Path(job["uses"]).name))
    for name, job in load_workflow(WORKFLOWS / "canton_nightly.yml")["jobs"].items()
    if job.get("uses", "").startswith("./.github/workflows/")
]
NIGHTLY_TEST_CALLS = [
    (name, job, called) for name, job, called in NIGHTLY_CALLS if running_test_jobs(called)
]


@pytest.mark.parametrize(
    ("name", "job", "called"), NIGHTLY_TEST_CALLS, ids=[name for name, _, _ in NIGHTLY_TEST_CALLS]
)
def test_nightly_test_jobs_opt_in_and_forward_the_marker(name, job, called):
    assert job["with"]["is_nightly_workflow"] == "true", name
    inputs = called["on"]["workflow_call"]["inputs"]
    assert inputs["is_nightly_workflow"]["type"] == "boolean"
    assert inputs["is_nightly_workflow"]["default"] == "false"
    for test_job in running_test_jobs(called):
        assert test_job["env"]["CANTON_NIGHTLY_TEST_RUN"] == "${{ inputs.is_nightly_workflow }}"


def test_ordinary_ci_does_not_opt_into_nightly_checks():
    marked_workflows = {job["uses"] for _, job, _ in NIGHTLY_TEST_CALLS}
    ordinary_calls = []
    for path in WORKFLOWS.glob("*.yml"):
        if path.name == "canton_nightly.yml":
            continue
        for name, job in load_workflow(path)["jobs"].items():
            if job.get("uses") in marked_workflows:
                if (
                    path.name == "canton_build_required.yml"
                    and name == "topology_consistency_nightly"
                ):
                    # This draft-only call deliberately exercises the nightly mode in a PR.
                    assert job["with"]["expected_topology_validation"] == "enabled"
                    continue
                ordinary_calls.append(job)
                assert job.get("with", {}).get("is_nightly_workflow", "false") == "false"
    assert ordinary_calls


def test_real_integration_proof_exercises_the_default_and_explicit_nightly_input():
    jobs = load_workflow(WORKFLOWS / "canton_build_required.yml")["jobs"]
    normal = jobs["topology_consistency_normal"]
    nightly = jobs["topology_consistency_nightly"]
    assert normal["uses"] == nightly["uses"] == "./.github/workflows/_test.yml"
    assert normal["needs"] == nightly["needs"] == ["pick_runner", "compile"]
    assert "is_nightly_workflow" not in normal["with"]
    assert nightly["with"]["is_nightly_workflow"] == "true"
    assert normal["with"]["expected_topology_validation"] == "disabled"
    assert nightly["with"]["expected_topology_validation"] == "enabled"
    assert normal["with"]["test_suite"] == nightly["with"]["test_suite"]
    assert normal["with"]["job_name"] != nightly["with"]["job_name"]


def test_exact_suite_filter_is_forwarded_through_the_nix_composite_output():
    action = load_workflow(WORKFLOWS.parent / "actions/test/run_ordinary_tests/action.yml")
    steps = action["runs"]["steps"]
    selection = next(step for step in steps if step.get("id") == "exact_suite")
    run_tests = next(
        step for step in steps if step.get("uses") == "./.github/actions/test/run_tests"
    )
    assert selection["with"]["output-name"] == "include-pattern"
    assert run_tests["with"]["include_patterns"] == "${{ steps.exact_suite.outputs.result }}"


@pytest.mark.parametrize(
    ("normal", "nightly", "required", "expected_code"),
    [
        ("success", "success", "true", 0),
        ("skipped", "success", "true", 1),
        ("success", "skipped", "true", 1),
        ("skipped", "skipped", "false", 0),
    ],
)
def test_draft_gate_requires_both_proofs_to_finish_successfully(
    normal, nightly, required, expected_code
):
    gate = load_workflow(WORKFLOWS / "canton_build_required.yml")["jobs"][
        "canton_build_required_gate"
    ]
    step = next(step for step in gate["steps"] if step.get("name") == "Evaluate workflow results")
    script = step["with"]["command"].split("python3 <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    result = subprocess.run(
        [sys.executable, "-c", script],
        env={
            **os.environ,
            "TOPOLOGY_PROOF_REQUIRED": required,
            "NEEDS_JSON": json.dumps(
                {
                    "topology_consistency_normal": {"result": normal},
                    "topology_consistency_nightly": {"result": nightly},
                }
            ),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == expected_code, result.stderr
