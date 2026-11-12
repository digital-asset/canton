"""Keep nightly topology validation coverage when test workflows are shared with PR CI."""

from pathlib import Path

import pytest
import yaml


WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def load_workflow(path: Path) -> dict:
    # Preserve on/true/false as strings rather than YAML 1.1 boolean aliases.
    return yaml.load(path.read_text(), Loader=yaml.BaseLoader)


def test_jobs(workflow: dict) -> list[dict]:
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
NIGHTLY_TEST_CALLS = [(name, job, called) for name, job, called in NIGHTLY_CALLS if test_jobs(called)]


@pytest.mark.parametrize(
    ("name", "job", "called"), NIGHTLY_TEST_CALLS, ids=[name for name, _, _ in NIGHTLY_TEST_CALLS]
)
def test_nightly_test_jobs_opt_in_and_forward_the_marker(name, job, called):
    assert job["with"]["is_nightly_workflow"] == "true", name
    inputs = called["on"]["workflow_call"]["inputs"]
    assert inputs["is_nightly_workflow"]["type"] == "boolean"
    assert inputs["is_nightly_workflow"]["default"] == "false"
    for test_job in test_jobs(called):
        assert test_job["env"]["CANTON_NIGHTLY_TEST_RUN"] == "${{ inputs.is_nightly_workflow }}"


def test_ordinary_ci_does_not_opt_into_nightly_checks():
    marked_workflows = {job["uses"] for _, job, _ in NIGHTLY_TEST_CALLS}
    ordinary_calls = []
    for path in WORKFLOWS.glob("*.yml"):
        if path.name == "canton_nightly.yml":
            continue
        for job in load_workflow(path)["jobs"].values():
            if job.get("uses") in marked_workflows:
                ordinary_calls.append(job)
                assert job.get("with", {}).get("is_nightly_workflow", "false") == "false"
    assert ordinary_calls
