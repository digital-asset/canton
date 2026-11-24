import importlib.util
from pathlib import Path


def _load_gate_module():
    module_path = Path(__file__).with_name("rerun_failed_jobs_gate.py")
    spec = importlib.util.spec_from_file_location("rerun_failed_jobs_gate", module_path)
    assert spec is not None and spec.loader is not None, (
        f"Could not load module spec for {module_path}"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rerun_failed_jobs_gate = _load_gate_module()
compute_rerun_gate = rerun_failed_jobs_gate.compute_rerun_gate


def test_compute_rerun_gate_blocks_when_blocked_job_failed():
    should_rerun, blocked_failed, blocked_jobs = compute_rerun_gate(
        failed_job_names=["Compile", "Some Test Job"],
        workflow_name="Canton Build Required",
    )

    assert should_rerun is False
    assert blocked_failed == ["Compile"]
    assert blocked_jobs == ["Compile", "Static Tests"]


def test_compute_rerun_gate_allows_when_only_non_blocked_jobs_fail():
    should_rerun, blocked_failed, blocked_jobs = compute_rerun_gate(
        failed_job_names=["Some Test Job 1", "Some Test Job 2"],
        workflow_name="Canton Build Required",
    )

    assert should_rerun is True
    assert blocked_failed == []
    assert blocked_jobs == ["Compile", "Static Tests"]


def test_compute_rerun_gate_allows_when_workflow_has_no_policy():
    should_rerun, blocked_failed, blocked_jobs = compute_rerun_gate(
        failed_job_names=["Compile", "Some Test Job"],
        workflow_name="Example Workflow With No Policy",
    )

    assert should_rerun is True
    assert blocked_failed == []
    assert blocked_jobs == []


def test_compute_rerun_gate_blocks_when_blocked_job_cancelled():
    should_rerun, blocked_failed, blocked_jobs = compute_rerun_gate(
        failed_job_names=["Static Tests", "Some Test Job"],
        workflow_name="Canton Build Required",
    )

    assert should_rerun is False
    assert blocked_failed == ["Static Tests"]
    assert blocked_jobs == ["Compile", "Static Tests"]


def test_compute_rerun_gate_matches_jobs_of_reusable_workflows():
    # The API names a job of a called workflow "<caller job> / <job>".
    should_rerun, blocked_failed, _ = compute_rerun_gate(
        failed_job_names=["Compile / Compile", "Static Tests / Check SQL table definitions"],
        workflow_name="Canton Build Required",
    )

    assert should_rerun is False
    assert blocked_failed == ["Compile", "Static Tests"]


def test_compute_rerun_gate_ignores_blocked_names_of_called_jobs():
    # Only the caller job counts: a job named "Compile" inside another caller does not block.
    should_rerun, blocked_failed, _ = compute_rerun_gate(
        failed_job_names=["Unit Tests / Compile", "Docs / Static Tests"],
        workflow_name="Canton Build Required",
    )

    assert should_rerun is True
    assert blocked_failed == []
