#!/usr/bin/env python3

import os
import subprocess
import tempfile


# Per-workflow rerun policy: if any listed job fails, skip auto-rerun.
BLOCKED_JOBS_BY_WORKFLOW = {
    "Canton Build Required": {"Static Tests", "Compile"},
}


def compute_rerun_gate(
    failed_job_names: list[str], workflow_name: str
) -> tuple[bool, list[str], list[str]]:
    blocked_names = sorted(BLOCKED_JOBS_BY_WORKFLOW.get(workflow_name, set()))
    blocked_failed = sorted(set(failed_job_names) & set(blocked_names))
    should_rerun = not blocked_failed
    return should_rerun, blocked_failed, blocked_names


def main() -> None:
    repo = os.environ["REPO"]
    run_id = os.environ["RUN_ID"]
    workflow_name = os.environ.get("WORKFLOW_NAME", "")
    emit_result_file_output = os.environ.get("EMIT_RESULT_FILE_OUTPUT", "").lower() in {
        "1",
        "true",
        "yes",
    }
    if emit_result_file_output:
        fd, output_path = tempfile.mkstemp(prefix="rerun-gate-", suffix=".txt")
        os.close(fd)
    else:
        output_path = os.environ.get("OUTPUT_FILE") or os.environ["GITHUB_OUTPUT"]

    # Treat timeout/cancel outcomes as failures for rerun-gate purposes.
    failed_job_names = [
        line.strip()
        for line in subprocess.check_output(
            [
                "gh",
                "api",
                "--paginate",
                f"/repos/{repo}/actions/runs/{run_id}/jobs?per_page=100",
                "--jq",
                '.jobs[] | select(.conclusion == "failure" or .conclusion == "timed_out" or .conclusion == "cancelled") | .name',
            ],
            text=True,
        ).splitlines()
        if line.strip()
    ]

    should_rerun, blocked_failed, blocked_names = compute_rerun_gate(
        failed_job_names=failed_job_names,
        workflow_name=workflow_name,
    )

    print(f"Workflow: {workflow_name}")
    print(f"Failed jobs: {failed_job_names}")
    if blocked_failed:
        print(
            "Skip auto-rerun because one of the blocked jobs failed: " + ", ".join(blocked_failed)
        )
    elif blocked_names:
        print("Auto-rerun allowed (no blocked failed jobs)")
    else:
        print("Auto-rerun allowed (no blocking policy for this workflow)")

    with open(output_path, "a", encoding="utf-8") as out:
        out.write(f"should_rerun={'true' if should_rerun else 'false'}\n")
        out.write(f"failed_jobs={','.join(failed_job_names)}\n")
        out.write(f"blocked_jobs={','.join(blocked_names)}\n")
        out.write(f"blocked_failed={','.join(blocked_failed)}\n")

    if emit_result_file_output:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
            out.write(f"result={output_path}\n")


if __name__ == "__main__":
    main()
