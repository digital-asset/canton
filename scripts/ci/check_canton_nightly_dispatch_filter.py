#!/usr/bin/env python3
"""Guard that keeps every `canton_nightly` job selectable through the `jobs` dispatch input.

`.github/workflows/canton_nightly.yml` runs on `workflow_dispatch` with a `jobs` input
listing the job ids to run (empty runs everything). Each job opts in through its `if`:

    if: inputs.jobs == '' || contains(format(',{0},', inputs.jobs), ',<job id>,')

That condition is hand-written per job, so it can silently drift:

  - a new job added without it runs on every dispatch, whatever was selected
  - a job missing the `inputs.jobs == ''` part never runs in a full nightly
  - a copied condition naming another job's id makes the job unselectable and runs it
    whenever that other job is selected

Without arguments, this script asserts that every job outside an explicit allowlist has
an `if` with both parts, selecting it by its own id. It exits non-zero (listing the
offenders) when it does not. Both parts are matched as substrings of the
whitespace-stripped `if`, so this catches drift from the usual pattern rather than
parsing it: a condition that merely mentions both passes.

With `--validate JOBS`, it checks a dispatch's `jobs` input instead (the `validate_jobs`
job runs it): every id must name a selectable job, and each selected job's `needs` must
be selected too or run always. Otherwise the job is skipped and the run goes green with
nothing executed. Jobs whose `if` checks `!cancelled()` handle skipped upstream jobs
themselves (e.g. `unstable_test_slow_36` after `toxiproxy_test_slow_36`), so their
`needs` may stay unselected.

Run locally with:  python3 scripts/ci/check_canton_nightly_dispatch_filter.py [--validate JOBS]
"""

import argparse
import sys
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).parents[2] / ".github/workflows/canton_nightly.yml"

# Jobs that run on every dispatch: the build prerequisites the selected jobs need, the
# validation of the input itself, and `notify`, which reports on whatever ran.
ALWAYS_RUN = {"pick_runner", "validate_jobs", "compile", "package_community", "notify"}

FULL_RUN = "inputs.jobs==''"
SELECTOR = "contains(format(',{{0}},',inputs.jobs),',{job},')"
TOLERATES_SKIPPED_NEEDS = "!cancelled()"


def _condition(job):
    """Return a job's `if` with all whitespace removed, so formatting does not matter."""
    return "".join(str((job or {}).get("if", "")).split())


def _needs(job):
    needs = (job or {}).get("needs") or []
    return [needs] if isinstance(needs, str) else list(needs)


def _unselectable(jobs):
    """Return the ids of jobs outside ALWAYS_RUN whose `if` does not both run them in a
    full nightly and select them by their own id."""
    return sorted(
        key
        for key, job in jobs.items()
        if key not in ALWAYS_RUN
        and not (FULL_RUN in _condition(job) and SELECTOR.format(job=key) in _condition(job))
    )


def _selection_problems(jobs, selection):
    """Return why the `jobs` input `selection` cannot run as asked (empty when it can)."""
    if any(c.isspace() for c in selection):
        return [f"the jobs input must not contain whitespace: {selection!r}"]
    selected = [job for job in selection.split(",") if job]
    if not selected:
        return [
            f"the jobs input selects no job: {selection!r} "
            "(leave it empty to run the whole nightly)"
        ]
    selectable = set(jobs) - ALWAYS_RUN
    problems = [f"{job!r} is not a selectable job" for job in selected if job not in selectable]
    for job in selected:
        if job not in selectable or TOLERATES_SKIPPED_NEEDS in _condition(jobs[job]):
            continue
        missing = [n for n in _needs(jobs[job]) if n not in ALWAYS_RUN and n not in selected]
        if missing:
            problems.append(
                f"{job!r} needs {', '.join(missing)}, which would be skipped (select them too)"
            )
    return problems


def _load_jobs():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return (workflow or {}).get("jobs") or {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--validate", metavar="JOBS", help="check a dispatch's jobs input")
    args = parser.parse_args()
    jobs = _load_jobs()

    if args.validate is not None:
        problems = _selection_problems(jobs, args.validate)
        for problem in problems:
            print(f"::error::{problem}")
        if problems:
            sys.exit(1)
        print(f"Selected jobs: {args.validate}")
        return

    missing = _unselectable(jobs)
    if missing:
        print(
            f"Jobs in {WORKFLOW.name} that the `jobs` dispatch input cannot select (give them "
            f"`if: inputs.jobs == '' || {SELECTOR.format(job='<job id>')}`, or add the job "
            f"to ALWAYS_RUN in {Path(__file__).name}):"
        )
        print("\n".join(f"  - {job}" for job in missing))
        sys.exit(1)
    print(
        f"All {len(jobs) - len(ALWAYS_RUN & set(jobs))} selectable jobs in {WORKFLOW.name} "
        "have the dispatch filter."
    )


# --- tests (collected by the scripts pytest job) ---------------------------


def _filter(job):
    return f"inputs.jobs == '' || contains(format(',{{0}},', inputs.jobs), ',{job},')"


TOLERANT = (
    "(inputs.jobs == '' && success()) ||\n"
    "(!cancelled() && needs.compile.result == 'success' &&\n"
    " contains(format(',{0},', inputs.jobs), ',test_37,'))"
)

JOBS = {
    "pick_runner": {},
    "compile": {"needs": "pick_runner"},
    "test_36": {"needs": ["pick_runner", "compile"], "if": _filter("test_36")},
    "test_37": {"needs": ["pick_runner", "compile", "test_36"], "if": TOLERANT},
    "docs": {"needs": ["pick_runner", "test_36"], "if": _filter("docs")},
}


def test_jobs_with_full_run_clause_and_own_selector_pass():
    assert _unselectable(JOBS) == []


def test_job_without_filter_fails():
    assert _unselectable({"compile": {}, "test_36": {}, "notify": {"if": "always()"}}) == [
        "test_36"
    ]


def test_job_with_selector_only_fails():
    jobs = {"test_36": {"if": "contains(format(',{0},', inputs.jobs), ',test_36,')"}}
    assert _unselectable(jobs) == ["test_36"]


def test_job_selecting_another_id_fails():
    assert _unselectable({"test_37": {"if": _filter("test_36")}}) == ["test_37"]


def test_selection_with_needs_selected_passes():
    assert _selection_problems(JOBS, "test_36,docs") == []


def test_selection_missing_needs_fails():
    assert _selection_problems(JOBS, "docs") == [
        "'docs' needs test_36, which would be skipped (select them too)"
    ]


def test_selection_of_job_tolerating_skipped_needs_passes():
    assert _selection_problems(JOBS, "test_37") == []


def test_selection_of_unknown_or_always_run_job_fails():
    assert _selection_problems(JOBS, "nope,compile") == [
        "'nope' is not a selectable job",
        "'compile' is not a selectable job",
    ]


def test_selection_without_job_ids_fails():
    assert _selection_problems(JOBS, ",,") == [
        "the jobs input selects no job: ',,' (leave it empty to run the whole nightly)"
    ]


def test_selection_with_whitespace_fails():
    assert _selection_problems(JOBS, "test_36, docs") == [
        "the jobs input must not contain whitespace: 'test_36, docs'"
    ]


if __name__ == "__main__":
    main()
