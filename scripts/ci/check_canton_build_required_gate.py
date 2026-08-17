#!/usr/bin/env python3
"""Guard that keeps the aggregate jobs in `canton_build_required` in sync.

Two jobs in `.github/workflows/canton_build_required.yml` are expected to gather
every other job through their `needs:` list:

  - `canton_build_required_gate` aggregates every job so branch protection can
    require a single stable check
  - `notify` sends the red-main failure alert, so it must see every job for the
    alert to fire (and name the culprit) whenever any of them fails

Both lists are hand-maintained, so they can silently drift from the jobs they
are meant to cover:

  - a new job added without being appended to a list is left un-gated or
    un-alerted, and the aggregator stays green even when that job fails
  - a renamed or removed job leaves a `needs` entry pointing at nothing
  - an excluded job (`notify`, the gate itself) wrongly added to `needs` makes
    the aggregator depend on something it should not

This script asserts the invariant that each aggregator's `needs` equals the set
of all jobs minus an explicit exclusion allowlist. It exits non-zero (listing
the offenders) when the invariant is broken.

It also guards `scripts/ci/notify_job_classification.yml`, which `notify` reads
to decide which failures alert the red-main channel. This script asserts its
`test_jobs` and `infra_jobs` partition the same job set the aggregators cover, so
a newly added or renamed job cannot slip through unclassified.

The workflow is parsed with PyYAML (available in the repo's nix shell) rather
than by hand, so any valid reshaping of the YAML (inline comments, quoted keys,
reordered keys) is understood without special-casing.

Run locally with:  python3 scripts/ci/check_canton_build_required_gate.py
"""

import sys
from pathlib import Path

import notify_job_classification
import yaml

WORKFLOW = Path(__file__).parents[2] / ".github/workflows/canton_build_required.yml"
GATE = "canton_build_required_gate"
NOTIFY = "notify"

# Repo-relative path to the classification file, so drift messages point the
# reader straight at the file to edit rather than a bare basename. Both modules
# are siblings in scripts/ci, so this is always under the same repo root that
# WORKFLOW above relies on.
CLASSIFICATION_PATH = notify_job_classification.CLASSIFICATION.relative_to(
    Path(__file__).parents[2]
).as_posix()

# Aggregator jobs expected to cover every *other* job, and so kept in sync here.
AGGREGATORS = (GATE, NOTIFY)

# Jobs no aggregator should depend on. `notify` and the gate report alert or
# aggregate status rather than build health, and a job cannot depend on itself,
# so both are excluded from every aggregator's expected coverage. Adding to this
# set is a conscious, reviewable choice, which is the whole point of the guard.
EXCLUDED = {NOTIFY, GATE}


def _needs_list(job):
    """Return a job's `needs` as a list.

    GitHub Actions allows `needs` to be absent, a single job name, or a list of
    job names. Normalise all three so duplicate detection sees the raw entries.
    """
    needs = job.get("needs")
    if needs is None:
        return []
    if isinstance(needs, str):
        return [needs]
    return list(needs)


def _check(name, job, job_names):
    """Return the drift problems for one aggregator (empty list when in sync)."""
    raw_needs = _needs_list(job)
    if not raw_needs:
        return [f"  `{name}` has an empty `needs`, so it covers nothing"]

    needs = set(raw_needs)
    expected = job_names - EXCLUDED
    missing = sorted(expected - needs)
    stale = sorted(needs - job_names)
    wrongly_included = sorted(EXCLUDED & needs)
    duplicates = sorted({job for job in raw_needs if raw_needs.count(job) > 1})

    problems = []
    if missing:
        problems.append("  jobs not covered (add them to `needs`):")
        problems += [f"    - {job}" for job in missing]
    if stale:
        problems.append("  `needs` entries with no matching job (remove or rename):")
        problems += [f"    - {job}" for job in stale]
    if wrongly_included:
        problems.append("  excluded jobs that must not be in `needs` (remove them):")
        problems += [f"    - {job}" for job in wrongly_included]
    if duplicates:
        problems.append("  `needs` entries listed more than once (remove duplicates):")
        problems += [f"    - {job}" for job in duplicates]
    return problems


def _classification_problems(job_names, test_jobs, infra_jobs):
    """Return classification drift problems (empty when in sync).

    `test_jobs` and `infra_jobs` must partition the aggregators' coverage (all
    jobs minus EXCLUDED): every such job in one list and none in both.
    """
    classified = test_jobs | infra_jobs
    expected = job_names - EXCLUDED

    unclassified = sorted(expected - classified)
    stale = sorted(classified - job_names)
    overlap = sorted(test_jobs & infra_jobs)
    excluded_listed = sorted(EXCLUDED & classified)

    problems = []
    if unclassified:
        problems.append(
            f"  jobs not classified (add to `test_jobs` or `infra_jobs` in {CLASSIFICATION_PATH}):"
        )
        problems += [f"    - {job}" for job in unclassified]
    if stale:
        problems.append("  classified names with no matching job (remove or rename):")
        problems += [f"    - {job}" for job in stale]
    if overlap:
        problems.append("  jobs listed as both test and infra (keep only one):")
        problems += [f"    - {job}" for job in overlap]
    if excluded_listed:
        problems.append("  excluded jobs that must not be classified (remove them):")
        problems += [f"    - {job}" for job in excluded_listed]
    return problems


def _check_classification(job_names):
    """Load notify_job_classification.yml and return its drift problems."""
    test_jobs, infra_jobs = notify_job_classification.load_classification()
    return _classification_problems(job_names, test_jobs, infra_jobs)


def main():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    jobs = (workflow or {}).get("jobs") or {}
    job_names = set(jobs)

    failed = False
    for name in AGGREGATORS:
        if name not in jobs:
            print(f"{WORKFLOW.name}: could not find the `{name}` job")
            failed = True
            continue
        problems = _check(name, jobs[name], job_names)
        if problems:
            failed = True
            print(f"`{name}` in {WORKFLOW.name} has drifted from the job list:")
            for line in problems:
                print(line)

    classification_problems = _check_classification(job_names)
    if classification_problems:
        failed = True
        print(f"{CLASSIFICATION_PATH} has drifted from the job list:")
        for line in classification_problems:
            print(line)

    if failed:
        print(
            "  If a job is intentionally outside an aggregator, add it to "
            f"EXCLUDED in {Path(__file__).name}."
        )
        sys.exit(1)

    print(
        f"`{GATE}` and `{NOTIFY}` each cover all {len(job_names - EXCLUDED)} jobs "
        f"(excluding {', '.join(sorted(EXCLUDED))}), and every job is classified "
        f"in {CLASSIFICATION_PATH}."
    )


# --- tests (collected by the scripts pytest job) ---------------------------


def test_classification_accepts_a_full_partition():
    job_names = {"compile", "test", NOTIFY, GATE}
    assert _classification_problems(job_names, {"test"}, {"compile"}) == []


def test_classification_flags_unclassified_job():
    problems = _classification_problems({"compile", "test", "brand_new"}, {"test"}, {"compile"})
    assert any("brand_new" in line for line in problems)


def test_classification_flags_overlap_and_stale():
    problems = _classification_problems(
        {"compile", "test"}, {"test", "compile"}, {"compile", "gone"}
    )
    assert any("compile" in line for line in problems)  # overlap
    assert any("gone" in line for line in problems)  # stale


def test_classification_flags_excluded_job_listed():
    problems = _classification_problems({"test", NOTIFY}, {"test", NOTIFY}, set())
    assert any(NOTIFY in line for line in problems)


def test_real_files_are_in_sync():
    """The shipped YAML must actually classify the real workflow's jobs."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    job_names = set((workflow or {}).get("jobs") or {})
    assert _check_classification(job_names) == []


if __name__ == "__main__":
    main()
