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

The workflow is parsed with PyYAML (available in the repo's nix shell) rather
than by hand, so any valid reshaping of the YAML (inline comments, quoted keys,
reordered keys) is understood without special-casing.

Run locally with:  python3 scripts/ci/check_canton_build_required_gate.py
"""

import sys
from pathlib import Path

import yaml

WORKFLOW = Path(__file__).parents[2] / ".github/workflows/canton_build_required.yml"
GATE = "canton_build_required_gate"
NOTIFY = "notify"

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

    if failed:
        print(
            "  If a job is intentionally outside an aggregator, add it to "
            f"EXCLUDED in {Path(__file__).name}."
        )
        sys.exit(1)

    print(
        f"`{GATE}` and `{NOTIFY}` each cover all {len(job_names - EXCLUDED)} jobs "
        f"(excluding {', '.join(sorted(EXCLUDED))})."
    )


if __name__ == "__main__":
    main()
