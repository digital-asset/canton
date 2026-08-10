#!/usr/bin/env python3
"""Guard that keeps `canton_build_required_gate` in sync with the job list.

The gate job in `.github/workflows/canton_build_required.yml` aggregates every
other job through its `needs:` list so branch protection can require a single
stable check. That list is hand-maintained, so it can silently drift from the
jobs it is meant to cover:

  - a new job added without being appended to `needs` is left un-gated, and the
    gate stays green even when that job fails
  - a renamed or removed job leaves a `needs` entry pointing at nothing
  - an excluded job (`notify`, the gate itself) wrongly added to `needs` makes
    the gate depend on something it should not

This script asserts the invariant that the gate's `needs` equals the set of all
jobs minus an explicit exclusion allowlist. It exits non-zero (listing the
offenders) when the invariant is broken.

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

# Jobs deliberately outside the gate. `notify` reports whether the failure alert
# sent, not build health, and a job cannot depend on itself. Adding to this set
# is a conscious, reviewable choice, which is the whole point of the guard.
EXCLUDED = {"notify", GATE}


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


def main():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    jobs = (workflow or {}).get("jobs") or {}

    if GATE not in jobs:
        sys.exit(f"{WORKFLOW.name}: could not find the `{GATE}` job")

    gate_needs = _needs_list(jobs[GATE])
    if not gate_needs:
        sys.exit(f"{WORKFLOW.name}: `{GATE}` has an empty `needs`, so it gates nothing")

    job_names = set(jobs)
    needs = set(gate_needs)

    missing = sorted((job_names - EXCLUDED) - needs)
    stale = sorted(needs - job_names)
    wrongly_included = sorted(EXCLUDED & needs)
    duplicates = sorted({job for job in gate_needs if gate_needs.count(job) > 1})

    if missing or stale or wrongly_included or duplicates:
        print(f"{GATE} in {WORKFLOW.name} has drifted from the job list:")
        if missing:
            print("  jobs not covered by the gate (add them to `needs`):")
            for job in missing:
                print(f"    - {job}")
        if stale:
            print("  `needs` entries with no matching job (remove or rename):")
            for job in stale:
                print(f"    - {job}")
        if wrongly_included:
            print("  excluded jobs that must not be in `needs` (remove them):")
            for job in wrongly_included:
                print(f"    - {job}")
        if duplicates:
            print("  `needs` entries listed more than once (remove the duplicates):")
            for job in duplicates:
                print(f"    - {job}")
        print(
            "  If a job is intentionally outside the gate, add it to EXCLUDED in "
            f"{Path(__file__).name}."
        )
        sys.exit(1)

    print(
        f"{GATE} covers all {len(job_names - EXCLUDED)} gated jobs "
        f"(excluding {', '.join(sorted(EXCLUDED))})."
    )


if __name__ == "__main__":
    main()
