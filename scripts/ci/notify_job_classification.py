#!/usr/bin/env python3
"""Decide which canton_build_required job failures alert the red-main channel.

Test-job failures go to the flaky pipeline, so they must not ping the red-main
channel. Build/infra failures must. The split lives in the sibling
notify_job_classification.yml, shared by _notify.yml (alerts) and
check_canton_build_required_gate.py (guards it against drift).

Reads NEEDS_JSON from the environment and prints the comma-separated jobs to
alert, or nothing so the caller can stay silent.
"""

import json
import os
import sys
from pathlib import Path

import yaml

CLASSIFICATION = Path(__file__).with_name("notify_job_classification.yml")

FAILED_RESULTS = frozenset({"failure", "cancelled"})


def load_classification(path=CLASSIFICATION):
    """Return ``(test_jobs, infra_jobs)`` as sets read from the YAML file."""
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return set(data.get("test_jobs") or []), set(data.get("infra_jobs") or [])


def alerting_failures(needs, test_jobs):
    """Return the failed/cancelled non-test jobs to alert, preserving needs order.

    Denylist not allowlist: an unclassified job that breaks still alerts, and the
    drift guard flags the missing classification separately.
    """
    return [
        name
        for name, detail in needs.items()
        if (detail or {}).get("result") in FAILED_RESULTS and name not in test_jobs
    ]


def main():
    needs = json.loads(os.environ["NEEDS_JSON"])
    test_jobs, _infra_jobs = load_classification()

    all_failed = [n for n, d in needs.items() if (d or {}).get("result") in FAILED_RESULTS]
    suppressed = [n for n in all_failed if n in test_jobs]
    failed = alerting_failures(needs, test_jobs)

    if suppressed:
        # stderr, so it stays out of the stdout the caller captures.
        print(
            "Suppressed test-job failures (tracked via GH flaky issues): " + ", ".join(suppressed),
            file=sys.stderr,
        )
    print("FAILED (alerting): " + (", ".join(failed) or "<none>"), file=sys.stderr)

    print(", ".join(failed))


# --- tests (collected by the scripts pytest job) ---------------------------


def test_alerting_failures_suppresses_test_jobs():
    needs = {
        "compile": {"result": "failure"},
        "test": {"result": "failure"},
        "build_docs": {"result": "success"},
        "toxiproxy_test_fast": {"result": "cancelled"},
    }
    test_jobs = {"test", "toxiproxy_test_fast"}
    assert alerting_failures(needs, test_jobs) == ["compile"]


def test_alerting_failures_preserves_order_and_allows_unclassified():
    needs = {
        "scalafix": {"result": "failure"},
        "test": {"result": "failure"},
        "brand_new_job": {"result": "failure"},
    }
    assert alerting_failures(needs, {"test"}) == ["scalafix", "brand_new_job"]


def test_alerting_failures_empty_when_only_tests_fail():
    needs = {"test": {"result": "failure"}, "compile": {"result": "success"}}
    assert alerting_failures(needs, {"test"}) == []


def test_load_classification_reads_the_real_file():
    test_jobs, infra_jobs = load_classification()
    assert "test" in test_jobs
    assert "compile" in infra_jobs
    assert test_jobs.isdisjoint(infra_jobs)


if __name__ == "__main__":
    main()
