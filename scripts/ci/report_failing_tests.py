#!/usr/bin/env python3
"""Runs the flaky-test CI pipeline as a single entry point.

Four focused steps run in sequence, passing data via JSON artifacts in a shared
temp dir:

    parse_failing_tests   ->  failing_tests.json
    report_to_datadog     <-  failing_tests.json
    manage_flaky_issues   <-  failing_tests.json  ->  streaks.json
    alert_slack           <-  streaks.json

The call sites (the GHA composite action, the CircleCI @test command, and
sbt-ci-wrapper.sh) share this one entry point. Each step is imported and run
in-process. A step that raises is logged and does not skip the others, but the
pipeline still exits non-zero so CI surfaces it. A missing or empty input
artifact makes a downstream step a no-op.

Usage: report_failing_tests.py [FAILURE_MESSAGE] [--self-test]
  FAILURE_MESSAGE (optional) records a single failing "test", used by
  sbt-ci-wrapper.sh on an SBT crash. Otherwise ./test-reports is scanned.
"""

import argparse
import os
import sys
import tempfile
import traceback

import alert_slack
import flaky_common
import manage_flaky_issues
import parse_failing_tests
import report_to_datadog
import select_rota


def _artifact_dir() -> str:
    return (
        os.environ.get("FLAKY_ARTIFACT_DIR")
        or os.environ.get("RUNNER_TEMP")
        or tempfile.mkdtemp(prefix="flaky-")
    )


def _run_self_tests() -> int:
    # Run each step's self-test in-process. Collect results without short-circuiting
    # so a failure in one step still surfaces the rest.
    checks = [
        ("flaky_common", flaky_common.self_test),
        ("parse_failing_tests", parse_failing_tests.self_test),
        ("report_to_datadog", report_to_datadog.self_test),
        ("manage_flaky_issues", manage_flaky_issues.self_test),
        ("select_rota", select_rota.self_test),
        ("alert_slack", alert_slack.self_test),
    ]
    rc = 0
    for name, fn in checks:
        print(f"--- flaky pipeline self-test: {name} ---", flush=True)
        try:
            flaky_common.run_guarded_self_test(fn)
        except Exception:
            print(f"flaky pipeline: self-test '{name}' failed", flush=True)
            traceback.print_exc()
            rc = 1
    return rc


def main(argv) -> int:
    parser = argparse.ArgumentParser(description="Run the flaky-test CI pipeline.")
    parser.add_argument(
        "failure_message",
        nargs="?",
        default=None,
        help="Optional CI failure message recorded as the single failing test (sbt crash path).",
    )
    parser.add_argument(
        "--self-test", action="store_true", help="Run every step's self-test and exit."
    )
    args = parser.parse_args(argv)

    if args.self_test:
        return _run_self_tests()

    # All steps share one artifact directory so the JSON handoffs line up.
    artifact_dir = _artifact_dir()
    os.environ["FLAKY_ARTIFACT_DIR"] = artifact_dir
    failing_tests_json = os.path.join(artifact_dir, "failing_tests.json")
    streaks_json = os.path.join(artifact_dir, "streaks.json")

    # Each step is imported and run in-process. With a crash message the parse step
    # records it as the single failing test (sbt crash path), otherwise it scans
    # ./test-reports plus found_problems.txt. A step that raises is logged and does
    # not skip the others, but we still exit non-zero so CI surfaces it.
    steps = [
        ("parse", lambda: parse_failing_tests.run(args.failure_message, failing_tests_json)),
        ("datadog", lambda: report_to_datadog.run(failing_tests_json)),
        ("issues", lambda: manage_flaky_issues.run(failing_tests_json, streaks_json)),
        ("slack", lambda: alert_slack.run(streaks_json)),
    ]
    rc = 0
    for name, step in steps:
        print(f"--- flaky pipeline: {name} ---", flush=True)
        try:
            step()
        except Exception:
            print(
                f"flaky pipeline: step '{name}' failed (continuing with remaining steps)",
                flush=True,
            )
            traceback.print_exc()
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
