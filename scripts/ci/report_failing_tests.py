#!/usr/bin/env python3
"""Runs the flaky-test CI pipeline as a single entry point.

Four focused steps run in sequence, passing data via JSON artifacts in a shared
temp dir:

    parse_failing_tests   ->  failing_tests.json
    report_to_datadog     <-  failing_tests.json
    manage_flaky_issues   <-  failing_tests.json, evidence.json  ->  streaks.json
    alert_slack           <-  streaks.json

evidence.json is written beforehand by collect_flake_evidence.py, a separate CI
step, so that the suite logs can be uploaded as an artifact in between and
listed in the evidence comments (FLAKE_*_ARTIFACT_NAME / _ID). Without it the
issues are managed as before, just without evidence comments.

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
import json
import os
import sys
import tempfile
import traceback
from pathlib import Path
from unittest.mock import patch

import alert_slack
import collect_flake_evidence
import flaky_common
import manage_flaky_issues
import parse_failing_tests
import report_to_datadog
import select_rota
import generate_watchdog_xml


def _notify_step_failure(name: str) -> None:
    """Best-effort: page the CI rota when a pipeline step crashes.

    A step crashing already makes the job exit non-zero, but that reads the same as any
    other test failure, nobody is told this was the *pipeline itself* breaking, not a
    test. Call this from inside the ``except`` block so ``traceback.format_exc()`` still
    sees the live exception.
    """
    try:
        roster = json.loads(select_rota.ROSTER_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Could not load roster to report the step failure either: {exc!r}")
        return
    tb = traceback.format_exc()[-2000:]
    select_rota.notify_tooling_failure(
        f":warning: report_failing_tests.py step `{name}` crashed on `{flaky_common.branch}`, "
        f"reported test failures for this run may be incomplete:\n```{tb}```",
        roster,
        os.environ.get(select_rota.SLACK_TOKEN_ENV),
    )


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
        ("collect_flake_evidence", collect_flake_evidence.self_test),
        ("report_to_datadog", report_to_datadog.self_test),
        ("manage_flaky_issues", manage_flaky_issues.self_test),
        ("select_rota", select_rota.self_test),
        ("alert_slack", alert_slack.self_test),
        ("generate_watchdog_xml", generate_watchdog_xml.self_test),
        ("report_failing_tests", self_test),
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


def self_test() -> None:
    """_notify_step_failure pages the CI rota via select_rota.notify_tooling_failure
    when called from inside an except block, and doesn't raise if the roster is
    unreadable."""
    real_notify = select_rota.notify_tooling_failure
    real_roster_path = select_rota.ROSTER_PATH
    calls: list[tuple[str, str | None]] = []
    try:
        select_rota.notify_tooling_failure = lambda message, roster, token, **kw: calls.append(
            (message, token)
        )
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            _notify_step_failure("issues")
        assert len(calls) == 1
        assert "`issues`" in calls[0][0] and "boom" in calls[0][0]

        select_rota.ROSTER_PATH = Path("/nonexistent/roster.json")
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            _notify_step_failure("issues")  # unreachable roster -> must not raise
        assert len(calls) == 1  # ... and no additional call either
    finally:
        select_rota.notify_tooling_failure = real_notify
        select_rota.ROSTER_PATH = real_roster_path

    # The checks above only exercise _notify_step_failure in isolation, so they would
    # not catch the notification call being dropped from main()'s own step loop. Drive
    # main() for real, with the other three steps stubbed as no-ops and "issues" raising.
    calls.clear()
    real_notify = select_rota.notify_tooling_failure
    saved_artifact_dir = os.environ.get("FLAKY_ARTIFACT_DIR")
    try:
        select_rota.notify_tooling_failure = lambda message, roster, token, **kw: calls.append(
            (message, token)
        )
        with (
            patch("parse_failing_tests.run"),
            patch("report_to_datadog.run"),
            patch("manage_flaky_issues.run", side_effect=RuntimeError("kaboom")),
            patch("alert_slack.run"),
        ):
            rc = main([])
        assert rc == 1, rc
        assert len(calls) == 1, calls
        assert "`issues`" in calls[0][0] and "kaboom" in calls[0][0], calls
    finally:
        select_rota.notify_tooling_failure = real_notify
        if saved_artifact_dir is None:
            os.environ.pop("FLAKY_ARTIFACT_DIR", None)
        else:
            os.environ["FLAKY_ARTIFACT_DIR"] = saved_artifact_dir

    print("report_failing_tests self-checks passed")


def test_self_test() -> None:
    """Pytest entry point so scripts/ CI exercises the self-check above (otherwise
    only reachable via --self-test, which nothing wires up automatically)."""
    self_test()


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
            _notify_step_failure(name)
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
