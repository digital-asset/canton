#!/usr/bin/env python3
"""Collects the failing tests from a CI run into a JSON artifact.

Reads the JUnit XML test reports under ./test-reports plus an optional
found_problems.txt log-failure summary, and writes the set of failing test
names to failing_tests.json for report_to_datadog.py and manage_flaky_issues.py.

Usage:
  parse_failing_tests.py [FAILURE_MESSAGE] [--failing-tests-json PATH] [--self-test]

With a positional FAILURE_MESSAGE (used by sbt-ci-wrapper.sh on an SBT crash)
the message is recorded as the single failing "test" and the test-reports are
not scanned. Otherwise the reports and log are parsed.
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, ElementTree
import os
import re
from typing import Dict
from pathlib import Path

from flaky_common import (
    write_failing_tests,
    failing_tests_path,
    run_guarded_self_test,
    DEFAULT_CAUSE,
)


# sbt generates test-reports in junit xml style that we aggregate into test-reports/<subproject>/<reports> in CircleCI
# through the CircleCI command `upload_test_reports`
# This methods reads those test reports to find out which tests failed
# see also https://www.scala-sbt.org/1.x/docs/Testing.html#Test+Reports
def iterate_through_test_reports():
    # Maps each failing test to its cause bucket (see flaky_common.DEFAULT_CAUSE).
    failing_tests: Dict[str, str] = {}
    for entry in Path('./test-reports').rglob("*.xml"):
        if entry.is_file():
            process_test_report(entry, failing_tests)
    return failing_tests


def process_test_report(path: Path, failing_tests: Dict[str, str]):
    tree: ElementTree = ET.parse(path)
    root: Element = tree.getroot()
    # To understand this XML parsing I recommend to just go through this code with a debugger on
    # an example test report; alternatively see e.g. https://stackoverflow.com/a/26661423
    for child in root:
        if 'name' not in child.attrib:
            continue
        problems = [childchild for childchild in child if childchild.tag in ('failure', 'error')]
        if problems:
            # Example value: LedgerAPIParticipantPruningTestPostgres
            test_name = child.attrib.get('classname', child.attrib.get('name', 'unknown')).split(
                '.'
            )[-1]
            # A watchdog kill is emitted by generate_watchdog_xml.py as a <failure
            # type="WatchdogTimeout">, so distinguish it from a regular assertion.
            is_timeout = any(p.get('type') == 'WatchdogTimeout' for p in problems)
            cause = 'timeout' if is_timeout else DEFAULT_CAUSE
            print(f"Found failing test '{test_name}' (cause: {cause})")
            # A timeout is more specific than a plain failure, so never let a
            # regular report for the same suite downgrade a recorded timeout.
            if failing_tests.get(test_name) != 'timeout':
                failing_tests[test_name] = cause
    return failing_tests


# Canton emits an MDC "<logger>:<TestSuite>" marker only inside a test's thread
# context, so the colon reliably tags a log line with the suite under test.
# Third-party/shutdown loggers (grpc, pekko) and SBT [warn]/[error] lines carry
# no such marker and fall through to the residual bucket in compute_log_failures.
_SUITE_LOG_RE = re.compile(r'(WARN|ERROR)\s+[\w.$]+:([A-Z][A-Za-z0-9_]*)')


def compute_log_failures(lines: list[str]) -> Dict[str, str]:
    """Buckets found_problems.txt lines by cause, attributing per test suite.

    Lines carrying a suite (LEVEL logger:Suite/...) are grouped per suite and
    keyed by the suite, with cause 'error_other [Suite]' / 'warn_other [Suite]'
    (ERROR wins over WARN for a suite). Every remaining line with no suite
    (third-party loggers, SBT [warn]/[error], unmatched noise) collapses into a
    single residual entry via compute_single_log_failure, keyed by that text
    with a bare 'error_other' / 'warn_other'.
    """
    suite_levels: Dict[str, str] = {}
    residual_lines: list[str] = []
    for line in lines:
        match = _SUITE_LOG_RE.search(line)
        if match:
            level, suite = match.group(1), match.group(2)
            if suite_levels.get(suite) != 'ERROR':
                suite_levels[suite] = level
        else:
            residual_lines.append(line)

    result: Dict[str, str] = {}
    for suite, level in suite_levels.items():
        bucket = 'error_other' if level == 'ERROR' else 'warn_other'
        result[suite] = f"{bucket} [{suite}]"

    residual = compute_single_log_failure(residual_lines)
    if residual:
        # Bucket by the leading log level, not any "error" mention in the message
        # body. compute_single_log_failure normalizes the chosen line so it starts
        # with its level token, so a WARN line that merely says "error" downstream
        # stays warn_other. Text with no recognizable level defaults to warn_other,
        # the safer bucket for unclassifiable noise.
        result[residual] = (
            'error_other' if re.match(r'\s*error', residual, re.IGNORECASE) else 'warn_other'
        )
    return result


def check_for_log_failures(failing_tests_result: Dict[str, str]):
    if not os.path.exists("found_problems.txt"):
        return failing_tests_result
    with open("found_problems.txt", "r") as f:
        # splitlines() strips trailing newlines unlike readlines()
        lines = f.read().splitlines()

    for key, cause in compute_log_failures(lines).items():
        # A JUnit failure or timeout already recorded for the same suite is the
        # real cause, so a log warning must not overwrite it. Only add problems
        # we don't already know about.
        if key in failing_tests_result:
            continue
        failing_tests_result[key] = cause
        print(f"Reporting log problem ({cause}): '{key}'")

    return failing_tests_result


def compute_single_log_failure(lines: list[str]):
    if not lines:
        return None
    failures = []
    for line in lines:
        failure = None
        try:
            # If WARN or ERROR (case sensitive) is in the line, we report the line from WARN/ERROR until the end of the logger name
            failure = re.search("((WARN|ERROR).*?)(:| -| tid)", line).group(1)
        except (IndexError, AttributeError):
            pass

        try:
            # Likely an SBT failure
            failure = " ".join(re.search("(warn|error)\\][ ]+\t*(.+)", line).group(1, 2))
        except (IndexError, AttributeError):
            pass

        if failure:
            failures.append(failure)
        else:  # Give up and append the untouched line
            failures.append(line)

    # Prefer the longest line as it's likely the most informative
    failures.sort(key=len)
    # Prefer errors over warnings
    errors = [f for f in failures if "error" in f.lower()]
    if errors:
        return errors[-1]
    return failures[-1]


def collect_failing_tests(failure_message):
    if failure_message:
        # sbt-ci-wrapper.sh crash path: the message is the single failing "test".
        print(f"Recording CI failure as a failed test: {failure_message}")
        return {failure_message: DEFAULT_CAUSE}
    print("Starting to iterate through generated test reports.")
    failing_tests = iterate_through_test_reports()
    print("Now checking if any log problems were found.")
    failing_tests = check_for_log_failures(failing_tests)
    print(f"Found {len(failing_tests)} different failed tests after slight deduplication.")
    return failing_tests


def self_test():
    test_compute_single_log_failure()
    test_compute_log_failures()
    test_process_test_report_causes()
    test_check_for_log_failures_cause()
    print("parse_failing_tests self-checks passed")


def _write_report(directory: Path, name: str, xml: str) -> Path:
    path = directory / name
    path.write_text(xml)
    return path


def test_process_test_report_causes():
    import tempfile

    regular_xml = (
        '<testsuite name="s"><testcase classname="pkg.RegularFlakyTest" name="t">'
        '<failure type="AssertionError">boom</failure></testcase></testsuite>'
    )
    timeout_xml = (
        '<testsuite name="s"><testcase classname="pkg.HangingTest" name="t">'
        '<failure type="WatchdogTimeout">killed</failure></testcase></testsuite>'
    )
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        causes: Dict[str, str] = {}
        process_test_report(_write_report(tmp_path, "regular.xml", regular_xml), causes)
        process_test_report(_write_report(tmp_path, "timeout.xml", timeout_xml), causes)
        assert causes == {"RegularFlakyTest": "regular", "HangingTest": "timeout"}, causes

        # A regular report for a suite already recorded as a timeout must not downgrade it.
        downgrade_xml = (
            '<testsuite name="s"><testcase classname="pkg.HangingTest" name="t">'
            '<failure type="AssertionError">boom</failure></testcase></testsuite>'
        )
        process_test_report(_write_report(tmp_path, "downgrade.xml", downgrade_xml), causes)
        assert causes["HangingTest"] == "timeout", causes


def test_check_for_log_failures_cause():
    import tempfile

    cases = {
        "ERROR c.d.c.Svc - kaboom": "error_other",
        "WARN  c.d.c.Svc - heads up": "warn_other",
    }
    for line, expected_cause in cases.items():
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                with open("found_problems.txt", "w") as f:
                    f.write(line + "\n")
                result: Dict[str, str] = {}
                check_for_log_failures(result)
            finally:
                os.chdir(cwd)
        assert list(result.values()) == [expected_cause], (line, result)


def test_compute_log_failures():
    # Two suite-bearing lines for one suite plus one for another: keyed per suite,
    # ERROR beats WARN, and the Cause carries the suite name in [X].
    lines = [
        "WARN  c.d.c.p.p.v.ConfirmationResponseFactory:BroadcastPackageUsageIntegrationTest/participant=participant3/synchronizer=da tid:8c53 - Malformed request",
        "ERROR c.d.c.p.a.BroadcastPackageUsageService:BroadcastPackageUsageIntegrationTest/participant=participant3 tid:ef41 - An unexpected exception",
        "WARN  c.d.c.r.DbStorageSingle:BftOrderingGetConnectedSynchronizersIntegrationTest/sequencerx=sequencer1",
    ]
    assert compute_log_failures(lines) == {
        "BroadcastPackageUsageIntegrationTest": "error_other [BroadcastPackageUsageIntegrationTest]",
        "BftOrderingGetConnectedSynchronizersIntegrationTest": (
            "warn_other [BftOrderingGetConnectedSynchronizersIntegrationTest]"
        ),
    }, compute_log_failures(lines)

    # No-suite lines (third-party logger + SBT) collapse into one bare residual entry.
    residual_only = [
        "ERROR i.g.i.ManagedChannelOrphanWrapper - *~*~*~ Channel was not shutdown properly",
        "[error] sbt.librarymanagement.ResolveException: Error downloading io.spray:sbt-revolver",
    ]
    result = compute_log_failures(residual_only)
    assert len(result) == 1, result
    ((_, residual_cause),) = result.items()
    assert residual_cause == "error_other", f"residual cause must be bare: {residual_cause}"

    # A WARN residual whose message merely mentions "error" stays warn_other: the
    # bucket follows the leading log level, not any word in the body.
    warn_with_error_word = ["[warn]  Error downloading io.example:lib, retrying"]
    warn_result = compute_log_failures(warn_with_error_word)
    assert len(warn_result) == 1, warn_result
    ((_, warn_cause),) = warn_result.items()
    assert warn_cause == "warn_other", (
        f"warn line mentioning 'error' must stay warn_other: {warn_cause}"
    )

    # Suite lines mixed with no-suite noise: per-suite entries AND one residual.
    mixed = compute_log_failures(lines + residual_only)
    assert len(mixed) == 3, mixed  # 2 suites + 1 residual
    assert mixed["BroadcastPackageUsageIntegrationTest"].startswith("error_other ["), mixed

    assert compute_log_failures([]) == {}


def test_compute_single_log_failure():
    # Logs
    lines = [
        "ERROR i.g.i.ManagedChannelOrphanWrapper - *~*~*~ Channel ManagedChannelImpl{logId=6714, target=localhost:15272} was not shutdown properly!!! ~*~*~*",
        "2021-05-05 12:43:03,509 [...] WARN  c.d.l.p.s.v.SeedService$ - Trying to gather entropy from the underlying operating system to initialized the contract ID seeding, but the entropy pool seems empty.",
        "WARN  c.d.c.p.p.v.ConfirmationResponseFactory:BroadcastPackageUsageIntegrationTest/participant=participant3/synchronizer=da tid:8c53516be1ff7a431b87322eebf2d4ae - Malformed request RequestId(2021-05-05T11:00:30.588840Z). DAMLeError(Error(Contract could not be found with id ContractId(00c0d9eb114b6eec91c8837bad7975c19e0739e7d25cde0c8b9c4446b0cff1a81fca001220a20c40f20f8a329e3874819a5af2e9107e808e8c091d3dd2a4f7aceff24cfcee)))",
        "ERROR c.d.c.p.a.BroadcastPackageUsageService:BroadcastPackageUsageIntegrationTest/participant=participant3 tid:ef410c4f0c6bbc7664a1cd07f94644d7 - An unexpected exception occurred while updating UsePackage contracts.",
    ]
    expected = "ERROR c.d.c.p.a.BroadcastPackageUsageService"
    actual = compute_single_log_failure(lines)
    assert actual == expected, f"Expected '{expected}', got '{actual}'"

    # SBT failures
    lines = [
        "[warn] ",
        "[warn] 	Note: Some unresolved dependencies have extra attributes.  Check that these dependencies exist with the requested attributes.",
        "[warn] 		io.spray:sbt-revolver:0.9.1 (sbtVersion=1.0, scalaVersion=2.12)",
        "[warn] ",
        "[warn] 	Note: Unresolved dependencies path:",
        "[error] sbt.librarymanagement.ResolveException: Error downloading io.spray:sbt-revolver;sbtVersion=1.0;scalaVersion=2.12:0.9.1",
    ]
    expected = "error sbt.librarymanagement.ResolveException: Error downloading io.spray:sbt-revolver;sbtVersion=1.0;scalaVersion=2.12:0.9.1"
    actual = compute_single_log_failure(lines)
    assert actual == expected, f"Expected '{expected}', got '{actual}'"

    # General
    lines = ["unmatched", "longer unmatched"]
    expected = "longer unmatched"
    actual = compute_single_log_failure(lines)
    assert actual == expected, f"Expected '{expected}', got '{actual}'"


def run(failure_message=None, failing_tests_json=None):
    out_path = failing_tests_json or failing_tests_path()
    failing_tests = collect_failing_tests(failure_message)
    write_failing_tests(out_path, failing_tests)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Collect failing tests into a JSON artifact.")
    parser.add_argument(
        "failure_message",
        nargs="?",
        default=None,
        help="Optional CI failure message recorded as the single failing test (sbt crash path).",
    )
    parser.add_argument(
        "--failing-tests-json",
        default=None,
        help="Output path for the failing-tests JSON (default: shared CI temp dir).",
    )
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.self_test:
        run_guarded_self_test(self_test)
        sys.exit(0)
    run(args.failure_message, args.failing_tests_json)
