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
from typing import Set
from pathlib import Path

from flaky_common import (
    write_failing_tests,
    failing_tests_path,
    run_guarded_self_test,
)


# sbt generates test-reports in junit xml style that we aggregate into test-reports/<subproject>/<reports> in CircleCI
# through the CircleCI command `upload_test_reports`
# This methods reads those test reports to find out which tests failed
# see also https://www.scala-sbt.org/1.x/docs/Testing.html#Test+Reports
def iterate_through_test_reports():
    failing_tests: Set[str] = set()
    for entry in Path('./test-reports').rglob("*.xml"):
        if entry.is_file():
            process_test_report(entry, failing_tests)
    return failing_tests


def process_test_report(path: Path, failing_tests: Set[str]):
    tree: ElementTree = ET.parse(path)
    root: Element = tree.getroot()
    # To understand this XML parsing I recommend to just go through this code with a debugger on
    # an example test report; alternatively see e.g. https://stackoverflow.com/a/26661423
    for child in root:
        if 'name' not in child.attrib: continue
        contains_fail = any(childchild.tag == 'failure' for childchild in child)
        contains_error = any(childchild.tag == 'error' for childchild in child)
        if contains_fail or contains_error:
            # Example value: LedgerAPIParticipantPruningTestPostgres
            test_name = child.attrib.get('classname', child.attrib.get('name', 'unknown')).split('.')[-1]
            print(f"Found failing test '{test_name}'")
            failing_tests.add(test_name)
    return failing_tests


def check_for_log_failures(failing_tests_result: Set[str]):
    failure = None
    if os.path.exists("found_problems.txt"):
        with open("found_problems.txt", "r") as f:
            lines = f.read().splitlines() # splitlines() strips trailing newlines unlike readlines()
            failure = compute_single_log_failure(lines)

    if failure:
        print(f"Reporting following failure to datadog: '{failure}'")
        failing_tests_result.add(failure)

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
        else: # Give up and append the untouched line
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
        return {failure_message}
    print("Starting to iterate through generated test reports.")
    failing_tests = iterate_through_test_reports()
    print("Now checking if any log problems were found.")
    failing_tests = check_for_log_failures(failing_tests)
    print(f"Found {len(failing_tests)} different failed tests after slight deduplication.")
    return failing_tests


def self_test():
    test_compute_single_log_failure()
    print("parse_failing_tests self-checks passed")


def test_compute_single_log_failure():
    # Logs
    lines = [
        "ERROR i.g.i.ManagedChannelOrphanWrapper - *~*~*~ Channel ManagedChannelImpl{logId=6714, target=localhost:15272} was not shutdown properly!!! ~*~*~*",
        "2021-05-05 12:43:03,509 [...] WARN  c.d.l.p.s.v.SeedService$ - Trying to gather entropy from the underlying operating system to initialized the contract ID seeding, but the entropy pool seems empty.",
        "WARN  c.d.c.p.p.v.ConfirmationResponseFactory:BroadcastPackageUsageIntegrationTest/participant=participant3/synchronizer=da tid:8c53516be1ff7a431b87322eebf2d4ae - Malformed request RequestId(2021-05-05T11:00:30.588840Z). DAMLeError(Error(Contract could not be found with id ContractId(00c0d9eb114b6eec91c8837bad7975c19e0739e7d25cde0c8b9c4446b0cff1a81fca001220a20c40f20f8a329e3874819a5af2e9107e808e8c091d3dd2a4f7aceff24cfcee)))",
        "ERROR c.d.c.p.a.BroadcastPackageUsageService:BroadcastPackageUsageIntegrationTest/participant=participant3 tid:ef410c4f0c6bbc7664a1cd07f94644d7 - An unexpected exception occurred while updating UsePackage contracts."
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
        "[error] sbt.librarymanagement.ResolveException: Error downloading io.spray:sbt-revolver;sbtVersion=1.0;scalaVersion=2.12:0.9.1"
    ]
    expected = "error sbt.librarymanagement.ResolveException: Error downloading io.spray:sbt-revolver;sbtVersion=1.0;scalaVersion=2.12:0.9.1"
    actual = compute_single_log_failure(lines)
    assert actual == expected, f"Expected '{expected}', got '{actual}'"

    # General
    lines = [
        "unmatched",
        "longer unmatched"
    ]
    expected = "longer unmatched"
    actual = compute_single_log_failure(lines)
    assert actual == expected, f"Expected '{expected}', got '{actual}'"


def run(failure_message=None, failing_tests_json=None):
    out_path = failing_tests_json or failing_tests_path()
    failing_tests = collect_failing_tests(failure_message)
    write_failing_tests(out_path, failing_tests)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Collect failing tests into a JSON artifact.")
    parser.add_argument("failure_message", nargs="?", default=None,
                        help="Optional CI failure message recorded as the single failing test (sbt crash path).")
    parser.add_argument("--failing-tests-json", default=None,
                        help="Output path for the failing-tests JSON (default: shared CI temp dir).")
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.self_test:
        run_guarded_self_test(self_test)
        sys.exit(0)
    run(args.failure_message, args.failing_tests_json)
