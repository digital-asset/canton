#!/usr/bin/env python3
"""
Generates a mock JUnit XML report for tests forcefully killed by the CI watchdog.

Why this is needed:
When a single test hangs, `sbt-ci-wrapper.sh` triggers a timeout. This script parses
the ScalaTest slowpoke log line, safely extracts the suite and test names (handling
commas and XML-special characters such as <, >, &, ", '), emits GitHub Actions
annotations, and generates a mock JUnit XML file.

Where it is used:
Called exclusively by `scripts/ci/sbt-ci-wrapper.sh` upon triggering a timeout.
"""

import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET
from typing import Tuple, Optional


def strip_ansi(line: str) -> str:
    """Removes 7-bit C1 ANSI escape sequences."""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', line)


def parse_log_line(line: str) -> Optional[Tuple[str, str]]:
    """
    Extracts the suite and test name from a ScalaTest slowpoke warning.
    Uses a non-greedy match for the suite name to correctly split at `, test name: `.
    """
    clean_line = strip_ansi(line).strip()
    match = re.search(r"suite\s+name:\s+(.*?),\s+test\s+name:\s+(.*)\.$", clean_line)
    if match:
        return match.group(1), match.group(2)
    return None


def escape_for_gha(text: str) -> str:
    """Escapes strings for GitHub Actions inline annotations."""
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def report_to_gha(suite: str, test: str, time: str) -> None:
    """Prints a GitHub Actions error annotation if running in GHA."""
    if os.environ.get("GITHUB_ACTIONS") == "true":
        gha_suite = escape_for_gha(suite)
        gha_test = escape_for_gha(test)
        print(
            f"::error title=Watchdog Timeout::Hanging Test - Suite: {gha_suite} | Test: {gha_test} ({time}s)",
            flush=True,
        )


def generate_xml(suite: str, test: str, time: str) -> None:
    """
    Creates a JUnit XML file in a mock subproject directory structure.
    Writing to `watchdog-timeout/target/test-reports` ensures the downstream
    CI collectors (like the `cleanup_test_job` step feeding `parse_failing_tests.py`)
    pick it up automatically as if it were a standard SBT subproject.
    """
    report_dir = "watchdog-timeout/target/test-reports"
    os.makedirs(report_dir, exist_ok=True)
    file_path = os.path.join(report_dir, "TEST-watchdog-timeout.xml")

    # Create XML structure (ElementTree handles all escaping automatically)
    testsuite = ET.Element(
        "testsuite", {"name": suite, "tests": "1", "failures": "1", "errors": "0", "time": time}
    )

    testcase = ET.SubElement(
        testsuite, "testcase", {"classname": suite, "name": test, "time": time}
    )

    failure = ET.SubElement(
        testcase,
        "failure",
        {
            "type": "WatchdogTimeout",
            "message": f"Test forcefully terminated by CI watchdog after {time} seconds",
        },
    )
    failure.text = (
        "The SBT process was killed because this test exceeded the MAX_SINGLE_TEST_MINUTES limit."
    )

    tree = ET.ElementTree(testsuite)
    tree.write(file_path, encoding="utf-8", xml_declaration=True)
    print(f"Watchdog XML successfully written to {file_path}")


def self_test() -> None:
    import tempfile

    # --- Test Regex Parsing ---
    # Happy Path: Standard line
    log1 = "[info] *** Test still running after 23 minutes: suite name: Foo, test name: bar."
    assert parse_log_line(log1) == ("Foo", "bar"), "Failed standard parse"

    # Happy Path: Commas in suite and test names
    log2 = "[info] *** Test still running: suite name: My Suite, with commas, test name: My Test, with commas."
    assert parse_log_line(log2) == ("My Suite, with commas", "My Test, with commas"), (
        "Failed comma parse"
    )

    # Happy Path: Special characters
    log3 = "[info] *** Test still running: suite name: <Weird>&, test name: 'Quotes'."
    assert parse_log_line(log3) == ("<Weird>&", "'Quotes'"), "Failed special char parse"

    # Happy Path: ANSI escape codes present in stream (GHA reproduction)
    log_ansi = "\x1b[0m[info]\x1b[0m \x1b[33m*** Test still running: suite name: ANSI Suite, test name: ANSI Test.\x1b[0m\x1b[0m"
    assert parse_log_line(log_ansi) == ("ANSI Suite", "ANSI Test"), "Failed ANSI strip parse"

    # Happy Path: Raw log line from GHA
    log_raw_gha = "2026-08-20T17:52:56.6931789Z \x1b[0m[\x1b[0m\x1b[0minfo\x1b[0m] \x1b[0m\x1b[0m\x1b[33m*** Test still running after 30 minutes, 23 seconds: suite name: DbTeaTrafficStorePostgresTest, test name: TeaTrafficStore should classify a fired statement_timeout as transient. \x1b[0m\x1b[0m"
    assert parse_log_line(log_raw_gha) == (
        "DbTeaTrafficStorePostgresTest",
        "TeaTrafficStore should classify a fired statement_timeout as transient",
    ), "Failed GHA log line parse"

    # Unhappy Path: Missing test name
    assert parse_log_line("[info] *** Test still running: suite name: Foo.") is None, (
        "Should reject missing test name"
    )

    # Unhappy Path: Missing trailing dot
    assert (
        parse_log_line("[info] *** Test still running: suite name: Foo, test name: bar") is None
    ), "Should reject missing trailing dot"

    # Unhappy Path: Completely malformed garbage
    assert parse_log_line("Just some random sbt compiler output") is None, (
        "Should reject random logs"
    )
    assert parse_log_line("") is None, "Should reject empty string"

    # --- Test GHA Escaping ---
    assert escape_for_gha("Line1\nLine2\r%") == "Line1%0ALine2%0D%25", "Failed GHA escaping"

    # --- Test XML Generation ---
    with tempfile.TemporaryDirectory() as tmpdir:
        original_cwd = os.getcwd()
        os.chdir(tmpdir)
        try:
            xml_path = "watchdog-timeout/target/test-reports/TEST-watchdog-timeout.xml"

            # XML special characters
            generate_xml("com.package.weird & <suite> name", "test 'with' \"quotes\"", "123")
            assert os.path.exists(xml_path), "XML file was not created"

            root = ET.parse(xml_path).getroot()
            assert root.attrib["name"] == "com.package.weird & <suite> name"
            assert root.find("testcase").attrib["name"] == "test 'with' \"quotes\""
            assert root.find("testcase").attrib["time"] == "123"

            # Empty strings
            generate_xml("", "", "")
            root = ET.parse(xml_path).getroot()
            assert root.attrib["name"] == ""
            assert root.find("testcase").attrib["name"] == ""
            assert root.find("testcase").attrib["time"] == ""

            # Unicode and Emojis
            generate_xml("IntegrationTest 🚀", "Handles 漢字 correctly", "42")
            root = ET.parse(xml_path).getroot()
            assert root.attrib["name"] == "IntegrationTest 🚀"
            assert root.find("testcase").attrib["name"] == "Handles 漢字 correctly"

            # Newlines and Tabs
            generate_xml("Suite\nLine2", "Test\tTabbed\nName", "99")
            root = ET.parse(xml_path).getroot()
            assert root.attrib["name"] == "Suite\nLine2"
            assert root.find("testcase").attrib["name"] == "Test\tTabbed\nName"

        finally:
            os.chdir(original_cwd)

    print("generate_watchdog_xml self-checks passed")


def test_generate_watchdog_xml() -> None:
    """Wrapper for pytest to collect and execute the self_test function."""
    self_test()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mock JUnit XML for a watchdog timeout.")
    parser.add_argument("--log-line", required=False, help="The raw ScalaTest slowpoke log line.")
    parser.add_argument("--time", required=False, help="Elapsed time in seconds.")
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit.")

    args = parser.parse_args()

    if args.self_test:
        self_test()
        sys.exit(0)

    if not (args.log_line and args.time):
        print(
            "Error: --log-line and --time are required unless running --self-test",
            file=sys.stderr,
        )
        sys.exit(1)

    parsed = parse_log_line(args.log_line)
    if not parsed:
        print("Error: Could not parse suite/test name from log line.", file=sys.stderr)
        sys.exit(1)

    suite_name, test_name = parsed
    report_to_gha(suite_name, test_name, args.time)
    generate_xml(suite_name, test_name, args.time)
