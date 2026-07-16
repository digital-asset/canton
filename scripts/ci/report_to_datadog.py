#!/usr/bin/env python3
"""Sends one count metric per failing test to Datadog.

Reads failing_tests.json (written by parse_failing_tests.py) and sends one count
metric per failing test to Datadog. Not gated on the branch: Datadog metrics are
collected for every branch.

Usage:
  report_to_datadog.py [--failing-tests-json PATH] [--self-test]
"""

import argparse
import os
import sys

from flaky_common import (
    metric_short_version,
    branch,
    format_test_name,
    get_ci_build_url,
    get_ci_node_index,
    get_ci_job_name,
    read_failing_tests,
    failing_tests_path,
    assert_required_env_vars,
    ci_required_context_vars,
    run_guarded_self_test,
)


def report_to_datadog(metric_name: str, test_name: str):
    from datadog import initialize, api

    api_key = os.environ.get('DATADOG_API_KEY', '').strip()
    if not api_key:
        raise RuntimeError("DATADOG_API_KEY is empty or missing")
    options = {'api_key': api_key, 'api_host': 'https://api.datadoghq.com/'}
    initialize(**options)
    send_args = {
        'metric': metric_name,
        'type': 'count',
        'points': 1,
        'tags': [
            f"name:{format_test_name(test_name)}",
            f"branch:{branch}",
            f"url:{get_ci_build_url()}",
            f"container_index:{get_ci_node_index()}",
            f"job:{get_ci_job_name()}",
        ],
    }
    resp = api.Metric.send(**send_args)
    if resp.get('status') != 'ok':
        print(f"Received error response while reporting test '{test_name}': \n {resp}")
        resp2 = api.Metric.send(**send_args)
        if resp2.get('status') != 'ok':
            raise Exception(f"Failed to report test '{test_name}' to Datadog after retry: {resp2}")
        print(f"Received following response upon retrying: \n {resp2}")


def self_test():
    # No module-specific unit tests yet. Shared helpers are covered by flaky_common.
    print("report_to_datadog self-checks passed")


def run(failing_tests_json=None):
    in_path = failing_tests_json or failing_tests_path()
    failing_tests = read_failing_tests(in_path)
    if not failing_tests:
        print("No failing tests to report to Datadog.")
        return

    assert_required_env_vars(['DATADOG_API_KEY'] + ci_required_context_vars())

    print(f"Reporting {len(failing_tests)} failed test(s) to datadog")
    for test_name in failing_tests:
        report_to_datadog(metric_short_version, test_name)
    print("Finished reporting failed tests to datadog.")


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Report failing tests to Datadog.")
    parser.add_argument(
        "--failing-tests-json",
        default=None,
        help="Input path for the failing-tests JSON (default: shared CI temp dir).",
    )
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.self_test:
        run_guarded_self_test(self_test)
        sys.exit(0)
    run(args.failing_tests_json)
