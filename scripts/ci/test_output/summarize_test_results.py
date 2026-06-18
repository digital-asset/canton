#!/usr/bin/env python3
import argparse
import glob
import html
import json
import os
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Summarize JUnit XML results for a single test shard and optionally write JSON output."
    )
    parser.add_argument("--shard-index", type=int, required=True, help="Index of the current shard.")
    parser.add_argument("--total-shards", type=int, required=True, help="Total number of shards in the test run.")
    parser.add_argument("--junit-glob", required=True, help="Glob pattern used to find JUnit XML files.")
    parser.add_argument(
        "--summary-path",
        default="",
        help="Path to append markdown summary to. If empty, uses GITHUB_STEP_SUMMARY when set.",
    )
    parser.add_argument(
        "--json-output",
        default="",
        help="Optional path to write JSON summary payload.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of not-passed tests to list in the markdown details section.",
    )
    return parser.parse_args()


def gather_results(paths):
    passed = 0
    failures = 0
    errors = 0
    skipped = 0
    parse_errors = 0
    failed_tests = []
    errored_tests = []
    not_passed_tests = []

    for path in paths:
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            parse_errors += 1
            continue

        for testcase in root.findall(".//testcase"):
            classname = testcase.attrib.get("classname", "")
            name = testcase.attrib.get("name", "")
            full_name = f"{classname}.{name}" if classname else name

            if testcase.find("error") is not None:
                errors += 1
                if full_name:
                    errored_tests.append(full_name)
                    not_passed_tests.append(full_name)
            elif testcase.find("failure") is not None:
                failures += 1
                if full_name:
                    failed_tests.append(full_name)
                    not_passed_tests.append(full_name)
            elif testcase.find("skipped") is not None:
                skipped += 1
            else:
                passed += 1

    total = passed + failures + errors + skipped
    unique_failed = len(dict.fromkeys(failed_tests))
    unique_errors = len(dict.fromkeys(errored_tests))
    unique_not_passed = list(dict.fromkeys(not_passed_tests))

    return {
        "passed": passed,
        "failures": failures,
        "failures_unique": unique_failed,
        "errors": errors,
        "errors_unique": unique_errors,
        "skipped": skipped,
        "parse_errors": parse_errors,
        "total": total,
        "not_passed_tests": unique_not_passed,
    }


def build_summary(args, files, results):
    failures_unique = results.get("failures_unique", results["failures"])
    errors_unique = results.get("errors_unique", results["errors"])

    lines = []
    lines.append(f"## Test summary (shard {args.shard_index}/{args.total_shards})")
    lines.append("")
    lines.append(f"- JUnit files found: {len(files)}")
    lines.append(f"- Total: {results['total']}")
    lines.append(f"- Passed: {results['passed']}")
    lines.append(f"- Failed: {results['failures']} ({failures_unique} unique)")
    lines.append(f"- Errors: {results['errors']} ({errors_unique} unique)")
    lines.append(f"- Skipped: {results['skipped']}")
    if results["parse_errors"]:
        lines.append(f"- XML parse errors: {results['parse_errors']}")
    lines.append("")

    lines.append("### Not passed tests")
    if results["not_passed_tests"]:
        total_not_passed = len(results["not_passed_tests"])
        shown_not_passed = min(total_not_passed, args.limit)
        lines.append(
            f"<details><summary>Show failed and error tests ({total_not_passed}, shown {shown_not_passed})</summary>"
        )
        lines.append("")
        for test_name in results["not_passed_tests"][: args.limit]:
            lines.append(f"- {html.escape(test_name)}")
        if total_not_passed > args.limit:
            remaining = total_not_passed - args.limit
            lines.append(f"- and {remaining} more")
        lines.append("")
        lines.append("</details>")
    else:
        lines.append("- none")

    return "\n".join(lines) + "\n"


def main():
    args = parse_args()
    files = sorted(glob.glob(args.junit_glob, recursive=True))
    files = [path for path in files if os.path.isfile(path)]

    results = gather_results(files)
    summary = build_summary(args, files, results)

    print(summary)

    summary_path = args.summary_path or os.environ.get("GITHUB_STEP_SUMMARY", "")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as out:
            out.write(summary)

    if args.json_output:
        json_output_path = Path(args.json_output)
        json_output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "shard_index": args.shard_index,
            "total_shards": args.total_shards,
            "junit_files": len(files),
            "results": results,
        }
        with open(json_output_path, "w", encoding="utf-8") as out:
            json.dump(payload, out, ensure_ascii=True, indent=2)


def self_test():
    test_gather_results_counts()
    test_gather_results_deduplicates()
    test_gather_results_skips_empty_full_name()
    test_gather_results_handles_parse_error()
    test_build_summary_no_failures()
    test_build_summary_with_failures()
    test_build_summary_limit()
    print("All self-checks passed")


def _write_junit(tmp_path, content):
    path = os.path.join(tmp_path, "junit.xml")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def test_gather_results_counts():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_junit(tmp, """
        <testsuite>
          <testcase classname="com.example.Foo" name="testA"/>
          <testcase classname="com.example.Foo" name="testB"><failure>boom</failure></testcase>
          <testcase classname="com.example.Foo" name="testC"><error>oops</error></testcase>
          <testcase classname="com.example.Foo" name="testD"><skipped/></testcase>
        </testsuite>
        """)
        r = gather_results([path])
        assert r["passed"] == 1, f"Expected 1 passed, got {r['passed']}"
        assert r["failures"] == 1, f"Expected 1 failure, got {r['failures']}"
        assert r["failures_unique"] == 1, f"Expected 1 unique failure, got {r['failures_unique']}"
        assert r["errors"] == 1, f"Expected 1 error, got {r['errors']}"
        assert r["errors_unique"] == 1, f"Expected 1 unique error, got {r['errors_unique']}"
        assert r["skipped"] == 1, f"Expected 1 skipped, got {r['skipped']}"
        assert r["total"] == 4, f"Expected total 4, got {r['total']}"
        assert r["parse_errors"] == 0, f"Expected 0 parse errors, got {r['parse_errors']}"


def test_gather_results_deduplicates():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_junit(tmp, """
        <testsuite>
          <testcase classname="com.example.Foo" name="testA"><failure>boom</failure></testcase>
          <testcase classname="com.example.Foo" name="testA"><failure>boom again</failure></testcase>
        </testsuite>
        """)
        r = gather_results([path])
        assert r["failures"] == 2, f"Expected 2 failures counted, got {r['failures']}"
        assert r["failures_unique"] == 1, f"Expected 1 unique failure, got {r['failures_unique']}"
        assert len(r["not_passed_tests"]) == 1, f"Expected 1 unique not-passed test after dedup, got {r['not_passed_tests']}"


def test_gather_results_skips_empty_full_name():
    with tempfile.TemporaryDirectory() as tmp:
        # testcase with no classname and no name produces empty full_name
        path = _write_junit(tmp, """
        <testsuite>
          <testcase><failure>no name</failure></testcase>
          <testcase classname="com.example.Foo" name="testA"><failure>named</failure></testcase>
        </testsuite>
        """)
        r = gather_results([path])
        assert r["failures"] == 2, f"Expected 2 failures counted, got {r['failures']}"
        assert r["not_passed_tests"] == ["com.example.Foo.testA"], \
            f"Expected only named test in not_passed_tests, got {r['not_passed_tests']}"


def test_gather_results_handles_parse_error():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_junit(tmp, "not valid xml <<<<<")
        r = gather_results([path])
        assert r["parse_errors"] == 1, f"Expected 1 parse error, got {r['parse_errors']}"
        assert r["total"] == 0, f"Expected total 0 for unparseable file, got {r['total']}"


def _make_args(shard_index="1", total_shards="4", limit=100):
    import types
    args = types.SimpleNamespace(
        shard_index=shard_index,
        total_shards=total_shards,
        limit=limit,
    )
    return args


def test_build_summary_no_failures():
    args = _make_args()
    results = {"passed": 5, "failures": 0, "failures_unique": 0,
               "errors": 0, "errors_unique": 0, "skipped": 1,
               "parse_errors": 0, "total": 6, "not_passed_tests": []}
    summary = build_summary(args, ["a.xml", "b.xml"], results)
    assert "## Test summary (shard 1/4)" in summary, f"Missing header in: {summary}"
    assert "- JUnit files found: 2" in summary
    assert "- Total: 6" in summary
    assert "- none" in summary, "Expected '- none' when no failures"
    assert "<details>" not in summary, "Should not have details block when no failures"


def test_build_summary_with_failures():
    args = _make_args()
    results = {"passed": 3, "failures": 2, "failures_unique": 2,
               "errors": 0, "errors_unique": 0, "skipped": 0,
               "parse_errors": 0, "total": 5,
               "not_passed_tests": ["com.example.Foo.testA", "com.example.Bar.testB"]}
    summary = build_summary(args, [], results)
    assert "<details>" in summary, "Expected details block when failures present"
    assert "- com.example.Foo.testA" in summary
    assert "- com.example.Bar.testB" in summary


def test_build_summary_limit():
    args = _make_args(limit=2)
    not_passed = [f"com.example.Test{i}.test" for i in range(5)]
    results = {"passed": 0, "failures": 5, "failures_unique": 5,
               "errors": 0, "errors_unique": 0, "skipped": 0,
               "parse_errors": 0, "total": 5, "not_passed_tests": not_passed}
    summary = build_summary(args, [], results)
    assert "and 3 more" in summary, f"Expected 'and 3 more' in: {summary}"
    assert "shown 2" in summary, f"Expected 'shown 2' in: {summary}"


if __name__ == "__main__":
    self_test()
    main()
