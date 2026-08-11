#!/usr/bin/env python3

import argparse
import glob
import json
import os
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Merge first-run and failed-only rerun JUnit results into one "
            "deterministic JUnit file. Rerun testcases replace matching "
            "first-run testcases by classname+name."
        )
    )
    parser.add_argument(
        "--first-run-glob", required=True, help="Glob for first-run JUnit XML files."
    )
    parser.add_argument("--rerun-glob", required=True, help="Glob for rerun JUnit XML files.")
    parser.add_argument(
        "--output-dir", required=True, help="Output directory for merged JUnit XML."
    )
    parser.add_argument(
        "--metadata-json",
        default="",
        help="Optional path for merge metadata JSON (rerun used, rerun classes, counts).",
    )
    return parser.parse_args()


def discover_files(pattern):
    return sorted(path for path in glob.glob(pattern, recursive=True) if os.path.isfile(path))


def testcase_key(testcase, path, index):
    classname = testcase.attrib.get("classname", "").strip()
    name = testcase.attrib.get("name", "").strip()
    if classname or name:
        return f"{classname}::{name}"
    return f"__anonymous__::{path}::{index}"


def testcase_status(testcase):
    if testcase.find("error") is not None:
        return "error"
    if testcase.find("failure") is not None:
        return "failure"
    if testcase.find("skipped") is not None:
        return "skipped"
    return "passed"


def serialize_testcase(testcase):
    status = testcase_status(testcase)
    detail_elem = None
    if status == "error":
        detail_elem = testcase.find("error")
    elif status == "failure":
        detail_elem = testcase.find("failure")
    elif status == "skipped":
        detail_elem = testcase.find("skipped")

    # Deliberately reduced JUnit representation: keep only testcase attrs + status detail needed for merge/summary.
    detail = None
    if detail_elem is not None:
        detail = {
            "tag": detail_elem.tag,
            "attrib": dict(detail_elem.attrib),
            "text": detail_elem.text or "",
        }

    return {
        "attrs": dict(testcase.attrib),
        "status": status,
        "detail": detail,
    }


def parse_testcases(paths):
    by_key = {}
    classnames = set()
    parse_errors = 0

    for path in paths:
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:
            parse_errors += 1
            continue

        for index, testcase in enumerate(root.findall(".//testcase")):
            key = testcase_key(testcase, path, index)
            record = serialize_testcase(testcase)
            by_key[key] = record

            classname = record["attrs"].get("classname", "").strip()
            if classname:
                classnames.add(classname)

    return by_key, sorted(classnames), parse_errors


def list_not_passed_tests(records):
    tests = []
    seen = set()
    for key in sorted(records.keys()):
        record = records[key]
        if record["status"] not in {"failure", "error"}:
            continue

        classname = record["attrs"].get("classname", "").strip()
        name = record["attrs"].get("name", "").strip()
        full_name = f"{classname}.{name}" if classname else name
        if full_name and full_name not in seen:
            seen.add(full_name)
            tests.append(full_name)

    return tests


def write_merged_junit(records, output_file):
    root = ET.Element("testsuite")

    failures = 0
    errors = 0
    skipped = 0

    for key in sorted(records.keys()):
        record = records[key]
        testcase = ET.SubElement(root, "testcase", record["attrs"])

        status = record["status"]
        if status == "failure":
            failures += 1
        elif status == "error":
            errors += 1
        elif status == "skipped":
            skipped += 1

        detail = record["detail"]
        if detail is not None:
            detail_elem = ET.SubElement(testcase, detail["tag"], detail["attrib"])
            detail_elem.text = detail["text"]

    total = len(records)
    root.attrib.update(
        {
            "name": "merged-junit-results",
            "tests": str(total),
            "failures": str(failures),
            "errors": str(errors),
            "skipped": str(skipped),
        }
    )

    tree = ET.ElementTree(root)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    tree.write(output_file, encoding="utf-8", xml_declaration=True)


def self_test():
    test_parse_testcases_collects_classnames_and_failures()
    test_parse_testcases_counts_parse_errors()
    test_rerun_records_override_first_run_records()
    test_write_merged_junit_sets_summary_counts()
    print("All self-checks passed")


def _write_junit_file(path, testcases):
    root = ET.Element("testsuite", name="suite", tests=str(len(testcases)))
    for attrs, detail in testcases:
        testcase = ET.SubElement(root, "testcase", attrs)
        if detail is not None:
            detail_tag, detail_text = detail
            elem = ET.SubElement(testcase, detail_tag)
            elem.text = detail_text
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def test_parse_testcases_collects_classnames_and_failures():
    with tempfile.TemporaryDirectory() as tmp:
        report_path = os.path.join(tmp, "TEST-one.xml")
        _write_junit_file(
            report_path,
            [
                ({"classname": "com.example.Foo", "name": "testA"}, None),
            ],
        )

        records, classnames, parse_errors = parse_testcases([report_path])

        assert classnames == ["com.example.Foo"], f"Unexpected classnames: {classnames!r}"
        assert len(records) == 1, f"Unexpected records size: {len(records)!r}"
        assert parse_errors == 0, f"Unexpected parse_errors: {parse_errors!r}"
        not_passed = list_not_passed_tests(records)
        assert not not_passed, f"Expected no failed tests, got: {not_passed!r}"


def test_parse_testcases_counts_parse_errors():
    with tempfile.TemporaryDirectory() as tmp:
        valid_path = os.path.join(tmp, "TEST-valid.xml")
        invalid_path = os.path.join(tmp, "TEST-invalid.xml")

        _write_junit_file(valid_path, [({"classname": "com.example.Foo", "name": "testA"}, None)])
        with open(invalid_path, "w", encoding="utf-8") as out:
            out.write("<testsuite><testcase")

        records, classnames, parse_errors = parse_testcases([valid_path, invalid_path])

        assert len(records) == 1, f"Unexpected records size: {len(records)!r}"
        assert classnames == ["com.example.Foo"], f"Unexpected classnames: {classnames!r}"
        assert parse_errors == 1, f"Expected 1 parse error, got: {parse_errors!r}"


def test_rerun_records_override_first_run_records():
    with tempfile.TemporaryDirectory() as tmp:
        first_path = os.path.join(tmp, "TEST-first.xml")
        rerun_path = os.path.join(tmp, "TEST-rerun.xml")

        _write_junit_file(
            first_path,
            [
                (
                    {"classname": "com.example.Foo", "name": "testA"},
                    ("failure", "first run failed"),
                )
            ],
        )
        _write_junit_file(
            rerun_path,
            [({"classname": "com.example.Foo", "name": "testA"}, None)],
        )

        first_records, _, _ = parse_testcases([first_path])
        rerun_records, _, _ = parse_testcases([rerun_path])
        merged_records = dict(first_records)
        merged_records.update(rerun_records)

        key = "com.example.Foo::testA"
        assert merged_records[key]["status"] == "passed", (
            "Expected rerun record to override first-run failure"
        )


def test_write_merged_junit_sets_summary_counts():
    with tempfile.TemporaryDirectory() as tmp:
        output_file = os.path.join(tmp, "merged", "TEST-merged.xml")
        records = {
            "a": {
                "attrs": {"classname": "a.C", "name": "pass"},
                "status": "passed",
                "detail": None,
            },
            "b": {
                "attrs": {"classname": "a.C", "name": "fail"},
                "status": "failure",
                "detail": {"tag": "failure", "attrib": {}, "text": "boom"},
            },
            "c": {
                "attrs": {"classname": "a.C", "name": "skip"},
                "status": "skipped",
                "detail": {"tag": "skipped", "attrib": {}, "text": "n/a"},
            },
        }

        write_merged_junit(records, output_file)
        root = ET.parse(output_file).getroot()

        assert root.attrib.get("tests") == "3", f"Unexpected tests count: {root.attrib!r}"
        assert root.attrib.get("failures") == "1", f"Unexpected failures count: {root.attrib!r}"
        assert root.attrib.get("errors") == "0", f"Unexpected errors count: {root.attrib!r}"
        assert root.attrib.get("skipped") == "1", f"Unexpected skipped count: {root.attrib!r}"


def main():
    self_test()
    args = parse_args()

    first_run_files = discover_files(args.first_run_glob)
    rerun_files = discover_files(args.rerun_glob)

    if not first_run_files and not rerun_files:
        print(
            "::warning::No JUnit XML files found for both globs "
            f"(first-run: {args.first_run_glob!r}, rerun: {args.rerun_glob!r}). "
            "Merged report may be empty."
        )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    merged_junit_path = output_dir / "TEST-merged.xml"
    metadata_path = args.metadata_json or str(output_dir / "merge-metadata.json")

    first_run_records, _, first_run_parse_errors = parse_testcases(first_run_files)
    if first_run_parse_errors > 0:
        print(
            "::warning::Failed to parse some first-run JUnit XML files "
            f"(parse_errors={first_run_parse_errors}, files={len(first_run_files)}). "
            "Some testcases may be missing from merged output."
        )
    first_run_not_passed_tests = list_not_passed_tests(first_run_records)

    # Fast path: no rerun files, so merged output is first-run only.
    if not rerun_files:
        write_merged_junit(first_run_records, str(merged_junit_path))
        metadata = {
            "rerun_used": False,
            "first_run_files": len(first_run_files),
            "rerun_files": 0,
            "first_run_testcases": len(first_run_records),
            "rerun_testcases": 0,
            "merged_testcases": len(first_run_records),
            "rerun_classes": [],
            "first_run_not_passed_tests": first_run_not_passed_tests,
            "first_run_parse_errors": first_run_parse_errors,
            "rerun_parse_errors": 0,
            "parse_errors": first_run_parse_errors,
        }
        with open(metadata_path, "w", encoding="utf-8") as out:
            json.dump(metadata, out, ensure_ascii=True, indent=2)
        print(
            f"Merged JUnit results written to {merged_junit_path} "
            "(rerun_used=False, merged_testcases="
            f"{metadata['merged_testcases']})"
        )
        return

    rerun_records, rerun_classes, rerun_parse_errors = parse_testcases(rerun_files)
    if rerun_parse_errors > 0:
        print(
            "::warning::Failed to parse some rerun JUnit XML files "
            f"(parse_errors={rerun_parse_errors}, files={len(rerun_files)}). "
            "Some rerun testcases may be missing from merged output."
        )
    merged_records = dict(first_run_records)
    merged_records.update(rerun_records)

    write_merged_junit(merged_records, str(merged_junit_path))

    metadata = {
        "rerun_used": bool(rerun_files),
        "first_run_files": len(first_run_files),
        "rerun_files": len(rerun_files),
        "first_run_testcases": len(first_run_records),
        "rerun_testcases": len(rerun_records),
        "merged_testcases": len(merged_records),
        "rerun_classes": rerun_classes,
        "first_run_not_passed_tests": first_run_not_passed_tests,
        "first_run_parse_errors": first_run_parse_errors,
        "rerun_parse_errors": rerun_parse_errors,
        "parse_errors": first_run_parse_errors + rerun_parse_errors,
    }

    with open(metadata_path, "w", encoding="utf-8") as out:
        json.dump(metadata, out, ensure_ascii=True, indent=2)

    print(
        f"Merged JUnit results written to {merged_junit_path} "
        f"(rerun_used={metadata['rerun_used']}, "
        f"merged_testcases={metadata['merged_testcases']})"
    )


if __name__ == "__main__":
    main()
