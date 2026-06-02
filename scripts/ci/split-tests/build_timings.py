#!/usr/bin/env python3
import argparse
import glob
import json
import os
import sys
import tempfile
import xml.etree.ElementTree as ET


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--junit-glob", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args(argv)


def testcase_key(tc):
    # Use only classname so keys match the class-level entries in split_tests.py
    classname = (tc.attrib.get("classname") or "").strip()
    return classname


def parse_junit_file(path, timings):
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return

    for tc in root.iter("testcase"):
        key = testcase_key(tc)
        if not key:
            continue
        try:
            duration = float(tc.attrib.get("time") or 0.0)
        except ValueError:
            duration = 0.0
        timings[key] = timings.get(key, 0.0) + duration


def collect_junit_paths(junit_glob):
    return sorted(glob.glob(junit_glob, recursive=True))


def build_timings(paths):
    timings = {}
    for path in paths:
        if os.path.isfile(path):
            parse_junit_file(path, timings)
    return timings


def ensure_output_dir(output_path):
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)


def write_timings(output_path, timings):
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(timings, f, indent=2, sort_keys=True)


def test_testcase_key_uses_classname():
    tc = ET.Element("testcase", attrib={"classname": "  suite.Foo  ", "name": "x"})
    assert testcase_key(tc) == "suite.Foo"


def test_parse_junit_file_aggregates_durations():
    xml = """<testsuite>
      <testcase classname="suite.A" name="a1" time="1.5"/>
      <testcase classname="suite.A" name="a2" time="2.0"/>
      <testcase classname="suite.B" name="b" time="3"/>
    </testsuite>"""
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
        f.write(xml)
        path = f.name
    try:
        timings = {}
        parse_junit_file(path, timings)
        assert timings["suite.A"] == 3.5
        assert timings["suite.B"] == 3.0
    finally:
        os.remove(path)


def test_parse_junit_file_ignores_invalid_xml():
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
        f.write("<testsuite><testcase")
        path = f.name
    try:
        timings = {"suite.A": 1.0}
        parse_junit_file(path, timings)
        assert timings == {"suite.A": 1.0}
    finally:
        os.remove(path)


def test_build_timings_skips_missing_paths():
    timings = build_timings(["/tmp/does-not-exist-report.xml"])
    assert timings == {}


def test_write_timings_and_ensure_output_dir():
    with tempfile.TemporaryDirectory() as tmp:
        output_path = os.path.join(tmp, "nested", "timings.json")
        ensure_output_dir(output_path)
        write_timings(output_path, {"suite.A": 2.0})
        with open(output_path, encoding="utf-8") as f:
            payload = json.load(f)
        assert payload == {"suite.A": 2.0}


def self_test():
    test_testcase_key_uses_classname()
    test_parse_junit_file_aggregates_durations()
    test_parse_junit_file_ignores_invalid_xml()
    test_build_timings_skips_missing_paths()
    test_write_timings_and_ensure_output_dir()
    print("All self-checks passed")


def main():
    args = parse_args()
    paths = collect_junit_paths(args.junit_glob)
    timings = build_timings(paths)
    ensure_output_dir(args.output)
    write_timings(args.output, timings)


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        self_test()
    else:
        main()
