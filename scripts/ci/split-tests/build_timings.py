#!/usr/bin/env python3
import argparse
import glob
import json
import os
import sys
import tempfile
import re
import xml.etree.ElementTree as ET
from datetime import datetime


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--junit-glob", required=True)
    parser.add_argument("--output", required=True)
    # [TEST ONLY] Suite-wall weights (see build_suite_wall_timings). Both are optional; without them
    # the output is the per-class sum of <testcase> times, as before.
    parser.add_argument(
        "--suite-junit-glob",
        default="",
        help="Per-suite JUnit reports (first run). Classes whose reports have tests=\"0\" get weight 0, "
        "and the reports map LogReporter suite names to class names.",
    )
    parser.add_argument(
        "--canton-log-glob",
        default="",
        help="canton_test logs. A suite's weight becomes its wall time (LogReporter start to completion) "
        "minus the time it queued in ConcurrentEnvironmentLimiter.",
    )
    return parser.parse_args(argv)


def testcase_key(tc):
    # Use only classname so keys match the class-level entries in split_tests.py
    classname = (tc.attrib.get("classname") or "").strip()
    return classname


def parse_junit_file(path, timings):
    try:
        root = ET.parse(path).getroot()
    except (
        ET.ParseError
    ):  # silent-ok: an unreadable report adds no timings, merge_timings fails if none are usable
        return

    for tc in root.iter("testcase"):
        key = testcase_key(tc)
        if not key:
            continue
        try:
            duration = float(tc.attrib.get("time") or 0.0)
        except ValueError:  # silent-ok: a malformed time attribute only skews one test's weight
            duration = 0.0
        # sbt's JUnit listener writes -1ms (-0.001s) for test events whose
        # duration the framework did not report. Left as-is those sentinels
        # accumulate into a small negative per-class total, so clamp them to
        # zero instead of summing the noise.
        duration = max(duration, 0.0)
        timings[key] = timings.get(key, 0.0) + duration


def collect_junit_paths(junit_glob):
    return sorted(glob.glob(junit_glob, recursive=True))


def build_timings(paths):
    timings = {}
    for path in paths:
        if os.path.isfile(path):
            parse_junit_file(path, timings)
    return timings


# [TEST ONLY] Suite-wall weights.
#
# A <testcase> time sum leaves out what happens outside test bodies: starting the shared Canton
# environment in beforeAll, teardown, and time between tests. In the PV37 lane that is most of a
# suite's wall time, so shards balanced on test-case sums are unbalanced in wall time. Classes whose
# tests are all excluded at run time (tags) or that have no test cases never get a <testcase> time and
# would be weighted at the splitter's fallback every run; they get 0 here.
#
# The weight of a suite seen in the canton test log is its wall time minus its environment-limiter queue
# time: the queue depends on what else runs in the shard, not on the suite.
_LOG_TS = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}) ")
_SUITE_START = re.compile(r"LogReporter\S* - Starting test suite '([^']+)'")
_SUITE_END = re.compile(r"LogReporter\S* - (?:Completed|Aborted) test suite '([^']+)'")
_LIMITER = re.compile(r"ConcurrentEnvironmentLimiter\$ - (\S+): (\w+) => (\w+) after")


def _log_time(line):
    match = _LOG_TS.match(line)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S,%f").timestamp()


def parse_suite_junit(paths):
    """Returns ({class name: number of test cases}, {simple name: set of class names})."""
    counts = {}
    by_simple = {}
    for path in paths:
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError:  # silent-ok: an unreadable report only loses its suite-level metadata
            continue
        suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
        for suite in suites:
            name = (suite.attrib.get("name") or "").strip()
            if not name:
                continue
            try:
                tests = int(suite.attrib.get("tests") or 0)
            except ValueError:  # silent-ok: treat a malformed count as "has tests"
                tests = 1
            counts[name] = max(counts.get(name, 0), tests)
            by_simple.setdefault(name.rsplit(".", 1)[-1], set()).add(name)
    return counts, by_simple


def parse_canton_log(path, suites):
    """Adds {simple suite name: [start, end, queued seconds]} from one canton_test log.

    Suites already present (from an earlier log, e.g. the first run before a failed-only rerun) are kept.
    """
    seen_here = {}
    queued_since = {}
    with open(path, "rb") as f:
        for raw in f:
            if b"LogReporter" not in raw and b"ConcurrentEnvironmentLimiter" not in raw:
                continue
            line = raw.decode("utf-8", "replace")
            ts = _log_time(line)
            if ts is None:
                continue
            match = _SUITE_START.search(line)
            if match:
                seen_here.setdefault(match.group(1), [ts, None, 0.0])
                continue
            match = _SUITE_END.search(line)
            if match and match.group(1) in seen_here:
                seen_here[match.group(1)][1] = ts
                continue
            match = _LIMITER.search(line)
            if match:
                simple = match.group(1).rsplit(".", 1)[-1]
                if match.group(3) == "Queued":
                    queued_since[simple] = ts
                elif match.group(3) == "Running" and simple in queued_since:
                    entry = seen_here.get(simple)
                    if entry is not None:
                        entry[2] += ts - queued_since.pop(simple)
    for simple, entry in seen_here.items():
        if entry[1] is not None and simple not in suites:
            suites[simple] = entry
    return suites


def build_suite_wall_timings(timings, suite_junit_paths, log_paths):
    """Overrides test-case sums with suite-wall weights where the inputs allow it."""
    counts, by_simple = parse_suite_junit(suite_junit_paths)
    for name, tests in counts.items():
        if tests == 0 and name not in timings:
            timings[name] = 0.0
    suites = {}
    for path in log_paths:
        if os.path.isfile(path):
            parse_canton_log(path, suites)
    for simple, (start, end, queued) in suites.items():
        names = by_simple.get(simple, set())
        if len(names) != 1:  # unknown or ambiguous simple name: keep the test-case sum
            continue
        timings[next(iter(names))] = round(max(0.0, end - start - queued), 3)
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


def test_parse_junit_file_drops_negative_durations():
    xml = """<testsuite>
      <testcase classname="suite.A" name="a1" time="1.5"/>
      <testcase classname="suite.A" name="a2" time="-0.001"/>
      <testcase classname="suite.A" name="a3" time="-0.001"/>
    </testsuite>"""
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
        f.write(xml)
        path = f.name
    try:
        timings = {}
        parse_junit_file(path, timings)
        assert timings["suite.A"] == 1.5
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


def test_suite_wall_timings_from_log_and_zero_test_reports():
    log = """2026-11-09 21:00:00,000 [d] INFO  c.d.c.LogReporter:reporter=scala-test - Starting test suite 'FooIT'...
2026-11-09 21:00:01,000 [t] DEBUG c.d.c.i.ConcurrentEnvironmentLimiter$ - com.x.FooIT: New => Queued after 0s (1 pending)
2026-11-09 21:00:11,000 [t] DEBUG c.d.c.i.ConcurrentEnvironmentLimiter$ - com.x.FooIT: Queued => Running after 10s (1 pending)
2026-11-09 21:00:12,000 [d] INFO  c.d.c.LogReporter:reporter=scala-test - Starting 'FooIT/does a thing'...
2026-11-09 21:01:40,000 [d] INFO  c.d.c.LogReporter:reporter=scala-test - Completed test suite 'FooIT'.
2026-11-09 21:00:05,000 [d] INFO  c.d.c.LogReporter:reporter=scala-test - Starting test suite 'Unmapped'...
2026-11-09 21:00:06,000 [d] INFO  c.d.c.LogReporter:reporter=scala-test - Completed test suite 'Unmapped'.
"""
    reports = {
        "TEST-com.x.FooIT.xml": '<testsuite name="com.x.FooIT" tests="1" time="5.0"><testcase classname="com.x.FooIT" name="t" time="5.0"/></testsuite>',
        "TEST-com.x.KmsIT.xml": '<testsuite name="com.x.KmsIT" tests="0" time="0.0"></testsuite>',
    }
    with tempfile.TemporaryDirectory() as tmp:
        log_path = os.path.join(tmp, "canton_test.log")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(log)
        paths = []
        for name, xml in reports.items():
            paths.append(os.path.join(tmp, name))
            with open(paths[-1], "w", encoding="utf-8") as f:
                f.write(xml)
        timings = build_timings(paths)
        assert timings == {"com.x.FooIT": 5.0}
        build_suite_wall_timings(timings, paths, [log_path])
        assert timings["com.x.FooIT"] == 90.0, timings  # 100 s wall - 10 s queued
        assert timings["com.x.KmsIT"] == 0.0
        assert "Unmapped" not in timings


def test_suite_wall_keeps_first_log():
    first = "2026-11-09 21:00:00,000 [d] INFO  c.d.c.LogReporter:x - Starting test suite 'A'...\n2026-11-09 21:00:30,000 [d] INFO  c.d.c.LogReporter:x - Completed test suite 'A'.\n"
    rerun = "2026-11-09 22:00:00,000 [d] INFO  c.d.c.LogReporter:x - Starting test suite 'A'...\n2026-11-09 22:00:05,000 [d] INFO  c.d.c.LogReporter:x - Completed test suite 'A'.\n"
    with tempfile.TemporaryDirectory() as tmp:
        paths = []
        for i, text in enumerate([first, rerun]):
            paths.append(os.path.join(tmp, f"log{i}"))
            with open(paths[-1], "w", encoding="utf-8") as f:
                f.write(text)
        suites = {}
        for path in paths:
            parse_canton_log(path, suites)
        assert suites["A"][1] - suites["A"][0] == 30.0


def self_test():
    test_testcase_key_uses_classname()
    test_parse_junit_file_aggregates_durations()
    test_parse_junit_file_drops_negative_durations()
    test_parse_junit_file_ignores_invalid_xml()
    test_build_timings_skips_missing_paths()
    test_write_timings_and_ensure_output_dir()
    test_suite_wall_timings_from_log_and_zero_test_reports()
    test_suite_wall_keeps_first_log()
    print("All self-checks passed")


def main():
    args = parse_args()
    paths = collect_junit_paths(args.junit_glob)
    timings = build_timings(paths)
    if args.suite_junit_glob or args.canton_log_glob:
        suite_paths = collect_junit_paths(args.suite_junit_glob) if args.suite_junit_glob else []
        log_paths = sorted(glob.glob(args.canton_log_glob)) if args.canton_log_glob else []
        # First-run logs (kept as canton_test_first_run_*.log before a failed-only rerun) take precedence.
        log_paths.sort(key=lambda p: 0 if "first_run" in os.path.basename(p) else 1)
        build_suite_wall_timings(timings, suite_paths, log_paths)
    ensure_output_dir(args.output)
    write_timings(args.output, timings)


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        self_test()
    else:
        main()
