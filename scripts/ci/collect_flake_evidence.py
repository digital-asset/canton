#!/usr/bin/env python3
"""Collects failure evidence for the flaky-test issues.

For each failing suite it gathers what the tracking issue otherwise lacks: the
failing test cases with their failure message and stack trace (JUnit XML), the
suite's WARN/ERROR lines (found_problems.txt), and the trace ids of the
failure in the test log. It writes:

  evidence.json      consumed by manage_flaky_issues.py, which renders it into
                     an evidence comment on the issue
  flake-pkg/<Suite>/ the suite's investigation package: everything known about
                     the failure in one place (manifest.json, README.md, the
                     suite's part of canton_test.log, a log per failing test,
                     the JUnit reports, the problem lines). Uploaded as the
                     `flake-pkg-*` artifact and listed in the comment, so it
                     can be investigated without access to the issue.

It runs as its own CI step before report_failing_tests.py, so that the log
artifacts can be uploaded in between and their names and ids handed to the
issue step (FLAKE_*_ARTIFACT_NAME / _ID, see cleanup_test_job/action.yml).

This module also owns the evidence comment format (render_evidence_comment) and
its machine-readable marker (parse_evidence_marker), so writer and reader stay
in one place.

Usage:
  collect_flake_evidence.py [--reports-dir DIR] [--log-dir DIR] [--problems FILE]
                            [--evidence-json PATH] [--package-out DIR] [--self-test]
"""

import argparse
import datetime
import gzip
import hashlib
import html
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import BinaryIO, Dict, Iterator, List, Optional

from flaky_common import (
    FAILING_TESTS_MAX,
    evidence_path,
    get_ci_branch,
    get_ci_build_url,
    get_ci_commit_hash,
    get_ci_job_name,
    get_ci_node_index,
    is_dry_run,
    run_guarded_self_test,
    should_report_issues,
)
from parse_failing_tests import (
    _SUITE_LOG_RE,
    compute_single_log_failure,
    failing_testcases,
    junit_report_paths,
)

EVIDENCE_VERSION = 1
# The investigation package format (manifest.json "version"). Bump it on changes
# that break its readers: renamed or removed fields or files.
PACKAGE_FORMAT_VERSION = 1

# GitHub rejects comments above 65536 characters; stay well below it.
COMMENT_MAX_CHARS = 60_000
TRACE_BUDGET_CHARS = 40_000
PROBLEMS_BUDGET_CHARS = 5_000
MAX_TIDS = 10
# The marker (first line of a comment) must survive any truncation, so it carries
# at most this many test names, each capped, plus the total count.
MARKER_MAX_TESTS = 50
MARKER_MAX_NAME_CHARS = 200
# Bounds the suite log (uncompressed) for suites that ran long next to busy
# neighbours: beyond it, only its head and tail are kept.
SUITE_LOG_MAX_BYTES = 256 * 1024 * 1024
# The same for the log of each failing test (plain text, not gzipped).
TEST_LOG_MAX_BYTES = 16 * 1024 * 1024
# Bounds all suite and test logs of a run together (uncompressed), for runs
# where many suites fail at once. Suites beyond it get no logs in their package;
# the shard bundle still has the whole log.
SUITE_LOGS_TOTAL_MAX_BYTES = 1024 * 1024 * 1024
_COPY_CHUNK_BYTES = 1024 * 1024

# Must match the `retention-days` of the flake-pkg and shard bundle uploads in
# .github/actions/test/cleanup_test_job/action.yml.
FLAKE_PKG_RETENTION_DAYS = 30
SHARD_BUNDLE_RETENTION_DAYS = 7

_RECORD_START_RE_BYTES = re.compile(rb"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
_LEVEL_RE = re.compile(r"\]\s+(TRACE|DEBUG|INFO|WARN|ERROR)\s")
_TID_RE = re.compile(r"tid:([0-9a-f]{8,})")
_MARKER_RE = re.compile(r"<!-- flaky-evidence v1 (\{.*?\}) -->")
_FRAME_RE = re.compile(r"^\s+at\s+(\S+)")
# Stack frames of the test framework and the JDK plumbing around it. Collapsed
# in the evidence (never in the suite log): they are the same for every failure.
_FRAMEWORK_FRAME_PREFIXES = (
    "org.scalatest.",
    "sbt.",
    "scala.collection.",
    "scala.runtime.",
    "scala.util.Try",
    "java.base/java.util.concurrent.",
    "java.base/java.lang.Thread",
    "java.base/jdk.internal.",
    "jdk.internal.",
)


def compact_trace(lines: List[str]) -> List[str]:
    """Collapses runs of framework stack frames into one '... N framework frames' line,
    and runs of an identical frame (recursion) into one '... repeated N times' line."""
    out: List[str] = []
    framework = 0
    repeated = 0

    def flush():
        nonlocal framework, repeated
        if repeated:
            out.append(f"\t... (previous frame repeated {repeated} more times)")
            repeated = 0
        if framework:
            out.append(f"\t... {framework} framework frames")
            framework = 0

    for line in lines:
        match = _FRAME_RE.match(line)
        if match and match.group(1).startswith(_FRAMEWORK_FRAME_PREFIXES):
            if repeated:
                flush()
            framework += 1
            continue
        if match and not framework and out and out[-1] == line:
            repeated += 1
            continue
        flush()
        out.append(line)
    flush()
    return out


# --- JUnit ----------------------------------------------------------------


def failure_text(elem) -> str:
    # Same join as test_output/summarize_test_results.failure_text.
    message = (elem.get("message") or "").strip()
    body = (elem.text or "").strip()
    if message and body:
        return message + "\n" + body
    return message or body


def collect_junit_failures(
    reports_dir: Path,
) -> tuple[Dict[str, List[dict]], Dict[str, List[Path]]]:
    """({suite: [{name, type, text}]} for every failing <testcase>,
    {suite: [the report files they are in]})."""
    failures: Dict[str, List[dict]] = {}
    reports: Dict[str, List[Path]] = {}
    if not reports_dir.is_dir():
        return failures, reports
    # The same test cases and suite keys as the issues (parse_failing_tests), so that
    # the evidence lines up with the entries in failing_tests.json.
    for path in junit_report_paths(reports_dir):
        for suite, case, problems in failing_testcases(path):
            suite_reports = reports.setdefault(suite, [])
            if path not in suite_reports:
                suite_reports.append(path)
            for problem in problems:
                failures.setdefault(suite, []).append(
                    {
                        "name": case.get("name", "unknown"),
                        "type": problem.get("type") or problem.tag,
                        "text": "\n".join(compact_trace(failure_text(problem).splitlines())),
                    }
                )
    return failures, reports


# --- found_problems.txt ---------------------------------------------------


def collect_problem_lines(problems_file: Path) -> tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """The found_problems.txt lines, keyed like parse_failing_tests.compute_log_failures:

    ({suite: [lines]} for the lines attributed to a suite,
     {residual key: [lines]} for all other lines).

    The lines without a suite (SBT [warn]/[error], third-party loggers) get their
    own issue, keyed by the one line compute_single_log_failure picks from them,
    so they are all kept under that key.
    """
    by_suite: Dict[str, List[str]] = {}
    residual: List[str] = []
    if not problems_file.is_file():
        return by_suite, {}
    for line in problems_file.read_text(errors="replace").splitlines():
        match = _SUITE_LOG_RE.search(line)
        if match:
            by_suite.setdefault(match.group(2), []).append(line)
        else:
            residual.append(line)
    key = compute_single_log_failure(residual)
    return by_suite, ({key: residual} if key else {})


# --- test log ---------------------------------------------------------------


class LogRecord:
    """A timestamped header line plus its continuation lines (e.g. a stack trace),
    located by byte offsets in the log file."""

    __slots__ = ("header", "level", "start", "end")

    def __init__(self, header: str, start: int):
        self.header = header
        match = _LEVEL_RE.search(header)
        self.level = match.group(1) if match else ""
        self.start = start
        self.end = start


def iter_records(f: BinaryIO) -> Iterator[LogRecord]:
    """Groups the lines of a log opened in binary mode into records.

    Binary, so that the offsets are exact byte positions; only the headers are
    decoded. A record ends where the next one starts, so its continuation lines
    (stack traces) lie within [start, end).
    """
    current: Optional[LogRecord] = None
    offset = 0
    for raw in f:
        if _RECORD_START_RE_BYTES.match(raw):
            if current is not None:
                yield current
            current = LogRecord(raw.decode("utf-8", errors="replace").rstrip("\n"), offset)
        offset += len(raw)
        if current is not None:
            current.end = offset
    if current is not None:
        yield current


def suite_matcher(suite: str) -> re.Pattern:
    """Matches a record header that belongs to `suite`.

    Canton tags log lines with an MDC "<logger>:<Suite>[/...]" marker, test
    threads are named "...ScalaTest-running-<Suite>", and LogReporter mentions
    the suite as '<Suite>' or '<Suite>/<test>'.
    """
    s = re.escape(suite)
    return re.compile(rf"(?:\s[\w.$]+:{s}(?=[/\s])|ScalaTest-running-{s}\]|'{s}(?:'|/))")


def find_test_log(log_dir: Path) -> Optional[Path]:
    """The test log of the failing run.

    On PRs and in the merge queue a failed test class is rerun, and the first
    attempt's log is kept as canton_test_first_run_<run>_<shard>.log
    (.github/actions/test/run_tests/action.yml). That one is deliberately not
    read: on main, where issues are reported, there is no rerun.
    """
    path = log_dir / "canton_test.log"
    return path if path.is_file() else None


class SuiteLog:
    """Where a suite is in the log: from the start of its first record to the end
    of its last one, plus the trace ids of its WARN/ERROR records, and where each
    of the given failing tests is: from its LogReporter "Starting" record to the
    end of its "Test failed" record (with the stack trace)."""

    def __init__(self, suite: str, tests: Optional[List[str]] = None):
        self.suite = suite
        self.matcher = suite_matcher(suite)
        self.records = 0
        self.first_start: Optional[int] = None
        self.last_end = 0
        self._tids: List[str] = []
        self._starting = {f"- Starting '{suite}/{t}'...": t for t in tests or []}
        self._failed = {f"- Test failed: '{suite}/{t}'": t for t in tests or []}
        self._test_starts: Dict[str, int] = {}
        # {test: (start, end)} of the failing tests whose range was found.
        self.test_ranges: Dict[str, tuple] = {}

    def add(self, record: LogRecord):
        self.records += 1
        if self.first_start is None:
            self.first_start = record.start
        self.last_end = record.end
        # The trace ids of the WARN/ERROR records point at the failing requests.
        if record.level in ("WARN", "ERROR") and len(self._tids) < MAX_TIDS:
            for tid in _TID_RE.findall(record.header):
                if tid not in self._tids:
                    self._tids.append(tid)
        if "reporter=scala-test" in record.header:
            self._add_reporter_record(record)

    def _add_reporter_record(self, record: LogRecord):
        # The message ends the header, possibly followed by ", context: ...".
        for marker, test in self._starting.items():
            if _ends_message(record.header, marker) and test not in self._test_starts:
                self._test_starts[test] = record.start
                return
        for marker, test in self._failed.items():
            if _ends_message(record.header, marker) and test in self._test_starts:
                self.test_ranges.setdefault(test, (self._test_starts[test], record.end))
                return

    def tids(self) -> List[str]:
        return list(self._tids[:MAX_TIDS])


def _ends_message(header: str, message: str) -> bool:
    # Exact, so that test "foo" does not match the markers of test "foo bar".
    at = header.find(message)
    if at < 0:
        return False
    rest = header[at + len(message) :]
    return rest == "" or rest.startswith(", context: ")


def scan_log(
    log_file: Path, suites: List[str], tests: Optional[Dict[str, List[str]]] = None
) -> Dict[str, SuiteLog]:
    """Reads the log once and returns {suite: SuiteLog} for the suites found in it,
    with the ranges of the failing `tests` ({suite: [test names]})."""
    tests = tests or {}
    suite_logs = {s: SuiteLog(s, tests.get(s)) for s in suites}
    with open(log_file, "rb") as f:
        for record in iter_records(f):
            for suite_log in suite_logs.values():
                if suite_log.matcher.search(record.header):
                    suite_log.add(record)
    return {s: log for s, log in suite_logs.items() if log.records}


def write_suite_log(log_file: Path, suite_log: SuiteLog, out: Path) -> int:
    """Writes the suite's range of the log, gzipped, and returns its size in bytes.

    Everything between its first and last record is kept, also the lines of
    other suites running in parallel and of loggers without the suite marker.
    """
    start, end = suite_log.first_start or 0, suite_log.last_end
    with open(log_file, "rb") as src, gzip.open(out, "wb") as dst:
        _write_range(src, dst, start, end, SUITE_LOG_MAX_BYTES, "suite's log")
    return end - start


def write_test_log(log_file: Path, start: int, end: int, out: Path) -> int:
    """Writes a failing test's range of the log, as plain text, and returns its size
    in bytes. Like the suite log, it keeps the lines of other suites in between."""
    with open(log_file, "rb") as src, open(out, "wb") as dst:
        _write_range(src, dst, start, end, TEST_LOG_MAX_BYTES, "test's log")
    return end - start


def _write_range(src: BinaryIO, dst, start: int, end: int, max_bytes: int, what: str) -> None:
    """Copies [start, end) of `src`; above `max_bytes` only its head and tail."""
    size = end - start
    if size <= max_bytes:
        _copy_range(src, dst, start, size)
        return
    head = max_bytes // 4
    tail = max_bytes - head
    _copy_range(src, dst, start, head)
    cut = size - head - tail
    dst.write(f"\n... ({cut} bytes of the {what} cut, see the shard bundle)\n".encode())
    _copy_range(src, dst, end - tail, tail)


def _copy_range(src: BinaryIO, dst, start: int, length: int) -> None:
    src.seek(start)
    while length > 0:
        chunk = src.read(min(_COPY_CHUNK_BYTES, length))
        if not chunk:
            return
        dst.write(chunk)
        length -= len(chunk)


def _safe_file_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)[:150]


def _unique_name(name: str, used: set, suffix: str = "") -> str:
    """`name` made safe for a file name, plus `suffix`; with a digest of the full name
    if sanitising made two names equal (`Foo$Bar` and `Foo_Bar`, a shared 150-char
    prefix, or names differing in case only, for case-insensitive file systems)."""
    unique = f"{_safe_file_name(name)}{suffix}"
    if unique.lower() in used:
        digest = hashlib.sha256(name.encode()).hexdigest()[:8]
        unique = f"{_safe_file_name(name)}-{digest}{suffix}"
    used.add(unique.lower())
    return unique


# --- the investigation package ------------------------------------------------


def ci_context() -> dict:
    """Where the failure happened, from the CI environment only (no API calls)."""
    env = os.environ.get
    return {
        "sha": get_ci_commit_hash(),
        "branch": get_ci_branch(),
        "job": get_ci_job_name(),
        "shard": get_ci_node_index(),
        "protocol_version": env("CANTON_PROTOCOL_VERSION") or None,
        "run_url": get_ci_build_url() or None,
        "run_id": env("GITHUB_RUN_ID") or None,
        "run_attempt": env("GITHUB_RUN_ATTEMPT") or None,
        "runner": env("RUNNER_NAME") or None,
        "date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


def package_log_bytes(suite_log: SuiteLog) -> int:
    """What the logs of a suite's package take (uncompressed), after the caps."""
    size = min(suite_log.last_end - (suite_log.first_start or 0), SUITE_LOG_MAX_BYTES)
    return size + sum(min(e - s, TEST_LOG_MAX_BYTES) for s, e in suite_log.test_ranges.values())


def write_package(
    pkg_dir: Path,
    record: dict,
    junit_reports: List[Path],
    log_file: Optional[Path],
    suite_log: Optional[SuiteLog],
) -> dict:
    """Writes the investigation package of one suite (see the module docstring and
    scripts/ci/README.md) and returns its manifest. Without `suite_log` the package
    has no logs. Sets the record's `suite_log_bytes`."""
    pkg_dir.mkdir(parents=True)
    files: Dict[str, object] = {
        "readme": "README.md",
        "suite_log": None,
        "junit": [],
        "problems": None,
    }
    truncated: List[str] = []
    if suite_log and log_file:
        record["suite_log_bytes"] = write_suite_log(log_file, suite_log, pkg_dir / "suite.log.gz")
        files["suite_log"] = "suite.log.gz"
        if record["suite_log_bytes"] > SUITE_LOG_MAX_BYTES:
            truncated.append("suite.log.gz")

    test_logs: Dict[str, Optional[str]] = {}
    used: set = set()
    for test in record["tests"]:
        name = test["name"]
        test_range = suite_log.test_ranges.get(name) if suite_log and log_file else None
        if name in test_logs or not test_range:
            test_logs.setdefault(name, None)
            continue
        log = f"tests/{_unique_name(name, used, '.log')}"
        (pkg_dir / "tests").mkdir(exist_ok=True)
        if write_test_log(log_file, *test_range, pkg_dir / log) > TEST_LOG_MAX_BYTES:
            truncated.append(log)
        test_logs[name] = log

    used = set()
    for report in junit_reports:
        copy = f"junit/{_unique_name(report.stem, used, report.suffix)}"
        (pkg_dir / "junit").mkdir(exist_ok=True)
        shutil.copyfile(report, pkg_dir / copy)
        files["junit"].append(copy)
    if record["problems"]:
        (pkg_dir / "problems.txt").write_text("\n".join(record["problems"]) + "\n")
        files["problems"] = "problems.txt"

    # One entry per test, like the evidence comment counts them: a test with both a
    # <failure> and an <error> lists both types.
    tests: Dict[str, dict] = {}
    for t in record["tests"]:
        name = t["name"]
        entry = tests.setdefault(name, {"name": name, "types": [], "log": test_logs[name]})
        if t["type"] not in entry["types"]:
            entry["types"].append(t["type"])

    manifest = {
        "format": "flake-pkg",
        "version": PACKAGE_FORMAT_VERSION,
        "suite": record["suite"],
        "tests": list(tests.values()),
        "problem_count": len(record["problems"]),
        "tids": record["tids"],
        "ci": ci_context(),
        "files": files,
        "truncated": truncated,
    }
    with open(pkg_dir / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    (pkg_dir / "README.md").write_text(render_package_readme(manifest))
    return manifest


def render_package_readme(manifest: dict) -> str:
    """The package's entry point for a human: what failed, and what the files hold."""
    ci = manifest["ci"]
    suite = manifest["suite"]
    files = manifest["files"]
    run = f" ([CI run]({ci['run_url']}))" if ci["run_url"] else ""
    pv = f", protocol version {ci['protocol_version']}" if ci["protocol_version"] else ""
    lines = [
        f"# Investigation package: {suite}",
        "",
        (
            f"`{suite}` failed in job `{ci['job']}` (shard {ci['shard']}{pv}) on `{ci['branch']}`, "
            f"commit `{ci['sha']}`, {ci['date']}{run}."
        ),
        "",
    ]
    if manifest["tests"]:
        lines.append("Failing tests:")
        for t in manifest["tests"]:
            log = f"`{t['log']}`" if t["log"] else "no log found"
            lines.append(f"- `{t['name']}` ({', '.join(t['types'])}): {log}")
    else:
        lines.append(
            f"No failing tests: the suite failed on {manifest['problem_count']} log problem(s)."
        )
    rows = [("manifest.json", f"this package, machine-readable (flake-pkg v{manifest['version']})")]
    if files["suite_log"]:
        what = (
            "the suite's part of `canton_test.log`, from its first to its last record,"
            " including the lines of suites running in parallel"
        )
        rows.append((files["suite_log"], what))
    if any(t["log"] for t in manifest["tests"]):
        what = (
            f"per failing test, the log from `Starting '{suite}/<test>'...` to the stack"
            f" trace of `Test failed: '{suite}/<test>'`"
        )
        rows.append(("tests/*.log", what))
    if files["junit"]:
        rows.append(("junit/*.xml", "the JUnit reports: failure messages, stack traces, output"))
    if files["problems"]:
        rows.append((files["problems"], "the suite's unexpected WARN/ERROR lines (check-logs.sh)"))
    lines += ["", "| File | Content |", "|---|---|"] + [f"| `{f}` | {what} |" for f, what in rows]
    lines += [
        "",
        (
            "Log records read"
            " `<date> [<thread>] <LEVEL> <logger>:<suite>/<node> tid:<trace id> - <message>`."
        ),
    ]
    if manifest["tids"]:
        tids = ", ".join(f"`{t}`" for t in manifest["tids"])
        lines.append(
            f"Trace ids of the failure's WARN/ERROR records: {tids}."
            " `zgrep tid:<id> suite.log.gz` follows one request across nodes."
        )
    lines.append("To browse the logs, use lnav with `canton.lnav.json` from the repository root.")
    if manifest["truncated"]:
        cut = ", ".join(f"`{f}`" for f in manifest["truncated"])
        lines.append(
            f"Too big, only head and tail kept: {cut}. The shard bundle has the whole log."
        )
    return "\n".join(lines) + "\n"


# --- evidence.json ----------------------------------------------------------


def collect_evidence(
    reports_dir: Path, log_dir: Path, problems_file: Path, package_out: Optional[Path]
) -> dict:
    junit, junit_reports = collect_junit_failures(reports_dir)
    problems, residual_problems = collect_problem_lines(problems_file)
    # Only the failing tests that manage_flaky_issues reports get evidence: the
    # same keys as parse_failing_tests, in the same (sorted) order and cap.
    reported = set(sorted(set(junit) | set(problems) | set(residual_problems))[:FAILING_TESTS_MAX])
    residual_problems = {k: v for k, v in residual_problems.items() if k in reported}
    suites = sorted((set(junit) | set(problems)) & reported)
    log_file = find_test_log(log_dir)
    failing_tests = {s: [t["name"] for t in junit.get(s, [])] for s in suites}
    suite_logs = scan_log(log_file, suites, failing_tests) if suites and log_file else {}
    # Problem lines without a suite: no tests, no package, just the lines.
    evidence = {
        key: {
            "suite": key,
            "tests": [],
            "problems": lines,
            "tids": [],
            "package_dir": None,
            "suite_log_bytes": 0,
        }
        for key, lines in residual_problems.items()
    }
    used_names: set = set()
    budget = SUITE_LOGS_TOTAL_MAX_BYTES
    for suite in suites:
        suite_log = suite_logs.get(suite)
        record = {
            "suite": suite,
            "tests": junit.get(suite, []),
            "problems": problems.get(suite, []),
            "tids": suite_log.tids() if suite_log else [],
            "package_dir": None,
            "suite_log_bytes": 0,
        }
        evidence[suite] = record
        if package_out is None:
            continue
        if suite_log and package_log_bytes(suite_log) > budget:
            print(f"Suite logs over {SUITE_LOGS_TOTAL_MAX_BYTES} bytes, none for {suite}.")
            suite_log = None
        elif suite_log:
            budget -= package_log_bytes(suite_log)
        record["package_dir"] = _unique_name(suite, used_names)
        write_package(
            package_out / record["package_dir"],
            record,
            junit_reports.get(suite, []),
            log_file,
            suite_log,
        )
    return {"version": EVIDENCE_VERSION, "suites": evidence}


def write_evidence(path: str, evidence: dict) -> None:
    with open(path, "w") as f:
        json.dump(evidence, f, indent=2)
    print(f"Wrote evidence for {len(evidence['suites'])} suite(s) to {path}")


def read_evidence(path: Optional[str] = None) -> Dict[str, dict]:
    """{suite: record} from evidence.json, or {} if there is none."""
    path = path or evidence_path()
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f).get("suites", {})


# --- the evidence comment ---------------------------------------------------


def fence_for(text: str) -> str:
    """A backtick fence long enough that `text` cannot close it early."""
    fence = "```"
    while fence in text:
        fence += "`"
    return fence


def _details(summary: str, text: str) -> str:
    fence = fence_for(text)
    return f"<details><summary>{summary}</summary>\n\n{fence}\n{text}\n{fence}\n\n</details>"


def _truncate(text: str, budget: int, what: str) -> str:
    if len(text) <= budget:
        return text
    return text[:budget] + f"\n... ({what} truncated, {len(text) - budget} chars cut)"


def _truncate_middle(text: str, budget: int, what: str) -> str:
    # Keeps both ends: the failure message is at the top, the root cause
    # ("Caused by: ...") at the bottom.
    if len(text) <= budget:
        return text
    head = text[: budget // 2]
    tail = text[-(budget // 2) :]
    head = head[: head.rfind("\n") + 1] or head
    tail = tail[tail.find("\n") + 1 :] or tail
    return f"{head}\t... ({what} truncated, {len(text) - len(head) - len(tail)} chars cut)\n{tail}"


def _render_traces(tests: List[dict], budget: int) -> str:
    if not tests:
        return ""
    per_test = max(budget // len(tests), 1_500)
    blocks = []
    for test in tests:
        header = f"--- {test['name']} ({test['type']})"
        blocks.append(header + "\n" + _truncate_middle(test["text"], per_test, "trace"))
    return _truncate("\n\n".join(blocks), budget, "traces")


def _marker(meta: dict) -> str:
    # "--" inside the JSON would end the HTML comment; it can only occur inside
    # strings, where - is an equivalent JSON escape.
    payload = json.dumps(meta, sort_keys=True, separators=(",", ":"))
    payload = payload.replace("--", "\\u002d\\u002d")
    return f"<!-- flaky-evidence v1 {payload} -->"


def parse_evidence_marker(comment_body: str) -> Optional[dict]:
    """The metadata of an evidence comment, or None if it is not one."""
    match = _MARKER_RE.search(comment_body)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def evidence_label(test_count: int) -> str:
    """What an evidence comment holds, in a few words: the end of its summary line
    and the text of its link in the issue table."""
    if not test_count:
        return "log problems"
    return f"{test_count} failing test{'s' if test_count != 1 else ''}"


def _expiry_date(retention_days: int) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    return (now + datetime.timedelta(days=retention_days)).strftime("%Y-%m-%d")


def render_evidence_comment(records: List[dict], ctx: dict) -> str:
    """Renders the evidence of one issue (possibly several suite variants).

    `ctx` holds the CI context: title, sha, commit_url, job, shard, job_url,
    log_url (the CI log of the failing step), run_url, run_id, repo, cause,
    date (UTC string), and `artifacts`: [{name, id and url (both optional),
    description, retention_days}] for the logs uploaded for this failure.
    """
    tests = [t for r in records for t in r["tests"]]
    problems = [p for r in records for p in r["problems"]]
    tids = [t for r in records for t in r["tids"]][:MAX_TIDS]
    artifacts = ctx.get("artifacts") or []
    test_names = sorted({t["name"] for t in tests})
    meta = {
        "sha": ctx["sha"],
        "job": ctx["job"],
        "shard": ctx["shard"],
        "cause": ctx["cause"],
        "suites": [r["suite"] for r in records],
        "tests": [n[:MARKER_MAX_NAME_CHARS] for n in test_names[:MARKER_MAX_TESTS]],
        "test_count": len(test_names),
        "job_url": ctx["job_url"],
        "run_id": ctx.get("run_id") or None,
        "artifacts": {a["name"]: a.get("id") for a in artifacts},
        "date": ctx["date"],
    }

    # The whole comment is collapsed under a one-line summary. The marker stays on
    # the first line, outside: manage_flaky_issues.existing_evidence reads it there.
    summary = html.escape(
        f"{ctx['job']} (shard {ctx['shard']}), commit {ctx['sha'][:8]}, {ctx['date']}, "
        f"cause {ctx['cause']}, {evidence_label(meta['test_count'])}"
    )
    lines = [
        _marker(meta),
        f"<details><summary><b>Failure evidence</b>: {summary}</summary>",
        "",
    ]
    problems_text = _truncate("\n".join(problems), PROBLEMS_BUDGET_CHARS, "problems")
    if problems and not tests:
        # A log-only failure: the problem lines are the evidence, so they come first.
        fence = fence_for(problems_text)
        lines += ["**Log problems** (found_problems.txt):", "", fence, problems_text, fence, ""]
    if tests:
        lines.append(f"**Failing tests** ({len(tests)}):")
        for t in tests[:20]:
            lines.append(f"- `{t['name']}` ({t['type']})")
        if len(tests) > 20:
            lines.append(f"- ... and {len(tests) - 20} more")
        lines.append("")

    lines.append(f"**CI log:** [failing step]({ctx.get('log_url') or ctx['job_url']})")
    if tids:
        lines.append("**Trace ids:** " + ", ".join(f"`{t}`" for t in tids))
    lines.append("")
    if artifacts:
        lines.append("**Artifacts:**")
        repo = ctx.get("repo") or "DACH-NY/canton"
        for a in artifacts:
            # The browser asks to confirm the download; `gh run download` is the
            # terminal alternative for the same artifact.
            name = f"[`{a['name']}`]({a['url']})" if a.get("url") else f"`{a['name']}`"
            lines.append(
                f"- {name}: {a['description']} (until {_expiry_date(a['retention_days'])})"
            )
            if ctx.get("run_id"):
                lines.append(f"  `gh run download {ctx['run_id']} -R {repo} -n {a['name']}`")
        if ctx.get("run_url"):
            lines.append(f"- [All artifacts of the run]({ctx['run_url']}#artifacts)")
        lines.append("")
    head = "\n".join(lines)

    sections = []
    if problems and tests:
        sections.append(_details("Log problems (found_problems.txt)", problems_text))

    trace_budget = TRACE_BUDGET_CHARS
    # Shrink the traces until the whole comment fits.
    while True:
        parts = list(sections)
        if tests:
            parts.insert(0, _details("Stack traces", _render_traces(tests, trace_budget)))
        # Markdown needs a blank line before an HTML block such as <details>.
        body = head + "\n" + "\n\n".join(parts) + "\n\n</details>"
        if len(body) <= COMMENT_MAX_CHARS:
            return body
        if trace_budget > 3_000:
            trace_budget = max(trace_budget - (len(body) - COMMENT_MAX_CHARS) - 200, 3_000)
        else:
            # Cut the readable part only: the marker line is kept whole, or the
            # comment could no longer be recognised (duplicates, wrong label).
            marker, rest = body.split("\n", 1)
            keep = COMMENT_MAX_CHARS - len(marker) - 100
            return f"{marker}\n{rest[:keep]}\n\n... (comment truncated)\n\n</details>"


# --- entry point ------------------------------------------------------------


def run(
    reports_dir: str = "test-reports",
    log_dir: str = "log",
    problems_file: str = "found_problems.txt",
    evidence_json: Optional[str] = None,
    package_out: str = "flake-pkg",
) -> None:
    if not (should_report_issues() or is_dry_run()):
        print("Skipping flake evidence collection: branch is not reported and no dry run.")
        return
    evidence = collect_evidence(
        Path(reports_dir), Path(log_dir), Path(problems_file), Path(package_out)
    )
    write_evidence(evidence_json or evidence_path(), evidence)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Collect failure evidence for flaky issues.")
    parser.add_argument("--reports-dir", default="test-reports")
    parser.add_argument("--log-dir", default="log")
    parser.add_argument("--problems", default="found_problems.txt")
    parser.add_argument("--evidence-json", default=None, help="Default: shared CI temp dir.")
    parser.add_argument("--package-out", default="flake-pkg")
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit.")
    return parser.parse_args(argv)


# --- self-tests ---------------------------------------------------------------

_FIXTURE_JUNIT = """<?xml version='1.0' encoding='UTF-8'?>
<testsuite name="com.example.FooIntegrationTest" tests="3">
  <testcase classname="com.example.FooIntegrationTest" name="foo should ping" time="1.0">
    <failure message="1 was not equal to 2" type="org.scalatest.exceptions.TestFailedException">
org.scalatest.exceptions.TestFailedException: 1 was not equal to 2
    at com.example.FooIntegrationTest.$anonfun$new$1(FooIntegrationTest.scala:42)
    </failure>
  </testcase>
  <testcase classname="com.example.FooIntegrationTest" name="foo should pass" time="0.1"/>
  <testcase classname="com.example.BarTest" name="bar works" time="0.1"/>
</testsuite>
"""


def _fixture_log(suite: str, other: str, failure: bool = True) -> str:
    ts = "2026-09-29 08:01:50,{:03d}"
    lines = [
        f"{ts.format(0)} [env-0] INFO  c.d.c.p.Participant:{other} - before the suite",
        f"{ts.format(1)} [ScalaTest-dispatcher] INFO  c.d.c.LogReporter:reporter=scala-test - Starting test suite '{suite}'...",
        f"{ts.format(1)} [ScalaTest-dispatcher] INFO  c.d.c.LogReporter:reporter=scala-test - Starting '{suite}/foo should pass'...",
        f"{ts.format(1)} [ScalaTest-dispatcher] INFO  c.d.c.LogReporter:reporter=scala-test - Test succeeded: '{suite}/foo should pass'",
        f"{ts.format(2)} [ScalaTest-dispatcher] INFO  c.d.c.LogReporter:reporter=scala-test - Starting '{suite}/foo should ping'...",
        f"{ts.format(3)} [env-1] INFO  c.d.c.p.Participant:{suite}/participant=participant1 tid:aaaa1111bbbb2222 - starting",
        f"{ts.format(4)} [env-1] INFO  c.d.c.p.Participant:{other}/participant=participant1 tid:cccc3333dddd4444 - other suite",
        f"{ts.format(5)} [env-1] WARN  c.d.c.p.Participant:{suite}/participant=participant1 tid:eeee5555ffff6666 - something odd",
        f"{ts.format(6)} [pool-1-thread-1-ScalaTest-running-{suite}] ERROR c.d.c.X:{suite} - boom",
        "java.lang.IllegalStateException: boom",
        "\tat com.example.X.y(X.scala:1)",
    ]
    if failure:
        lines += [
            f"{ts.format(7)} [ScalaTest-dispatcher] WARN  c.d.c.LogReporter:reporter=scala-test - Test failed: '{suite}/foo should ping'",
            "org.scalatest.exceptions.TestFailedException: 1 was not equal to 2",
            "\tat com.example.FooIntegrationTest.$anonfun$new$1(FooIntegrationTest.scala:42)",
        ]
    lines += [
        f"{ts.format(8)} [env-1] ERROR c.d.c.p.Participant:{suite}/participant=participant1 - last words",
        "java.lang.RuntimeException: stack trace of the last record",
        "\tat com.example.Last.words(Last.scala:1)",
        f"{ts.format(9)} [env-2] INFO  c.d.c.p.Participant:{other} - after the suite",
    ]
    return "\n".join(lines) + "\n"


def _make_fixture(tmp: Path, first_run_log: Optional[str] = None) -> tuple:
    reports = tmp / "test-reports" / "community" / "app"
    reports.mkdir(parents=True)
    (reports / "TEST-com.example.FooIntegrationTest.xml").write_text(_FIXTURE_JUNIT)
    log_dir = tmp / "log"
    log_dir.mkdir()
    (log_dir / "canton_test.log").write_text(
        _fixture_log("FooIntegrationTest", "FooIntegrationTestH2")
    )
    if first_run_log is not None:
        (log_dir / "canton_test_first_run_1_0.log").write_text(first_run_log)
    problems = tmp / "found_problems.txt"
    problems.write_text(
        "WARN  c.d.c.p.Participant:FooIntegrationTest/participant=participant1 tid:eeee5555ffff6666 - something odd\n"
        "WARN  c.d.c.p.Participant:LogOnlyIntegrationTest/participant=participant1 - log only\n"
        "ERROR i.g.i.ManagedChannelOrphanWrapper - *~*~*~ Channel was not shutdown properly\n"
        "[warn] sbt: something without a suite\n"
    )
    return tmp / "test-reports", log_dir, problems


def test_collect_evidence_from_junit_log_and_problems():
    from unittest.mock import patch

    from parse_failing_tests import compute_log_failures

    env = {
        "GITHUB_ACTIONS": "true",
        "GITHUB_SHA": "a1b2c3d4" + "0" * 32,
        "GITHUB_REF_NAME": "main",
        "GITHUB_JOB": "test",
        "CANTON_CI_JOB_NAME": "test_postgres",
        "MATRIX_SHARD": "3",
        "GITHUB_REPOSITORY": "DACH-NY/canton",
        "GITHUB_RUN_ID": "1",
        "GITHUB_RUN_ATTEMPT": "2",
        "CANTON_PROTOCOL_VERSION": "35",
    }
    with tempfile.TemporaryDirectory() as t, patch.dict(os.environ, env, clear=True):
        tmp = Path(t)
        reports, log_dir, problems = _make_fixture(tmp)
        evidence = collect_evidence(reports, log_dir, problems, tmp / "flake-pkg")["suites"]

        # Lines without a suite are kept under the key parse_failing_tests gives their issue.
        residual_key = next(
            k
            for k in compute_log_failures(problems.read_text().splitlines())
            if k not in ("FooIntegrationTest", "LogOnlyIntegrationTest")
        )
        assert set(evidence) == {
            "FooIntegrationTest",
            "LogOnlyIntegrationTest",
            residual_key,
        }, evidence
        residual = evidence[residual_key]
        assert residual["problems"] == [
            "ERROR i.g.i.ManagedChannelOrphanWrapper - *~*~*~ Channel was not shutdown properly",
            "[warn] sbt: something without a suite",
        ], residual
        assert residual["tests"] == [] and residual["package_dir"] is None
        assert sorted(p.name for p in (tmp / "flake-pkg").iterdir()) == [
            "FooIntegrationTest",
            "LogOnlyIntegrationTest",
        ], "lines without a suite get no package"
        foo = evidence["FooIntegrationTest"]
        assert [t["name"] for t in foo["tests"]] == ["foo should ping"], foo["tests"]
        assert "FooIntegrationTest.scala:42" in foo["tests"][0]["text"]
        assert len(foo["problems"]) == 1
        # Trace ids of the suite's WARN/ERROR records, not of the H2 variant's.
        assert foo["tids"] == ["eeee5555ffff6666"], foo["tids"]

        pkg = tmp / "flake-pkg" / foo["package_dir"]
        assert sorted(str(p.relative_to(pkg)) for p in pkg.rglob("*") if p.is_file()) == [
            "README.md",
            "junit/TEST-com.example.FooIntegrationTest.xml",
            "manifest.json",
            "problems.txt",
            "suite.log.gz",
            "tests/foo_should_ping.log",
        ]
        # The suite log is the log from the suite's first record to the end of its
        # last one: interleaved lines of other suites included, lines outside not.
        suite_log = gzip.open(pkg / "suite.log.gz", "rt").read()
        assert suite_log.startswith("2026-09-29 08:01:50,001 "), suite_log
        assert "Test failed: 'FooIntegrationTest/foo should ping'" in suite_log, suite_log
        assert "java.lang.IllegalStateException: boom" in suite_log
        assert "other suite" in suite_log, "interleaved lines are kept"
        assert suite_log.endswith("\tat com.example.Last.words(Last.scala:1)\n"), suite_log
        assert "before the suite" not in suite_log and "after the suite" not in suite_log
        assert foo["suite_log_bytes"] == len(suite_log.encode())
        # The failing test's log: from its Starting record to the end of the stack
        # trace of its Test failed record. Not the test that ran before it.
        test_log = (pkg / "tests" / "foo_should_ping.log").read_text()
        assert "Starting 'FooIntegrationTest/foo should ping'..." in test_log.splitlines()[0]
        assert test_log.endswith("(FooIntegrationTest.scala:42)\n"), test_log
        assert "foo should pass" not in test_log and "last words" not in test_log
        assert "other suite" in test_log, "interleaved lines are kept"
        # The JUnit report as it was, not compacted.
        assert (
            pkg / "junit" / "TEST-com.example.FooIntegrationTest.xml"
        ).read_text() == _FIXTURE_JUNIT
        assert (pkg / "problems.txt").read_text() == "\n".join(foo["problems"]) + "\n"

        manifest = json.loads((pkg / "manifest.json").read_text())
        assert manifest["format"] == "flake-pkg" and manifest["version"] == PACKAGE_FORMAT_VERSION
        assert manifest["suite"] == "FooIntegrationTest"
        assert manifest["tests"] == [
            {
                "name": "foo should ping",
                "types": ["org.scalatest.exceptions.TestFailedException"],
                "log": "tests/foo_should_ping.log",
            }
        ], manifest["tests"]
        assert manifest["tids"] == ["eeee5555ffff6666"] and manifest["problem_count"] == 1
        ci = manifest["ci"]
        assert (ci["sha"], ci["branch"], ci["job"], ci["shard"]) == (
            env["GITHUB_SHA"],
            "main",
            "test_postgres",
            "3",
        ), ci
        assert ci["run_url"] == "https://github.com/DACH-NY/canton/actions/runs/1/attempts/2"
        assert (ci["run_id"], ci["run_attempt"], ci["protocol_version"]) == ("1", "2", "35")
        assert manifest["files"] == {
            "readme": "README.md",
            "suite_log": "suite.log.gz",
            "junit": ["junit/TEST-com.example.FooIntegrationTest.xml"],
            "problems": "problems.txt",
        }, manifest["files"]
        assert manifest["truncated"] == []
        readme = (pkg / "README.md").read_text()
        assert readme.startswith("# Investigation package: FooIntegrationTest\n"), readme
        assert (
            "- `foo should ping` (org.scalatest.exceptions.TestFailedException):"
            " `tests/foo_should_ping.log`"
        ) in readme
        assert "`eeee5555ffff6666`" in readme and "| `problems.txt` |" in readme

        # A suite that is only in found_problems.txt gets a package with its lines, no logs.
        log_only = evidence["LogOnlyIntegrationTest"]
        assert log_only["tests"] == [] and log_only["tids"] == []
        pkg = tmp / "flake-pkg" / log_only["package_dir"]
        assert sorted(p.name for p in pkg.iterdir()) == [
            "README.md",
            "manifest.json",
            "problems.txt",
        ]
        assert (
            "No failing tests: the suite failed on 1 log problem(s)."
            in (pkg / "README.md").read_text()
        )


def _many_suites_fixture(tmp: Path, suites: List[str]) -> tuple:
    reports = tmp / "test-reports"
    reports.mkdir()
    cases = "".join(
        f'<testcase classname="com.example.{s}" name="t"><failure message="m">boom</failure></testcase>'
        for s in suites
    )
    (reports / "TEST-many.xml").write_text(f"<testsuite>{cases}</testsuite>")
    log_dir = tmp / "log"
    log_dir.mkdir()
    ts = "2026-09-29 08:01:50,000"
    (log_dir / "canton_test.log").write_text(
        "".join(f"{ts} [t] WARN  c.d.c.X:{s} - line of {s}\n" for s in suites)
    )
    return reports, log_dir, tmp / "no_problems.txt"


def test_evidence_only_for_reported_failures_and_within_budget():
    suites = [f"Suite{i:02d}Test" for i in range(FAILING_TESTS_MAX + 5)]
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        reports, log_dir, problems = _many_suites_fixture(tmp, suites)
        evidence = collect_evidence(reports, log_dir, problems, tmp / "flake-pkg")["suites"]
        # The same cap and order as manage_flaky_issues: the rest is never reported.
        assert sorted(evidence) == suites[:FAILING_TESTS_MAX], sorted(evidence)
        assert len(list((tmp / "flake-pkg").iterdir())) == FAILING_TESTS_MAX
        # Without LogReporter markers in the log, a failing test has no log of its own.
        manifest = json.loads((tmp / "flake-pkg" / suites[0] / "manifest.json").read_text())
        assert manifest["tests"] == [{"name": "t", "types": ["failure"], "log": None}], manifest

    global SUITE_LOGS_TOTAL_MAX_BYTES
    saved = SUITE_LOGS_TOTAL_MAX_BYTES
    SUITE_LOGS_TOTAL_MAX_BYTES = 160  # room for two of the ~76-byte suite logs
    try:
        with tempfile.TemporaryDirectory() as t:
            tmp = Path(t)
            reports, log_dir, problems = _many_suites_fixture(tmp, suites[:3])
            evidence = collect_evidence(reports, log_dir, problems, tmp / "flake-pkg")["suites"]
            has_log = [(tmp / "flake-pkg" / s / "suite.log.gz").exists() for s in suites[:3]]
            over = tmp / "flake-pkg" / suites[2]
            assert sorted(p.name for p in over.iterdir()) == ["README.md", "junit", "manifest.json"]
    finally:
        SUITE_LOGS_TOTAL_MAX_BYTES = saved
    assert has_log == [True, True, False], has_log
    assert evidence[suites[2]]["tests"], "over the budget there is still the other evidence"


def test_package_lists_a_test_once_with_all_its_types():
    record = {
        "suite": "FooTest",
        "tests": [
            {"name": "a", "type": "failure", "text": "x"},
            {"name": "b", "type": "failure", "text": "y"},
            {"name": "a", "type": "error", "text": "z"},
        ],
        "problems": [],
        "tids": [],
    }
    with tempfile.TemporaryDirectory() as t:
        manifest = write_package(Path(t) / "pkg", record, [], None, None)
        assert manifest["tests"] == [
            {"name": "a", "types": ["failure", "error"], "log": None},
            {"name": "b", "types": ["failure"], "log": None},
        ], manifest["tests"]
        readme = (Path(t) / "pkg" / "README.md").read_text()
        assert "- `a` (failure, error): no log found" in readme, readme


def test_budget_counts_the_test_logs():
    suite_log = SuiteLog("FooTest", ["a", "b"])
    suite_log.first_start, suite_log.last_end = 100, 1_100
    suite_log.test_ranges = {"a": (200, 300), "b": (400, 400 + TEST_LOG_MAX_BYTES + 1)}
    assert package_log_bytes(suite_log) == 1_000 + 100 + TEST_LOG_MAX_BYTES


def test_suite_log_names_are_unique():
    used: set = set()
    names = [_unique_name(s, used, ".log") for s in ("Foo$Bar", "Foo_Bar", "foo_bar")]
    assert names[0] == "Foo_Bar.log" and len({n.lower() for n in names}) == 3, names
    assert names[1].startswith("Foo_Bar-") and names[1].endswith(".log"), names
    assert names[2].startswith("foo_bar-"), "names that differ in case only are made unique"


def test_first_run_log_is_ignored():
    first = _fixture_log("FooIntegrationTest", "Other").replace("boom", "first-run-only")
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        reports, log_dir, problems = _make_fixture(tmp, first_run_log=first)
        foo = collect_evidence(reports, log_dir, problems, tmp / "flake-pkg")["suites"][
            "FooIntegrationTest"
        ]
        suite_log = gzip.open(tmp / "flake-pkg" / foo["package_dir"] / "suite.log.gz", "rt").read()
        assert "first-run-only" not in suite_log and suite_log.count("Test failed:") == 1


def test_suite_log_keeps_head_and_tail_when_too_big():
    lines = [
        f"2026-09-29 08:01:50,000 [t] INFO  c.d.c.X:BigTest - line {i:05d}" for i in range(5_000)
    ]
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        log = tmp / "canton_test.log"
        log.write_text("\n".join(lines) + "\n")
        suite_log = scan_log(log, ["BigTest"])["BigTest"]
        global SUITE_LOG_MAX_BYTES
        saved = SUITE_LOG_MAX_BYTES
        SUITE_LOG_MAX_BYTES = 40_000
        try:
            size = write_suite_log(log, suite_log, tmp / "BigTest.log.gz")
        finally:
            SUITE_LOG_MAX_BYTES = saved
        text = gzip.open(tmp / "BigTest.log.gz", "rt").read()
        assert size == log.stat().st_size
        assert "line 00000" in text and "line 04999" in text, "head and tail are kept"
        assert "bytes of the suite's log cut" in text and len(text) < 41_000, len(text)

        global TEST_LOG_MAX_BYTES
        saved = TEST_LOG_MAX_BYTES
        TEST_LOG_MAX_BYTES = 20_000
        try:
            size = write_test_log(log, 0, log.stat().st_size, tmp / "t.log")
        finally:
            TEST_LOG_MAX_BYTES = saved
        text = (tmp / "t.log").read_text()
        assert size == log.stat().st_size
        assert "line 00000" in text and "line 04999" in text, "head and tail are kept"
        assert "bytes of the test's log cut" in text and len(text) < 21_000, len(text)


def test_test_ranges_match_whole_names():
    header = "2026-09-29 08:01:50,000 [d] {} c.d.c.LogReporter:reporter=scala-test - {}"
    records = [
        ("INFO ", "Starting 'FooTest/t'..."),
        ("INFO ", "Starting 'FooTest/t bis'..., context: {x}"),
        ("WARN ", "Test failed: 'FooTest/t bis', context: {x}"),
        ("WARN ", "Test failed: 'FooTest/t'"),
    ]
    lines = [header.format(*r) + "\n" for r in records]
    offsets = [sum(len(line) for line in lines[:i]) for i in range(len(lines) + 1)]
    with tempfile.TemporaryDirectory() as t:
        log = Path(t) / "canton_test.log"
        log.write_text("".join(lines))
        ranges = scan_log(log, ["FooTest"], {"FooTest": ["t", "t bis"]})["FooTest"].test_ranges
    # "t" is not confused with "t bis", and a trailing ", context: ..." is allowed.
    assert ranges == {"t": (offsets[0], offsets[4]), "t bis": (offsets[1], offsets[3])}, ranges


def test_evidence_and_issues_agree_on_the_failing_suites():
    from parse_failing_tests import iterate_through_test_reports

    reports = {
        "TEST-a.xml": _FIXTURE_JUNIT,
        # generate_watchdog_xml.py's shape for a killed suite.
        "TEST-watchdog.xml": (
            '<testsuite name="watchdog"><testcase classname="pkg.HangingTest" name="t">'
            '<failure type="WatchdogTimeout">killed</failure></testcase></testsuite>'
        ),
        # Nested deeper than sbt writes: not counted, by either side.
        "TEST-nested.xml": (
            '<testsuites><testsuite name="s"><testcase classname="pkg.NestedTest" name="t">'
            "<failure>boom</failure></testcase></testsuite></testsuites>"
        ),
        # A report cut off by a killed JVM: skipped, the other reports still count.
        "TEST-broken.xml": '<testsuite name="s"><testcase classname="pkg.BrokenTest"',
    }
    with tempfile.TemporaryDirectory() as t:
        reports_dir = Path(t) / "test-reports"
        (reports_dir / "app").mkdir(parents=True)
        for name, xml in reports.items():
            (reports_dir / "app" / name).write_text(xml)
        evidence, _ = collect_junit_failures(reports_dir)
        issues = iterate_through_test_reports(reports_dir)
    assert set(evidence) == set(issues) == {"FooIntegrationTest", "HangingTest"}, (
        evidence,
        issues,
    )


def test_suite_matcher_is_exact():
    m = suite_matcher("FooTest")
    assert m.search("INFO  c.d.c.X:FooTest/participant=p1 tid:1 - a")
    assert m.search("INFO  c.d.c.X:FooTest - a")
    assert m.search("[pool-1-ScalaTest-running-FooTest] INFO  c.d.c.X - a")
    assert m.search("WARN  c.d.c.LogReporter:reporter=scala-test - Test failed: 'FooTest/t'")
    assert not m.search("INFO  c.d.c.X:FooTestPostgres/participant=p1 - a")
    assert not m.search("INFO  c.d.c.X:BarFooTest - a")


def _artifacts() -> list:
    return [
        {
            "name": "flake-pkg-test-run-1-pv-36-shard-3-attempt-1",
            "id": "9",
            "url": "https://github.com/DACH-NY/canton/actions/runs/1/artifacts/9",
            "description": "the investigation package",
            "retention_days": FLAKE_PKG_RETENTION_DAYS,
        },
        {
            "name": "test-bundle-run-1-pv-36-shard-3-attempt-1",
            "url": "https://github.com/DACH-NY/canton/actions/runs/1/artifacts/8",
            "description": "the shard bundle",
            "retention_days": SHARD_BUNDLE_RETENTION_DAYS,
        },
    ]


def _ctx(**overrides) -> dict:
    ctx = {
        "title": "[main] Flaky FooIntegrationTest",
        "sha": "a1b2c3d4" + "0" * 32,
        "commit_url": "https://github.com/DACH-NY/canton/commit/a1b2c3d4" + "0" * 32,
        "job": "test",
        "shard": "3",
        "job_url": "https://github.com/DACH-NY/canton/actions/runs/1/job/2",
        "log_url": "https://github.com/DACH-NY/canton/actions/runs/1/job/2#step:6",
        "run_url": "https://github.com/DACH-NY/canton/actions/runs/1/attempts/1",
        "run_id": "1",
        "repo": "DACH-NY/canton",
        "cause": "regular",
        "date": "2026-09-29 08:01 UTC",
        "artifacts": _artifacts(),
    }
    ctx.update(overrides)
    return ctx


def test_comment_renders_links_and_marker_round_trips():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        reports, log_dir, problems = _make_fixture(tmp)
        foo = collect_evidence(reports, log_dir, problems, None)["suites"]["FooIntegrationTest"]
    # A test name containing "--" must not end the HTML comment early.
    foo["tests"][0]["name"] = "foo --> should ping"
    body = render_evidence_comment([foo], _ctx())
    assert body.startswith("<!-- flaky-evidence v1 "), body[:80]
    # Collapsed as a whole, with the context in the summary line.
    assert body.splitlines()[1] == (
        "<details><summary><b>Failure evidence</b>: test (shard 3), commit a1b2c3d4, "
        "2026-09-29 08:01 UTC, cause regular, 1 failing test</summary>"
    ), body.splitlines()[1]
    assert body.endswith("\n\n</details>"), body[-40:]
    assert body.index("-->") > body.index("}"), "marker must close after its JSON"
    meta = parse_evidence_marker(body)
    assert meta["sha"] == _ctx()["sha"] and meta["job"] == "test" and meta["shard"] == "3"
    assert meta["tests"] == ["foo --> should ping"], meta
    assert (
        meta["run_id"] == "1"
        and meta["artifacts"]["flake-pkg-test-run-1-pv-36-shard-3-attempt-1"] == "9"
    )
    assert (
        "**CI log:** [failing step](https://github.com/DACH-NY/canton/actions/runs/1/job/2#step:6)"
        in body
    )
    assert "\n**Artifacts:**\n- [`flake-pkg-" in body, body
    # The link to all artifacts of the run is the last entry.
    assert (
        "-n test-bundle-run-1-pv-36-shard-3-attempt-1`\n"
        "- [All artifacts of the run]"
        "(https://github.com/DACH-NY/canton/actions/runs/1/attempts/1#artifacts)\n"
    ) in body, body
    # Each log: its direct link, then the gh command for the same artifact on the next line.
    logs_expiry = _expiry_date(FLAKE_PKG_RETENTION_DAYS)
    bundle_expiry = _expiry_date(SHARD_BUNDLE_RETENTION_DAYS)
    assert (
        "- [`flake-pkg-test-run-1-pv-36-shard-3-attempt-1`]"
        "(https://github.com/DACH-NY/canton/actions/runs/1/artifacts/9): "
        f"the investigation package (until {logs_expiry})\n"
        "  `gh run download 1 -R DACH-NY/canton -n flake-pkg-test-run-1-pv-36-shard-3-attempt-1`"
    ) in body, body
    assert (
        "- [`test-bundle-run-1-pv-36-shard-3-attempt-1`]"
        "(https://github.com/DACH-NY/canton/actions/runs/1/artifacts/8): "
        f"the shard bundle (until {bundle_expiry})\n"
        "  `gh run download 1 -R DACH-NY/canton -n test-bundle-run-1-pv-36-shard-3-attempt-1`"
    ) in body, body
    assert "Download from a terminal" not in body
    assert "`eeee5555ffff6666`" in body
    assert "<summary>Stack traces</summary>" in body
    assert "<summary>Log problems (found_problems.txt)</summary>" in body
    assert "Commit [" not in body, "the commit is in the summary line only"
    assert parse_evidence_marker("just a human comment") is None


def test_comment_without_artifacts_has_no_logs_section():
    record = {
        "suite": "S",
        "tests": [],
        "problems": ["WARN x:S - y"],
        "tids": [],
        "package_dir": None,
    }
    body = render_evidence_comment([record], _ctx(artifacts=[], log_url="", run_url=""))
    assert "**Artifacts:**" not in body and "#artifacts" not in body and "gh api" not in body
    assert (
        "**CI log:** [failing step](https://github.com/DACH-NY/canton/actions/runs/1/job/2)" in body
    )
    # A log-only failure shows its problem lines first, not collapsed.
    assert "**Log problems** (found_problems.txt):\n\n```\nWARN x:S - y\n```" in body, body
    assert body.index("**Log problems**") < body.index("**CI log:**")
    assert "<summary>Log problems" not in body
    assert "cause regular, log problems</summary>" in body


def test_comment_stays_under_github_limit():
    huge = {
        "suite": "S",
        "tests": [
            {"name": f"t{i}", "type": "E", "text": "\n".join(["\tat a.b.c(X.scala:1)"] * 5_000)}
            for i in range(5)
        ],
        "problems": ["WARN x:S - " + "p" * 500] * 200,
        "tids": [],
        "package_dir": "S",
    }
    body = render_evidence_comment([huge], _ctx())
    assert len(body) <= COMMENT_MAX_CHARS, len(body)
    assert parse_evidence_marker(body) is not None
    assert "truncated" in body


def test_marker_survives_many_long_test_names():
    # Names so long that the visible list alone overflows: forces the final cut.
    name = "the suite should " + "x" * 3_000
    tests = [{"name": f"{name} {i:04d}", "type": "E", "text": "boom"} for i in range(500)]
    record = {"suite": "S", "tests": tests, "problems": [], "tids": [], "package_dir": None}
    body = render_evidence_comment([record], _ctx())
    assert len(body) <= COMMENT_MAX_CHARS, len(body)
    meta = parse_evidence_marker(body.split("\n", 1)[0])
    assert meta is not None, "the marker line must stay whole"
    assert meta["test_count"] == 500 and len(meta["tests"]) == MARKER_MAX_TESTS, meta
    assert all(len(n) <= MARKER_MAX_NAME_CHARS for n in meta["tests"])
    assert "500 failing tests</summary>" in body
    assert "... (comment truncated)" in body and body.endswith("</details>")


def test_compact_trace_collapses_framework_frames():
    lines = [
        "org.scalatest.exceptions.TestFailedException: 1 was not equal to 2",
        "\tat org.scalatest.matchers.MatchersHelper$.indicateFailure(MatchersHelper.scala:392)",
        "\tat com.example.FooTest.$anonfun$new$1(FooTest.scala:42)",
        "\tat org.scalatest.OutcomeOf.outcomeOf(OutcomeOf.scala:85)",
        "\tat sbt.TestRunner.run(TestFramework.scala:168)",
        "\tat java.base/java.lang.Thread.run(Thread.java:1474)",
        "Caused by: java.lang.RuntimeException: root",
        "\tat com.example.Bar.baz(Bar.scala:7)",
    ]
    assert compact_trace(lines) == [
        "org.scalatest.exceptions.TestFailedException: 1 was not equal to 2",
        "\t... 1 framework frames",
        "\tat com.example.FooTest.$anonfun$new$1(FooTest.scala:42)",
        "\t... 3 framework frames",
        "Caused by: java.lang.RuntimeException: root",
        "\tat com.example.Bar.baz(Bar.scala:7)",
    ], compact_trace(lines)

    recursion = ["java.lang.IllegalStateException: deep"] + ["\tat com.example.R.r(R.scala:3)"] * 5
    recursion += ["\tat com.example.R.main(R.scala:9)"]
    assert compact_trace(recursion) == [
        "java.lang.IllegalStateException: deep",
        "\tat com.example.R.r(R.scala:3)",
        "\t... (previous frame repeated 4 more times)",
        "\tat com.example.R.main(R.scala:9)",
    ], compact_trace(recursion)


def test_long_trace_keeps_the_root_cause():
    text = "\n".join(
        ["java.lang.IllegalStateException: top"]
        + [f"\tat com.example.F.f{i}(F.scala:{i})" for i in range(2_000)]
        + ["Caused by: java.lang.RuntimeException: root cause", "\tat com.example.G.g(G.scala:1)"]
    )
    out = _render_traces([{"name": "t", "type": "E", "text": text}], 3_000)
    assert "IllegalStateException: top" in out and "root cause" in out, out
    assert "trace truncated" in out and len(out) < 3_300, len(out)


def self_test():
    test_compact_trace_collapses_framework_frames()
    test_marker_survives_many_long_test_names()
    test_long_trace_keeps_the_root_cause()
    test_collect_evidence_from_junit_log_and_problems()
    test_first_run_log_is_ignored()
    test_evidence_only_for_reported_failures_and_within_budget()
    test_suite_log_names_are_unique()
    test_package_lists_a_test_once_with_all_its_types()
    test_budget_counts_the_test_logs()
    test_test_ranges_match_whole_names()
    test_suite_log_keeps_head_and_tail_when_too_big()
    test_suite_matcher_is_exact()
    test_evidence_and_issues_agree_on_the_failing_suites()
    test_comment_renders_links_and_marker_round_trips()
    test_comment_without_artifacts_has_no_logs_section()
    test_comment_stays_under_github_limit()
    print("collect_flake_evidence self-checks passed")


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.self_test:
        run_guarded_self_test(self_test)
        sys.exit(0)
    run(args.reports_dir, args.log_dir, args.problems, args.evidence_json, args.package_out)
