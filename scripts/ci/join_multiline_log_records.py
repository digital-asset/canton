#!/usr/bin/env python3
"""Collapse selected indented continuation lines onto the preceding log record.

Some Canton warnings are emitted as a headline line followed by indented detail lines.
The CI log checkers apply ignore rules record-by-record, so keeping those detail lines
separate can turn an otherwise ignored warning into a false positive.

Keep this deliberately narrow: only join continuation lines for known record families
that are already ignored as a unit in the CI filters. Extend SUPPORTED_MULTILINE_RECORD_MARKERS
when a new confirmed false-positive case appears.
"""

import sys
import re
from typing import Iterable, Iterator, Optional


LOG_RECORD_START_RE = re.compile(
    r"^(?:\[[A-Za-z]+\]|(?:TRACE|DEBUG|INFO|WARN|WARNING|ERROR|SEVERE)\b|\d{4}-\d{2}-\d{2}\s)"
)

# Add new markers here only for log record families whose indented continuation lines are known to
# cause false positives and whose full records are already covered by scoped ignore rules.
SUPPORTED_MULTILINE_RECORD_MARKERS = ("MediatorReplayBenchmark/",)


def looks_like_log_record_start(line: str) -> bool:
    return bool(LOG_RECORD_START_RE.match(line))


def accepts_continuation_lines(line: str) -> bool:
    return any(marker in line for marker in SUPPORTED_MULTILINE_RECORD_MARKERS)


def record_accepts_continuation_lines(record: str) -> bool:
    normalized = record.lstrip()
    return looks_like_log_record_start(normalized) and accepts_continuation_lines(normalized)


def collapse_log_records(lines: Iterable[str]) -> Iterator[str]:
    # Only join prefix-less continuation lines for benchmark-scoped records.
    # Canton log records normally start with a timestamp/level prefix; prefix-less
    # lines here are expected to be detail or stack lines, so the heuristic is
    # intentionally narrow and easy to extend for future confirmed cases.
    current: Optional[str] = None
    current_accepts_continuation = False

    for raw_line in lines:
        line = raw_line.rstrip("\n")

        if not line:
            if current is not None:
                yield current
                current = None
                current_accepts_continuation = False
            continue

        if line[:1].isspace():
            stripped = line.lstrip()
            starts_new_record = bool(stripped) and looks_like_log_record_start(stripped)
            if current is None:
                current = line
                current_accepts_continuation = record_accepts_continuation_lines(current)
            elif current_accepts_continuation and stripped and not starts_new_record:
                current = f"{current} {stripped}"
            else:
                yield current
                current = line
                current_accepts_continuation = record_accepts_continuation_lines(current)
            continue

        if current is not None:
            yield current
        current = line
        current_accepts_continuation = record_accepts_continuation_lines(current)

    if current is not None:
        yield current


def main() -> int:
    try:
        for record in collapse_log_records(sys.stdin):
            print(record)
    except BrokenPipeError:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
