#!/usr/bin/env python3
#
# Copyright (c) 2023-2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.

import json
import sys
from pathlib import Path


# Transform one or more `summary.json` files into a Markdown table.
# `summary.json` contains multiple JSON objects (one for each test),
# so we parse them sequentially.
def parse_json_file(file_path):
    """Parses a file containing either single JSON objects or NDJSON (one object per line)."""
    records = []
    path = Path(file_path)

    if not path.exists():
        print(f"[WARN] File not found: {file_path}", file=sys.stderr)
        return records

    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            return records

        decoder = json.JSONDecoder()
        pos = 0
        while pos < len(content):
            while pos < len(content) and content[pos].isspace():
                pos += 1
            if pos >= len(content):
                break
            try:
                obj, end = decoder.raw_decode(content, pos)
                records.append(obj)
                pos = end
            except json.JSONDecodeError as e:
                print(
                    f"[WARN] Failed to parse JSON in {file_path} near character {pos}: {e}",
                    file=sys.stderr,
                )
                break

    return records


def generate_markdown_table(records):
    """Formats list of metric records into a strictly aligned Markdown table without extra divider padding."""
    if not records:
        return "No performance test results found."

    headers = ["Test Name", "TPS", "Start Time", "Duration", "Version", "Machine"]
    alignments = ["left", "right", "left", "left", "left", "left"]

    # Raw row values before padding
    raw_rows = []
    for r in records:
        test_name = f"`{r.get('test-name', 'N/A')}`"
        version = f"`{r.get('version', 'N/A')}`"
        machine = str(r.get("machine", "N/A"))
        start_time = str(r.get("start-time", "N/A"))
        duration = str(r.get("duration", "N/A"))

        results = r.get("results", {})
        tps = results.get("transactions-per-second")
        if isinstance(tps, (int, float)):
            tps_str = f"**{tps:.2f}**"
        elif tps is None:
            tps_str = "**N/A**"
        else:
            tps_str = str(tps)

        raw_rows.append([test_name, tps_str, start_time, duration, version, machine])

    # Determine maximum display width for each column (content width + 2 spaces padding)
    col_widths = [len(h) for h in headers]
    for row in raw_rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    # Account for 1 space on each side of text inside cells
    total_widths = [w + 2 for w in col_widths]

    # Build padded header
    padded_headers = [
        " "
        + (
            headers[i].ljust(col_widths[i])
            if alignments[i] == "left"
            else headers[i].rjust(col_widths[i])
        )
        + " "
        for i in range(len(headers))
    ]

    # Build divider (e.g. |:--------------------------|----------:|)
    divider = []
    for i in range(len(headers)):
        width = total_widths[i]
        if alignments[i] == "right":
            divider.append("-" * (width - 1) + ":")
        else:
            divider.append(":" + "-" * (width - 1))

    # Build padded data rows
    padded_rows = []
    for row in raw_rows:
        padded_row = [
            " "
            + (
                row[i].ljust(col_widths[i])
                if alignments[i] == "left"
                else row[i].rjust(col_widths[i])
            )
            + " "
            for i in range(len(row))
        ]
        padded_rows.append("|" + "|".join(padded_row) + "|")

    table = [
        "|" + "|".join(padded_headers) + "|",
        "|" + "|".join(divider) + "|",
    ] + padded_rows

    return "\n".join(table)


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: render-metrics-table.py <path-to-summary.json> [path-to-another.json ...]",
            file=sys.stderr,
        )
        sys.exit(1)

    all_records = []
    for arg in sys.argv[1:]:
        all_records.extend(parse_json_file(arg))

    markdown_output = generate_markdown_table(all_records)
    print(markdown_output)


if __name__ == "__main__":
    main()
