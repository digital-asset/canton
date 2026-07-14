#!/usr/bin/env python3
import argparse
import glob
import html
import json
import os
import tempfile


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aggregate shard-level test summaries into a single markdown and JSON state output."
    )
    parser.add_argument(
        "--input-glob",
        required=True,
        help="Glob pattern to locate shard summary JSON files.",
    )
    parser.add_argument(
        "--summary-path",
        default="",
        help="Optional markdown output path. Falls back to GITHUB_STEP_SUMMARY when empty.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of global not-passed tests to list in the details section.",
    )
    parser.add_argument(
        "--previous-state",
        default="",
        help="Optional path to previous aggregated state JSON used for rerun merging.",
    )
    parser.add_argument(
        "--state-output",
        default="",
        help="Optional path to write merged shard state JSON.",
    )
    return parser.parse_args()


def shard_sort_key(item):
    value = item["shard_index"]
    return (0, int(value)) if value.isdigit() else (1, value)


def load_shard_summaries(paths):
    shards = []
    parse_errors = []

    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            shard = normalize_shard_data(data, path)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            parse_errors.append(path)
            continue
        shards.append(shard)

    shards.sort(key=shard_sort_key)
    return shards, parse_errors


def normalize_shard_data(data, path):
    # Supports both raw test-summary.json format (nested "results") and persisted state format (flat fields).
    if not isinstance(data, dict):
        raise ValueError("shard data must be an object")
    results = data.get("results") if isinstance(data.get("results"), dict) else data

    missing_fields = []
    invalid_fields = []

    def parse_int(source, key):
        if key not in source:
            missing_fields.append(key)
            return None
        value = source.get(key)
        try:
            return int(value)
        except (TypeError, ValueError):
            invalid_fields.append(key)
            return None

    def parse_str(source, key, default="?"):
        if key not in source:
            missing_fields.append(key)
            return default
        return str(source.get(key, default))

    def parse_str_list(source, key):
        if key not in source:
            return []
        value = source.get(key)
        if not isinstance(value, list):
            invalid_fields.append(key)
            return []
        return [str(x) for x in value]

    return {
        "path": path,
        "shard_index": parse_str(data, "shard_index"),
        "total_shards": parse_str(data, "total_shards"),
        "junit_files": parse_int(data, "junit_files"),
        "passed": parse_int(results, "passed"),
        "failures": parse_int(results, "failures"),
        "errors": parse_int(results, "errors"),
        "skipped": parse_int(results, "skipped"),
        "parse_errors": parse_int(results, "parse_errors"),
        "total": parse_int(results, "total"),
        "not_passed_tests": parse_str_list(results, "not_passed_tests"),
        "missing_fields": sorted(set(missing_fields)),
        "invalid_fields": sorted(set(invalid_fields)),
    }


def shard_identity(shard):
    index = shard.get("shard_index", "?")
    if index != "?":
        return f"shard:{index}"
    return f"path:{shard.get('path', '?')}"


def load_previous_state(path):
    if not path or not os.path.isfile(path):
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []

    raw_shards = data.get("shards", []) if isinstance(data, dict) else []
    if not isinstance(raw_shards, list):
        return []

    shards = []
    for shard in raw_shards:
        try:
            shards.append(normalize_shard_data(shard, str(shard.get("path", "previous-state"))))
        except (ValueError, TypeError):
            continue
    return shards


def merge_shards(previous_shards, current_shards):
    merged = {}
    for shard in previous_shards:
        merged[shard_identity(shard)] = shard
    for shard in current_shards:
        merged[shard_identity(shard)] = shard

    merged_shards = list(merged.values())
    merged_shards.sort(key=shard_sort_key)
    return merged_shards


def write_state(path, shards):
    if not path:
        return

    output_dir = os.path.dirname(path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(path, "w", encoding="utf-8") as out:
        json.dump({"shards": shards}, out, indent=2, sort_keys=True)
        out.write("\n")


def resolve_summary_path(summary_path_arg):
    return summary_path_arg or os.environ.get("GITHUB_STEP_SUMMARY", "")


def infer_expected_shards(shards):
    total_shards_values = set()
    for shard in shards:
        value = shard["total_shards"]
        if value.isdigit():
            total_shards_values.add(int(value))
    if len(total_shards_values) == 1:
        return total_shards_values.pop()
    return None


def build_summary(files, shards, parse_errors, limit):
    def total_with_missing(field):
        values = [s[field] for s in shards if isinstance(s.get(field), int)]
        return sum(values), len(shards) - len(values)

    total_passed, missing_passed = total_with_missing("passed")
    total_failures, missing_failures = total_with_missing("failures")
    total_errors, missing_errors = total_with_missing("errors")
    total_skipped, missing_skipped = total_with_missing("skipped")
    total_parse_errors, missing_xml_parse_errors = total_with_missing("parse_errors")
    total_tests, missing_total = total_with_missing("total")
    total_junit_files, missing_junit_files = total_with_missing("junit_files")

    expected_shards = infer_expected_shards(shards)
    reported_shards = len(shards)

    missing_shards = []
    if expected_shards is not None:
        seen = {int(s["shard_index"]) for s in shards if s["shard_index"].isdigit()}
        missing_shards = [str(i) for i in range(expected_shards) if i not in seen]

    all_not_passed = []
    seen_tests = set()
    for shard in shards:
        for name in shard["not_passed_tests"]:
            if name not in seen_tests:
                seen_tests.add(name)
                all_not_passed.append(name)

    lines = []
    lines.append("## Test summary (all shards)")
    lines.append("")
    lines.append(f"- Summary files found: {len(files)}")
    lines.append(
        f"- Shards reported: {reported_shards}" + (f"/{expected_shards}" if expected_shards else "")
    )
    lines.append(
        f"- JUnit files found: {total_junit_files}"
        + (f" (missing in {missing_junit_files} shard(s))" if missing_junit_files else "")
    )
    lines.append(
        f"- Total: {total_tests}"
        + (f" (missing in {missing_total} shard(s))" if missing_total else "")
    )
    lines.append(
        f"- Passed: {total_passed}"
        + (f" (missing in {missing_passed} shard(s))" if missing_passed else "")
    )
    lines.append(
        f"- Failed: {total_failures}"
        + (f" (missing in {missing_failures} shard(s))" if missing_failures else "")
    )
    lines.append(
        f"- Errors: {total_errors}"
        + (f" (missing in {missing_errors} shard(s))" if missing_errors else "")
    )
    lines.append(
        f"- Skipped: {total_skipped}"
        + (f" (missing in {missing_skipped} shard(s))" if missing_skipped else "")
    )
    if total_parse_errors:
        lines.append(
            f"- XML parse errors: {total_parse_errors}"
            + (
                f" (missing in {missing_xml_parse_errors} shard(s))"
                if missing_xml_parse_errors
                else ""
            )
        )
    elif missing_xml_parse_errors:
        lines.append(f"- XML parse errors: n/a (missing in {missing_xml_parse_errors} shard(s))")
    if parse_errors:
        lines.append(f"- Invalid summary files: {len(parse_errors)}")
    if missing_shards:
        lines.append(
            f"- Missing shard indices: {', '.join(missing_shards)} (count: {len(missing_shards)})"
        )

    shards_with_missing_data = [
        s for s in shards if s.get("missing_fields") or s.get("invalid_fields")
    ]
    if shards_with_missing_data:
        lines.append(f"- Shards with missing/invalid fields: {len(shards_with_missing_data)}")
    lines.append("")

    if shards:
        lines.append("### Shard breakdown")
        lines.append("")
        lines.append(
            "| Shard | JUnit files | Total | Passed | Failed | Errors | Skipped | XML parse errors | Data status |"
        )
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")

        def display(value):
            return value if isinstance(value, int) else "n/a"

        def data_status(shard):
            parts = []
            if shard.get("missing_fields"):
                parts.append("missing: " + ", ".join(shard["missing_fields"]))
            if shard.get("invalid_fields"):
                parts.append("invalid: " + ", ".join(shard["invalid_fields"]))
            return "; ".join(parts) if parts else "ok"

        for shard in shards:
            lines.append(
                f"| {shard['shard_index']} | {display(shard['junit_files'])} | {display(shard['total'])} | {display(shard['passed'])} | {display(shard['failures'])} | {display(shard['errors'])} | {display(shard['skipped'])} | {display(shard['parse_errors'])} | {data_status(shard)} |"
            )
        lines.append("")

    lines.append("### Not passed tests (global)")
    if all_not_passed:
        shown = min(len(all_not_passed), limit)
        lines.append(
            f"<details><summary>Show failed and error tests ({len(all_not_passed)}, shown {shown})</summary>"
        )
        lines.append("")
        for test_name in all_not_passed[:limit]:
            lines.append(f"- {html.escape(test_name)}")
        if len(all_not_passed) > limit:
            lines.append(f"- and {len(all_not_passed) - limit} more")
        lines.append("")
        lines.append("</details>")
    else:
        lines.append("- none")

    return "\n".join(lines) + "\n"


def main():
    args = parse_args()
    files = sorted(glob.glob(args.input_glob, recursive=True))
    files = [path for path in files if os.path.isfile(path)]

    current_shards, parse_errors = load_shard_summaries(files)
    previous_shards = load_previous_state(args.previous_state)
    shards = merge_shards(previous_shards, current_shards)
    summary = build_summary(files, shards, parse_errors, args.limit)

    print(summary)

    summary_path = resolve_summary_path(args.summary_path)
    if summary_path:
        summary_dir = os.path.dirname(summary_path)
        if summary_dir:
            os.makedirs(summary_dir, exist_ok=True)
        with open(summary_path, "a", encoding="utf-8") as out:
            out.write(summary)

    write_state(args.state_output, shards)


def self_test():
    test_normalize_shard_data_supports_nested_results()
    test_load_shard_summaries_keeps_shard_with_missing_fields()
    test_load_shard_summaries_records_parse_errors_for_invalid_json()
    test_load_previous_state_reads_persisted_shards()
    test_merge_shards_prefers_current_shard_data()
    test_build_summary_handles_empty_input_glob()
    test_build_summary_reports_missing_shards_and_deduplicates_tests()
    test_build_summary_truncates_not_passed_tests()
    test_resolve_summary_path_prefers_arg_and_falls_back_to_env()
    test_write_state_round_trip()
    print("All self-checks passed")


def _sample_shard(
    shard_index="0",
    total_shards="2",
    path="/tmp/shard.json",
    passed=1,
    failures=0,
    errors=0,
    skipped=0,
    parse_errors=0,
    total=1,
    junit_files=1,
    not_passed_tests=None,
):
    if not_passed_tests is None:
        not_passed_tests = []
    return {
        "path": path,
        "shard_index": str(shard_index),
        "total_shards": str(total_shards),
        "junit_files": int(junit_files),
        "passed": int(passed),
        "failures": int(failures),
        "errors": int(errors),
        "skipped": int(skipped),
        "parse_errors": int(parse_errors),
        "total": int(total),
        "not_passed_tests": [str(x) for x in not_passed_tests],
        "missing_fields": [],
        "invalid_fields": [],
    }


def test_normalize_shard_data_supports_nested_results():
    data = {
        "shard_index": 1,
        "total_shards": 3,
        "junit_files": 2,
        "results": {
            "passed": 5,
            "failures": 1,
            "errors": 0,
            "skipped": 2,
            "parse_errors": 0,
            "total": 8,
            "not_passed_tests": ["com.example.Foo.testA"],
        },
    }
    shard = normalize_shard_data(data, "/tmp/s1.json")
    assert shard["shard_index"] == "1"
    assert shard["passed"] == 5
    assert shard["not_passed_tests"] == ["com.example.Foo.testA"]
    assert shard["missing_fields"] == []
    assert shard["invalid_fields"] == []


def test_load_shard_summaries_keeps_shard_with_missing_fields():
    with tempfile.TemporaryDirectory() as tmp:
        summary_path = os.path.join(tmp, "partial.json")
        with open(summary_path, "w", encoding="utf-8") as out:
            json.dump({"shard_index": 0, "total_shards": 2, "results": {"passed": 3}}, out)

        shards, parse_errors = load_shard_summaries([summary_path])
        assert parse_errors == [], f"Expected no parse errors for partial shard, got {parse_errors}"
        assert len(shards) == 1, f"Expected one partial shard, got {shards}"
        assert shards[0]["passed"] == 3
        assert shards[0]["junit_files"] is None
        assert "junit_files" in shards[0]["missing_fields"]


def test_load_shard_summaries_records_parse_errors_for_invalid_json():
    with tempfile.TemporaryDirectory() as tmp:
        broken_path = os.path.join(tmp, "broken.json")
        with open(broken_path, "w", encoding="utf-8") as out:
            out.write("{ this is not valid json")

        shards, parse_errors = load_shard_summaries([broken_path])
        assert shards == [], f"Expected no shards from invalid input, got {shards}"
        assert parse_errors == [broken_path], (
            f"Expected parse_errors to contain invalid file, got {parse_errors}"
        )


def test_load_previous_state_reads_persisted_shards():
    with tempfile.TemporaryDirectory() as tmp:
        state_path = os.path.join(tmp, "summary-state.json")
        with open(state_path, "w", encoding="utf-8") as out:
            json.dump({"shards": [_sample_shard(path="persisted.json", failures=1, total=2)]}, out)

        shards = load_previous_state(state_path)
        assert len(shards) == 1, f"Expected 1 shard, got {shards}"
        assert shards[0]["path"] == "persisted.json"
        assert shards[0]["failures"] == 1


def test_merge_shards_prefers_current_shard_data():
    previous = [_sample_shard(shard_index="0", path="old.json", passed=1, total=1)]
    current = [_sample_shard(shard_index="0", path="new.json", passed=3, total=3)]
    merged = merge_shards(previous, current)
    assert len(merged) == 1, f"Expected one merged shard, got {merged}"
    assert merged[0]["path"] == "new.json"
    assert merged[0]["passed"] == 3


def test_build_summary_handles_empty_input_glob():
    summary = build_summary([], [], [], limit=10)
    assert "- Summary files found: 0" in summary
    assert "- Shards reported: 0" in summary
    assert "### Not passed tests (global)" in summary
    assert "- none" in summary


def test_build_summary_reports_missing_shards_and_deduplicates_tests():
    shards = [
        _sample_shard(
            shard_index="0",
            total_shards="3",
            failures=1,
            total=2,
            not_passed_tests=["com.example.Foo.testA", "com.example.Bar.testB"],
        ),
        _sample_shard(
            shard_index="2",
            total_shards="3",
            errors=1,
            total=2,
            not_passed_tests=["com.example.Bar.testB", "com.example.Baz.testC"],
        ),
    ]
    summary = build_summary(["a.json", "b.json"], shards, ["broken.json"], limit=10)
    assert "- Missing shard indices: 1 (count: 1)" in summary, (
        f"Missing shard summary not found in: {summary}"
    )
    assert "- Invalid summary files: 1" in summary
    assert summary.count("- com.example.Bar.testB") == 1, "Expected deduplicated failed tests"
    assert "- Shards reported: 2/3" in summary


def test_build_summary_truncates_not_passed_tests():
    shards = [
        _sample_shard(
            shard_index="0",
            total_shards="1",
            failures=5,
            total=5,
            not_passed_tests=[f"com.example.Test{i}.test" for i in range(5)],
        )
    ]
    summary = build_summary(["a.json"], shards, [], limit=2)
    assert "shown 2" in summary, f"Expected shown count in summary, got: {summary}"
    assert "- and 3 more" in summary, f"Expected truncation suffix in summary, got: {summary}"


def test_resolve_summary_path_prefers_arg_and_falls_back_to_env():
    env_key = "GITHUB_STEP_SUMMARY"
    previous = os.environ.get(env_key)
    try:
        os.environ[env_key] = "/tmp/from-env.md"
        assert resolve_summary_path("") == "/tmp/from-env.md"
        assert resolve_summary_path("/tmp/from-arg.md") == "/tmp/from-arg.md"
    finally:
        if previous is None:
            os.environ.pop(env_key, None)
        else:
            os.environ[env_key] = previous


def test_write_state_round_trip():

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "state", "summary-state.json")
        shards = [_sample_shard(shard_index="1", path="roundtrip.json", passed=7, total=7)]
        write_state(path, shards)
        loaded = load_previous_state(path)
        assert loaded == shards, f"Expected round-trip persisted shards, got {loaded}"


if __name__ == "__main__":
    self_test()
    main()
