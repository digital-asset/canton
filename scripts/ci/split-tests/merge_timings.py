#!/usr/bin/env python3
import argparse
import glob
import json
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Merge shard timing JSON files and optionally backfill missing tests from previous timings."
    )
    parser.add_argument(
        "--input-glob",
        required=True,
        help="Glob pattern used to discover current shard timing JSON files.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to write merged timings JSON.",
    )
    parser.add_argument(
        "--previous-timings",
        default="",
        help="Optional previous timings JSON used as fallback for tests missing in current inputs.",
    )
    parser.add_argument(
        "--allow-empty-current",
        action="store_true",
        help="Allow empty merged current timings (still fails when no files match --input-glob).",
    )
    parser.add_argument(
        "--previous-only-if-no-current",
        action="store_true",
        help=(
            "For reruns: when no files match --input-glob because no shard ran in this attempt, "
            "write --previous-timings unchanged instead of failing. Still fails when there are "
            "no usable previous timings either."
        ),
    )
    return parser.parse_args()


def merge_timings(paths):
    merged = {}
    for path in paths:
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for k, v in data.items():
            merged[k] = merged.get(k, 0.0) + float(v)
    return merged


def merge_previous_as_fallback(current, previous):
    for k, v in previous.items():
        if k not in current:
            current[k] = float(v)
    return current


def validate_current_timings(paths, merged, allow_empty_current=False):
    if not paths:
        raise ValueError(
            "No timing files matched --input-glob. Failing to avoid silently dropping timing history."
        )
    if not merged and not allow_empty_current:
        raise ValueError(
            "Merged current timings are empty before applying --previous-timings. Failing to avoid silent fallback to default timings."
        )


def load_previous_timings(path):
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (
        OSError,
        ValueError,
        json.JSONDecodeError,
    ):  # silent-ok: previous timings are only a fallback, callers decide whether empty is fatal
        return {}
    if not isinstance(data, dict):
        return {}
    try:
        return {k: float(v) for k, v in data.items()}
    except (TypeError, ValueError):  # silent-ok: same as an unreadable file
        return {}


def resolve_timings(
    paths, merged, previous, allow_empty_current=False, previous_only_if_no_current=False
):
    """Returns the timings to write, or raises ValueError.

    A rerun in which no shard ran again has no current files; with previous_only_if_no_current the
    previous timings are kept as they are. Without usable previous timings that is still an error.
    """
    if not paths and previous_only_if_no_current and previous:
        return dict(previous)
    validate_current_timings(paths, merged, allow_empty_current)
    return merge_previous_as_fallback(merged, previous)


def main():
    args = parse_args()
    paths = sorted(glob.glob(args.input_glob, recursive=True))
    merged = merge_timings(paths)
    previous = load_previous_timings(args.previous_timings)

    try:
        merged = resolve_timings(
            paths,
            merged,
            previous,
            allow_empty_current=args.allow_empty_current,
            previous_only_if_no_current=args.previous_only_if_no_current,
        )
    except ValueError as e:
        print(f"merge-timings error: {e}", file=sys.stderr)
        raise SystemExit(1)
    if not paths:
        print("merge-timings: no shard timings from this attempt, keeping the previous timings")

    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, sort_keys=True)


def self_test():
    test_validate_current_timings_rejects_empty_paths()
    test_validate_current_timings_rejects_empty_merged()
    test_validate_current_timings_accepts_non_empty_input()
    test_validate_current_timings_allows_empty_merged_when_flag_enabled()
    test_merge_timings_sums_values()
    test_merge_timings_missing_file_is_skipped()
    test_merge_timings_empty_input()
    test_merge_timings_deduplicates_across_shards()
    test_merge_timings_with_previous_timings_fallback_only()
    test_resolve_timings_keeps_previous_when_no_shard_reran()
    test_resolve_timings_without_current_or_previous_still_fails()
    test_resolve_timings_without_flag_still_fails_on_no_current()
    test_resolve_timings_partial_rerun_overrides_only_rerun_classes()
    test_load_previous_timings_tolerates_bad_input()
    print("All self-checks passed")


def _write_timings(tmp_path, filename, data):
    path = os.path.join(tmp_path, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return path


def test_validate_current_timings_rejects_empty_paths():
    try:
        validate_current_timings([], {"com.example.FooTest": 1.0})
        assert False, "Expected ValueError for empty matched paths"
    except ValueError as e:
        assert "No timing files matched" in str(e), f"Unexpected error: {e}"


def test_validate_current_timings_rejects_empty_merged():
    try:
        validate_current_timings(["/tmp/s0.json"], {})
        assert False, "Expected ValueError for empty merged timings"
    except ValueError as e:
        assert "Merged current timings are empty" in str(e), f"Unexpected error: {e}"


def test_validate_current_timings_accepts_non_empty_input():
    validate_current_timings(["/tmp/s0.json"], {"com.example.FooTest": 1.0})


def test_validate_current_timings_allows_empty_merged_when_flag_enabled():
    validate_current_timings(["/tmp/s0.json"], {}, allow_empty_current=True)


def test_merge_timings_sums_values():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        p0 = _write_timings(
            tmp, "s0.json", {"com.example.FooTest": 1.5, "com.example.BarTest": 2.0}
        )
        p1 = _write_timings(
            tmp, "s1.json", {"com.example.FooTest": 0.5, "com.example.BazTest": 3.0}
        )
        result = merge_timings([p0, p1])
        assert result["com.example.FooTest"] == 2.0, (
            f"Expected 2.0, got {result['com.example.FooTest']}"
        )
        assert result["com.example.BarTest"] == 2.0, (
            f"Expected 2.0, got {result['com.example.BarTest']}"
        )
        assert result["com.example.BazTest"] == 3.0, (
            f"Expected 3.0, got {result['com.example.BazTest']}"
        )


def test_merge_timings_missing_file_is_skipped():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        p0 = _write_timings(tmp, "s0.json", {"com.example.FooTest": 1.0})
        result = merge_timings([p0, "/nonexistent/path/shard.json"])
        assert result == {"com.example.FooTest": 1.0}, (
            f"Expected only valid file merged, got: {result}"
        )


def test_merge_timings_empty_input():
    result = merge_timings([])
    assert result == {}, f"Expected empty dict for empty input, got: {result}"


def test_merge_timings_deduplicates_across_shards():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        # Same test appearing in 3 shards: timings must be summed, not overwritten
        p0 = _write_timings(tmp, "s0.json", {"com.example.FooTest": 10.0})
        p1 = _write_timings(tmp, "s1.json", {"com.example.FooTest": 20.0})
        p2 = _write_timings(tmp, "s2.json", {"com.example.FooTest": 30.0})
        result = merge_timings([p0, p1, p2])
        assert result["com.example.FooTest"] == 60.0, (
            f"Expected 60.0 (sum across shards), got {result['com.example.FooTest']}"
        )


def test_merge_timings_with_previous_timings_fallback_only():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        # Current shard timings
        current = _write_timings(
            tmp, "current.json", {"com.example.FooTest": 5.0, "com.example.BarTest": 2.0}
        )
        # Previous timings from a rerun
        previous = _write_timings(
            tmp, "previous.json", {"com.example.FooTest": 3.0, "com.example.BazTest": 1.0}
        )

        # Merge current with previous as fallback-only
        result = merge_timings([current])
        with open(previous, encoding="utf-8") as f:
            previous_data = json.load(f)
        merge_previous_as_fallback(result, previous_data)

        assert result["com.example.FooTest"] == 5.0, (
            f"Expected 5.0 (no inflation), got {result['com.example.FooTest']}"
        )
        assert result["com.example.BarTest"] == 2.0, (
            f"Expected 2.0, got {result['com.example.BarTest']}"
        )
        assert result["com.example.BazTest"] == 1.0, (
            f"Expected 1.0, got {result['com.example.BazTest']}"
        )


def test_resolve_timings_keeps_previous_when_no_shard_reran():
    previous = {"com.example.FooTest": 3.0}
    result = resolve_timings([], {}, previous, previous_only_if_no_current=True)
    assert result == previous, f"Expected previous timings unchanged, got {result}"


def test_resolve_timings_without_current_or_previous_still_fails():
    try:
        resolve_timings([], {}, {}, previous_only_if_no_current=True)
        assert False, "Expected ValueError without current and previous timings"
    except ValueError as e:
        assert "No timing files matched" in str(e), f"Unexpected error: {e}"


def test_resolve_timings_without_flag_still_fails_on_no_current():
    try:
        resolve_timings([], {}, {"com.example.FooTest": 3.0})
        assert False, "Expected ValueError on a first attempt without timing files"
    except ValueError as e:
        assert "No timing files matched" in str(e), f"Unexpected error: {e}"


def test_resolve_timings_partial_rerun_overrides_only_rerun_classes():
    # "Re-run failed jobs": only the rerun shard's classes come from this attempt, the others
    # keep the previous attempt's timings, nothing is summed with the previous value.
    previous = {"com.example.FooTest": 3.0, "com.example.BarTest": 4.0}
    current = {"com.example.FooTest": 5.0}
    result = resolve_timings(["s1.json"], current, previous, previous_only_if_no_current=True)
    assert result == {"com.example.FooTest": 5.0, "com.example.BarTest": 4.0}, result


def test_load_previous_timings_tolerates_bad_input():
    import tempfile

    assert load_previous_timings("") == {}
    assert load_previous_timings("/nonexistent/timings.json") == {}
    with tempfile.TemporaryDirectory() as tmp:
        bad = os.path.join(tmp, "bad.json")
        with open(bad, "w", encoding="utf-8") as f:
            f.write("{not json")
        assert load_previous_timings(bad) == {}
        good = _write_timings(tmp, "good.json", {"com.example.FooTest": 1})
        assert load_previous_timings(good) == {"com.example.FooTest": 1.0}


if __name__ == "__main__":
    self_test()
    main()
