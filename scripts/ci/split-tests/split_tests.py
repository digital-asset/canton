#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
import tempfile
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

DEFAULT_EXCLUDE_PATTERNS = [
    r'com\.digitalasset\.canton\.integration\.tests\.manual',
    r'com\.digitalasset\.canton\.integration\.tests\.crashrecovery',
    r'com\.digitalasset\.canton\.nightly',
    r'com\.digitalasset\.canton\.integration\.tests\.nightly',
    r'com\.digitalasset\.canton\.integration\.tests\.toxiproxy',
    r'com\.digitalasset\.canton\.integration\.tests\.release',
    r'com\.digitalasset\.canton\.integration\.tests\.benchmarks',
    r'com\.digitalasset\.canton\.integration\.tests\.continuity',
    r'com\.digitalasset\.canton\.integration\.tests\.variations',
    r'com\.digitalasset\.canton\.integration\.tests\.modelbased',
    r'com\.digitalasset\.canton\.integration\.tests\.upgrade\.MajorUpgrade.*Writer.*',
    r'com\.digitalasset\.canton\.integration\.tests\.UpgradesMatrixIntegration'
    ]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Split tests evenly across shards")
    parser.add_argument("total", type=int, help="Total number of shards")
    parser.add_argument("shard", type=int, help="Current shard index")
    parser.add_argument("test_file", help="File with test class names")
    parser.add_argument("timings_file", help="JSON file with historical timings (or empty string)")
    parser.add_argument("fallback_test_time", type=float, help="Default duration for unknown tests (seconds)")
    parser.add_argument("output_file", help="Output file for GitHub Actions GITHUB_OUTPUT")
    parser.add_argument(
        "--exclude-patterns",
        nargs="*",
        default=None,
        help="Regex patterns to exclude (one per argument). Uses built-in defaults when not provided.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print shard distribution debug output.",
    )
    return parser.parse_args(argv)


def validate_inputs(total: int, shard: int, test_file: str) -> None:
    if total <= 0:
        raise ValueError("total must be > 0")
    if shard < 0 or shard >= total:
        raise ValueError(f"shard must satisfy 0 <= shard < total (got shard={shard}, total={total})")
    if not os.path.isfile(test_file):
        raise ValueError(f"Test file not found: {test_file}")


def resolve_exclude_patterns(raw_patterns: Optional[Sequence[str]]) -> Sequence[str]:
    return raw_patterns if raw_patterns is not None else DEFAULT_EXCLUDE_PATTERNS


def load_test_names(test_file: str) -> List[str]:
    with open(test_file, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def filter_tests(tests: Iterable[str], exclude_patterns: Sequence[str]) -> List[str]:
    filtered: List[str] = []
    for test_name in tests:
        if any(re.search(pattern, test_name) for pattern in exclude_patterns):
            continue
        filtered.append(test_name)
    return filtered


def load_timings(timings_file: str) -> Dict[str, float]:
    if not timings_file or not os.path.isfile(timings_file):
        return {}
    with open(timings_file, encoding="utf-8") as f:
        payload = json.load(f)
    return {name: float(duration) for name, duration in payload.items()}


def build_weighted_items(filtered_tests: Iterable[str], timings: Dict[str, float], fallback: float) -> List[Tuple[str, float]]:
    items = [(test_name, float(timings.get(test_name, fallback))) for test_name in filtered_tests]
    items.sort(key=lambda pair: pair[1], reverse=True)
    return items


def assign_to_shards(items: Iterable[Tuple[str, float]], total_shards: int) -> Tuple[List[List[str]], List[float]]:
    buckets: List[List[str]] = [[] for _ in range(total_shards)]
    bucket_times = [0.0] * total_shards
    for test_name, duration in items:
        shard_index = min(range(total_shards), key=lambda idx: bucket_times[idx])
        buckets[shard_index].append(test_name)
        bucket_times[shard_index] += duration
    return buckets, bucket_times


def print_debug_distribution(total: int, shard: int, buckets: Sequence[Sequence[str]], bucket_times: Sequence[float]) -> None:
    print("\n" + "=" * 80)
    print("TEST SHARD DISTRIBUTION DEBUG")
    print("=" * 80)
    total_time = sum(bucket_times)
    for idx, (bucket, bucket_time) in enumerate(zip(buckets, bucket_times)):
        percentage = (bucket_time / total_time * 100) if total_time > 0 else 0
        print(f"Shard {idx:2d}: {len(bucket):3d} tests, {bucket_time:8.2f}s ({percentage:5.1f}%)")
    print("-" * 80)
    print(f"Total across all shards: {total_time:.2f}s")
    print(f"Average per shard: {total_time / total:.2f}s")
    print(f"Selected shard ({shard}): {len(buckets[shard])} tests, {bucket_times[shard]:.2f}s")
    print("=" * 80 + "\n")


def write_output(output_file: str, selected: Sequence[str], total_tests: int) -> None:
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"tests_per_shard={len(selected)}\n")
        f.write(f"total_tests={total_tests}\n")
        f.write("selected_tests=" + " ".join(selected) + "\n")


def test_validate_inputs_rejects_invalid_total() -> None:
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as test_file:
        test_file.write("com.example.TestSuite\n")
        path = test_file.name
    try:
        try:
            validate_inputs(0, 0, path)
            raise AssertionError("Expected ValueError for total <= 0")
        except ValueError as exc:
            assert "total must be > 0" in str(exc)
    finally:
        os.remove(path)


def test_validate_inputs_rejects_invalid_shard() -> None:
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as test_file:
        test_file.write("com.example.TestSuite\n")
        path = test_file.name
    try:
        try:
            validate_inputs(2, 2, path)
            raise AssertionError("Expected ValueError for out-of-range shard")
        except ValueError as exc:
            assert "0 <= shard < total" in str(exc)
    finally:
        os.remove(path)


def test_filter_tests_applies_regex_patterns() -> None:
    tests = [
        "com.digitalasset.canton.integration.tests.manual.SomeSuite",
        "com.digitalasset.canton.integration.tests.SomeRegularSuite",
    ]
    filtered = filter_tests(tests, DEFAULT_EXCLUDE_PATTERNS)
    assert filtered == ["com.digitalasset.canton.integration.tests.SomeRegularSuite"]


def test_load_timings_returns_empty_for_missing_file() -> None:
    timings = load_timings("/tmp/does-not-exist-timings.json")
    assert timings == {}


def test_assign_to_shards_balances_largest_first() -> None:
    items = [("a", 10.0), ("b", 8.0), ("c", 4.0), ("d", 2.0)]
    _, bucket_times = assign_to_shards(items, 2)
    assert bucket_times[0] == 12.0
    assert bucket_times[1] == 12.0


def test_write_output_format() -> None:
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as out_file:
        output_path = out_file.name
    try:
        write_output(output_path, ["A", "B"], 5)
        with open(output_path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        assert lines[0] == "tests_per_shard=2"
        assert lines[1] == "total_tests=5"
        assert lines[2] == "selected_tests=A B"
    finally:
        os.remove(output_path)


def test_full_flow_helpers() -> None:
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as test_file:
        test_file.write("suite.A\n")
        test_file.write("suite.B\n")
        test_file.write("suite.C\n")
        test_path = test_file.name
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as timings_file:
        json.dump({"suite.A": 10, "suite.B": 1}, timings_file)
        timings_path = timings_file.name
    try:
        validate_inputs(2, 0, test_path)
        tests = load_test_names(test_path)
        filtered = filter_tests(tests, [])
        timings = load_timings(timings_path)
        items = build_weighted_items(filtered, timings, 5.0)
        buckets, _ = assign_to_shards(items, 2)
        assert "suite.A" in buckets[0] + buckets[1]
        assert len(buckets[0]) + len(buckets[1]) == 3
    finally:
        os.remove(test_path)
        os.remove(timings_path)


def self_test() -> None:
    test_validate_inputs_rejects_invalid_total()
    test_validate_inputs_rejects_invalid_shard()
    test_filter_tests_applies_regex_patterns()
    test_load_timings_returns_empty_for_missing_file()
    test_assign_to_shards_balances_largest_first()
    test_write_output_format()
    test_full_flow_helpers()
    print("All self-checks passed")

def main():
    args = parse_args()

    try:
        validate_inputs(args.total, args.shard, args.test_file)
    except ValueError as exc:
        raise SystemExit(f"error: {exc}")

    exclude_patterns = resolve_exclude_patterns(args.exclude_patterns)

    tests = load_test_names(args.test_file)
    filtered = filter_tests(tests, exclude_patterns)
    timings = load_timings(args.timings_file)
    items = build_weighted_items(filtered, timings, args.fallback_test_time)
    buckets, bucket_times = assign_to_shards(items, args.total)

    selected = buckets[args.shard]

    if args.debug:
        print_debug_distribution(args.total, args.shard, buckets, bucket_times)

    write_output(args.output_file, selected, len(filtered))

if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        self_test()
    else:
        main()
