#!/usr/bin/env python3

import argparse
import glob
import json
import os
import xml.etree.ElementTree as ET


def parse_bool(value: str) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes"}


def load_failed_classes(summary_path: str) -> list[str]:
    if not os.path.exists(summary_path):
        return []
    with open(summary_path, encoding="utf-8") as f:
        summary = json.load(f)
    return summary.get("results", {}).get("not_passed_classes", [])


def load_selected_classes(selected_tests_file: str) -> list[str]:
    if not selected_tests_file or not os.path.isfile(selected_tests_file):
        return []
    with open(selected_tests_file, encoding="utf-8") as f:
        return [name.strip() for name in f.read().split() if name.strip()]


def discover_executed_classes(junit_glob: str) -> set[str]:
    executed_classes = set()
    for report_path in glob.glob(junit_glob, recursive=True):
        if not os.path.isfile(report_path):
            continue
        try:
            root = ET.parse(report_path).getroot()
        except ET.ParseError:
            continue
        for testcase in root.findall(".//testcase"):
            classname = testcase.attrib.get("classname", "").strip()
            if classname:
                executed_classes.add(classname)
    return executed_classes


def dedup_non_empty(classnames: list[str]) -> list[str]:
    normalized = []
    seen = set()
    for classname in classnames:
        classname = (classname or "").strip()
        if classname and classname not in seen:
            seen.add(classname)
            normalized.append(classname)
    return normalized


def select_rerun_classes(
    failed_classes: list[str],
    selected_classes: list[str],
    executed_classes: set[str],
    testcase_timeout_triggered: bool,
) -> list[str]:
    rerun_classes = dedup_non_empty(failed_classes)
    seen = set(rerun_classes)

    if testcase_timeout_triggered and selected_classes:
        remaining_classes = [
            classname for classname in selected_classes if classname not in executed_classes
        ]
        for classname in remaining_classes:
            if classname not in seen:
                seen.add(classname)
                rerun_classes.append(classname)

    # When the watchdog fires, rerun failed classes plus any selected classes not present in JUnit.
    return rerun_classes


def build_resolved_command(
    command_template: str,
    test_sub_command: str,
    rerun_classes: list[str],
    num_test_buckets: int,
) -> str:
    if not rerun_classes:
        return ""

    rerun_command = f"{test_sub_command} {' '.join(rerun_classes)}"
    sharding_enabled = num_test_buckets > 0
    if sharding_enabled and "$RUN_SPLITTED_TESTS_CMD" not in command_template:
        raise ValueError(
            "COMMAND_TEMPLATE must contain $RUN_SPLITTED_TESTS_CMD when failed-only rerun is "
            "needed in sharded mode"
        )

    # In sharded mode, quoting is owned by COMMAND_TEMPLATE around $RUN_SPLITTED_TESTS_CMD.
    # In non-sharded mode, quote the direct rerun command as a single shell token.
    if "$RUN_SPLITTED_TESTS_CMD" in command_template:
        return command_template.replace("$RUN_SPLITTED_TESTS_CMD", rerun_command)
    return f'"{rerun_command}"'


def write_lines(path: str, lines: list[str]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(f"{line}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare failed-only rerun command")
    parser.add_argument("--summary-json", required=True)
    parser.add_argument("--failed-test-classes-file", required=True)
    parser.add_argument("--resolved-command-file", required=True)
    parser.add_argument("--has-failed-test-classes-file", required=True)
    parser.add_argument("--failed-test-classes-count-file", required=True)
    parser.add_argument("--num-test-buckets", default="1")
    parser.add_argument("--command-template", required=True)
    parser.add_argument("--test-sub-command", required=True)
    parser.add_argument("--selected-tests-file", default="")
    parser.add_argument("--testcase-timeout-triggered", default="false")
    parser.add_argument("--junit-glob", default="**/TEST-*.xml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        num_test_buckets = int(args.num_test_buckets)
    except ValueError:
        num_test_buckets = 1

    testcase_timeout_triggered = parse_bool(args.testcase_timeout_triggered)
    failed_classes = load_failed_classes(args.summary_json)
    selected_classes = load_selected_classes(args.selected_tests_file)
    executed_classes = discover_executed_classes(args.junit_glob)

    rerun_classes = select_rerun_classes(
        failed_classes=failed_classes,
        selected_classes=selected_classes,
        executed_classes=executed_classes,
        testcase_timeout_triggered=testcase_timeout_triggered,
    )

    write_lines(args.failed_test_classes_file, rerun_classes)

    resolved_command = build_resolved_command(
        command_template=args.command_template,
        test_sub_command=args.test_sub_command,
        rerun_classes=rerun_classes,
        num_test_buckets=num_test_buckets,
    )

    with open(args.resolved_command_file, "w", encoding="utf-8") as f:
        f.write(resolved_command)
    with open(args.has_failed_test_classes_file, "w", encoding="utf-8") as f:
        f.write("true" if rerun_classes else "false")
    with open(args.failed_test_classes_count_file, "w", encoding="utf-8") as f:
        f.write(str(len(rerun_classes)))

    if rerun_classes:
        if testcase_timeout_triggered:
            print(
                f"Prepared timeout-recovery rerun for {len(rerun_classes)} classes "
                f"({len(executed_classes)} executed, {len(selected_classes)} selected)"
            )
        else:
            print(f"Prepared failed-only rerun for {len(rerun_classes)} test classes")
    else:
        print("No failed test classes found, skipping failed-only rerun")


if __name__ == "__main__":
    main()
