#!/usr/bin/env python3
"""List the test classes declared in Scala test files that a PR adds.

The result feeds the CI job that runs new tests many times to catch flakes before merge.
A test class counts as new when its file is added in `base...head`. Renamed or moved files
are not new. The candidates are intersected with the test class list that sbt `printTests`
writes, so only concrete, runnable test classes are kept, and the ordinary-test exclude
patterns drop the tests that the PR test jobs do not run.

Prints JSON: {"selected": [...], "over_limit": [...]}, both sorted.
"""

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Sequence, Set

# `package a.b` statements, possibly chained over several lines.
_PACKAGE_RE = re.compile(r"^\s*package\s+([\w.]+)\s*$", re.MULTILINE)
# Class and object definitions, with any modifiers in front. This also matches definitions in
# comments and nested ones (with the wrong qualified name); the intersection with the sbt test
# class list drops those.
_DEFINITION_RE = re.compile(
    r"^\s*(?:(?:final|abstract|sealed|case|private(?:\[\w+\])?|protected(?:\[\w+\])?)\s+)*"
    r"(?:class|object)\s+(\w+)",
    re.MULTILINE,
)


def added_test_files(repo: Path, base: str, head: str) -> List[str]:
    """Scala test sources added between the merge base of `base` and `head`, and `head`.

    git detects a rename only above 50% similarity, so a file that is moved and heavily edited
    counts as added and its classes as new.
    """
    output = subprocess.check_output(
        ["git", "diff", "--name-only", "--diff-filter=A", "-M", f"{base}...{head}"],
        cwd=repo,
        text=True,
    )
    return [
        path
        for path in output.splitlines()
        if path.endswith(".scala") and "/src/test/" in f"/{path}"
    ]


def declared_class_names(source: str) -> Set[str]:
    """Fully qualified names of the classes and objects defined in a Scala source file."""
    package = ".".join(_PACKAGE_RE.findall(source))
    prefix = f"{package}." if package else ""
    return {prefix + name for name in _DEFINITION_RE.findall(source)}


def select_new_test_classes(
    added_sources: Dict[str, str],
    test_classes: Sequence[str],
    exclude_patterns: Sequence[str],
    max_classes: int,
) -> Dict[str, List[str]]:
    declared: Set[str] = set()
    for source in added_sources.values():
        declared |= declared_class_names(source)
    candidates = sorted(
        name
        for name in set(test_classes) & declared
        if not any(re.search(pattern, name) for pattern in exclude_patterns)
    )
    return {"selected": candidates[:max_classes], "over_limit": candidates[max_classes:]}


def _read_lines(path: Path) -> List[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", type=Path, default=Path("."), help="Git repository root")
    parser.add_argument("--base", required=True, help="Base ref, e.g. origin/main")
    parser.add_argument("--head", default="HEAD", help="Head ref")
    parser.add_argument(
        "--test-classes-file",
        type=Path,
        required=True,
        help="Test class names written by sbt printTests (test-full-class-names.log)",
    )
    parser.add_argument(
        "--exclude-patterns-file",
        type=Path,
        required=True,
        help="Regex patterns, one per line, of test classes to drop",
    )
    parser.add_argument(
        "--max-classes", type=int, required=True, help="Maximum number of classes to select"
    )
    args = parser.parse_args(argv)
    if args.max_classes < 0:
        parser.error("--max-classes must not be negative")

    added_sources = {
        path: (args.repo / path).read_text(encoding="utf-8")
        for path in added_test_files(args.repo, args.base, args.head)
    }
    result = select_new_test_classes(
        added_sources,
        _read_lines(args.test_classes_file),
        _read_lines(args.exclude_patterns_file),
        args.max_classes,
    )
    json.dump(result, sys.stdout)
    print()


def test_declared_class_names_reads_modifiers_and_chained_packages() -> None:
    source = """
package com.digitalasset.canton
package integration.tests

import org.scalatest.Assertion

sealed abstract class FooIntegrationTest extends CommunityIntegrationTest {
  private object Helper
}

final class FooIntegrationTestPostgres extends FooIntegrationTest
class FooIntegrationTestH2 extends FooIntegrationTest
private[tests] trait NotAClass
"""
    prefix = "com.digitalasset.canton.integration.tests."
    assert declared_class_names(source) == {
        prefix + "FooIntegrationTest",
        prefix + "Helper",
        prefix + "FooIntegrationTestPostgres",
        prefix + "FooIntegrationTestH2",
    }


def test_select_keeps_runnable_unexcluded_classes_and_applies_limit() -> None:
    sources = {
        "a/src/test/scala/A.scala": "package p\nfinal class ATest\nclass BTest\nclass CTest",
        "a/src/test/scala/M.scala": "package p.manual\nclass ManualTest",
    }
    test_classes = ["p.ATest", "p.BTest", "p.CTest", "p.manual.ManualTest", "p.OldTest", "p.ATest"]
    result = select_new_test_classes(sources, test_classes, [r"p\.manual"], max_classes=2)
    assert result == {"selected": ["p.ATest", "p.BTest"], "over_limit": ["p.CTest"]}


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def _write(repo: Path, path: str, content: str) -> None:
    (repo / path).parent.mkdir(parents=True, exist_ok=True)
    (repo / path).write_text(content, encoding="utf-8")


def test_main_lists_only_classes_from_added_test_files(capsys) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp)
        _git(repo, "init", "-q", "-b", "main")
        _git(repo, "config", "user.email", "ci@example.com")
        _git(repo, "config", "user.name", "ci")
        old_body = "package p\nclass MovedTest extends AnyWordSpec {\n" + "  // body\n" * 20 + "}\n"
        _write(repo, "app/src/test/scala/p/MovedTest.scala", old_body)
        _write(repo, "app/src/test/scala/p/ChangedTest.scala", "package p\nclass ChangedTest\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-q", "-m", "base")
        _git(repo, "checkout", "-q", "-b", "pr")
        _git(repo, "mv", "app/src/test/scala/p/MovedTest.scala", "app/src/test/scala/q.scala")
        _write(repo, "app/src/test/scala/p/ChangedTest.scala", "package p\nclass ChangedTest2\n")
        _write(repo, "app/src/test/scala/p/NewTest.scala", "package p\nfinal class NewTest\n")
        _write(repo, "app/src/main/scala/p/NotATest.scala", "package p\nclass NotATest\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-q", "-m", "pr")
        _write(repo, "classes.log", "p.MovedTest\np.ChangedTest2\np.NewTest\np.NotATest\n")
        _write(repo, "exclude.txt", "")

        main(
            [
                f"--repo={repo}",
                "--base=main",
                "--head=pr",
                f"--test-classes-file={repo / 'classes.log'}",
                f"--exclude-patterns-file={repo / 'exclude.txt'}",
                "--max-classes=5",
            ]
        )

    assert json.loads(capsys.readouterr().out) == {"selected": ["p.NewTest"], "over_limit": []}


if __name__ == "__main__":
    main()
