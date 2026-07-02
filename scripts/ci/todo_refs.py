#!/usr/bin/env python3
"""Single source of truth for "does a TODO/XXX/FIXME reference issue #N?".

Three CI checks need to know whether the codebase still contains a TODO/XXX/FIXME
that references a given GitHub issue:

  * .circleci/todo-script/src/checkTodos.sc      (PR-gate todo checker)
  * .github/workflows/check_todos_on_issue_close.yml  (reopen on stale reference)
  * scripts/ci/close_stale_flake_issues.py       (stale-flake closer)

This module owns the things those checks must agree on: the TODO keywords, the
issue-reference forms, and the exclude set. The Scala checker only consumes the
exclude set (via `excludes --format=grep`); the other two use the per-issue
reference check (Python import / `references <N>` CLI).

Usage:
    todo_refs.py references <issue-number>
        Print matching "path:line:text" lines to stdout.
        Exit 0 if any TODO references the issue, 1 if none, 2 on ripgrep error.

    todo_refs.py excludes --format={grep,rg}
        Print the exclude arguments (one per line) for grep or ripgrep.
"""

import argparse
import subprocess
import sys

# This utility intentionally scopes to DACH-NY/canton only. Its two per-issue
# consumers (check_todos_on_issue_close.yml and close_stale_flake_issues.py)
# only ever operate on DACH-NY/canton. References to OSS (digital-asset/canton)
# and Daml (digital-asset/daml) issues are handled exclusively by checkTodos.sc,
# which keeps its own matching and only consumes the shared exclude set here.
REPO = "DACH-NY/canton"
TODO_KEYWORDS = ("TODO", "XXX", "FIXME")

# Canonical exclude set, the single source of truth for all three checks. This
# is the union of the lists previously duplicated in checkTodos.sc and
# todo-exclude-globs.txt. Keep both lists sorted and deduplicated.
#
# Directories are matched by name at any depth; file globs are matched against
# the file *basename* (so "checkTodos.sc" is an exact name match, "*.png" is a
# glob). Files that themselves embed TODO-reference literals (this module, the
# checkers, the test file) are excluded so the checks never match their own
# source.
#
# Note: .github and .direnv are intentionally NOT excluded here. The two per-issue
# consumers (issue-close workflow, stale-flake closer) scan .github so that TODOs
# in workflow files are caught when their issue is closed. checkTodos.sc (the PR
# gate) excludes them itself, because .github/actions/build/todo_checker/action.yml
# carries descriptive "TODO/FIXME" text with no issue reference that would
# otherwise be flagged as an ownerless TODO. See issue #32941.
EXCLUDE_DIRS = (
    ".git",
    ".idea",
    "3rdparty",
    "build",
    "contributing",
    "daml",
    "docs",
    "docs-open",
    "lib",
    "log",
    "release-notes",
    "target",
    "theory",
    "todo-out",
    "todo-script",
)
EXCLUDE_FILE_GLOBS = (
    "*.png",
    "UNRELEASED.md",
    "checkTodos.sc",
    "check_todos_on_issue_close.yml",
    "close_stale_flake_issues.py",
    "close_stale_flake_issues.yml",
    "test_todo_refs.py",
    "todo_refs.py",
)


def rg_exclude_args() -> list[str]:
    """Exclude arguments in ripgrep glob form (`--glob=!<pat>`)."""
    return [f"--glob=!{pat}" for pat in (*EXCLUDE_DIRS, *EXCLUDE_FILE_GLOBS)]


def grep_exclude_args() -> list[str]:
    """Exclude arguments in GNU grep form (`--exclude-dir`/`--exclude`)."""
    return [f"--exclude-dir={d}" for d in EXCLUDE_DIRS] + [
        f"--exclude={g}" for g in EXCLUDE_FILE_GLOBS
    ]


def reference_pattern(number: int) -> str:
    """Regex matching a TODO/XXX/FIXME line that references canton issue ``number``.

    Matches the reference anywhere on the line after the keyword, in any of the
    forms used across the repo: a full GitHub URL, the short ``#N`` form, or the
    ``iN`` form. ``([^0-9]|$)`` ensures ``#123`` does not match inside ``#1234``.
    """
    number = int(number)  # reject non-numeric input so it cannot alter the regex
    keywords = "|".join(TODO_KEYWORDS)
    return (
        rf"({keywords}).*("
        rf"(https://github\.com/)?{REPO}/issues/{number}([^0-9]|$)"
        rf"|#{number}([^0-9]|$)"
        rf"|[^a-zA-Z0-9]i{number}([^0-9]|$)"
        rf")"
    )


def _repo_root() -> str:
    return subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()


def _run_rg(number: int, path: str, cwd: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["rg", "-n", "--hidden", *rg_exclude_args(), reference_pattern(number), path],
        capture_output=True,
        text=True,
        cwd=cwd,
    )


def find_todo_references(number: int, root: str | None = None) -> list[str]:
    """Return the matching ``path:line:text`` lines, empty if none reference ``number``."""
    result = _run_rg(number, root or _repo_root())
    if result.returncode == 2:
        raise RuntimeError(f"rg error searching for TODO references: {result.stderr.strip()}")
    return result.stdout.splitlines()


def has_todo_reference(number: int, root: str | None = None) -> bool:
    """Return True if any TODO/XXX/FIXME in the codebase references issue ``number``."""
    return bool(find_todo_references(number, root))


def _cmd_references(number: int) -> int:
    # Run ripgrep with the repo root as cwd and search ".", so matches print
    # repo-relative without mutating this process's working directory. Treat a
    # failure to locate the repo root as an error (exit 2), not "no references"
    # (exit 1), so callers do not silently miss matches.
    try:
        root = _repo_root()
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(f"could not determine repo root: {exc}\n")
        return 2
    result = _run_rg(number, ".", cwd=root)
    sys.stdout.write(result.stdout)
    if result.returncode == 2:
        sys.stderr.write(result.stderr)
        return 2
    return 0 if result.stdout.strip() else 1


def _cmd_excludes(fmt: str) -> int:
    args = grep_exclude_args() if fmt == "grep" else rg_exclude_args()
    print("\n".join(args))
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_ref = sub.add_parser("references", help="check whether a TODO references issue N")
    p_ref.add_argument("number", type=int)

    p_exc = sub.add_parser("excludes", help="print exclude args for grep or ripgrep")
    p_exc.add_argument("--format", choices=("grep", "rg"), required=True)

    args = parser.parse_args(argv)
    # Any unexpected failure (e.g. ripgrep missing, git failure) must surface as
    # exit 2, never as exit 1, so callers cannot mistake an error for "no matches".
    try:
        if args.command == "references":
            return _cmd_references(args.number)
        return _cmd_excludes(args.format)
    except Exception as exc:  # noqa: BLE001 - deliberate catch-all backstop
        sys.stderr.write(f"todo_refs.py {args.command} failed: {exc}\n")
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
