"""Classify a Git tree diff for the required CI workflow."""

import subprocess
import sys


def is_docs_path(path: bytes) -> bool:
    return (
        path in {b"UNRELEASED.md", b"CONTRIBUTING.md"}
        or (path.startswith(b"release-notes/") and path.endswith(b".md"))
        or (path.startswith(b"contributing/") and path.endswith((b".md", b".png")))
    )


def is_docs_only(base: str) -> bool:
    # Treat renames as a delete and an add so both paths must be allowed.
    diff = subprocess.check_output(
        [
            "git",
            "diff-tree",
            "--no-commit-id",
            "--no-renames",
            "--ignore-submodules=none",
            "-r",
            "-z",
            base,
            "HEAD",
            "--",
        ]
    )
    if not diff:
        return False

    fields = diff[:-1].split(b"\0")
    for metadata, path in zip(fields[::2], fields[1::2], strict=True):
        # Allow ordinary files and the missing side of additions/deletions.
        old_mode, new_mode, *_ = metadata[1:].split()
        if {old_mode, new_mode} - {b"100644", b"000000"} or not is_docs_path(path):
            return False
    return True


if __name__ == "__main__":
    print("true" if is_docs_only(sys.argv[1]) else "false")
