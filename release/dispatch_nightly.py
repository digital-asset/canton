#!/usr/bin/env python3
"""Dispatch canton_nightly.yml for main and every supported release line.

The release lines come from release/supported-release-lines.txt. Each ref is
dispatched independently, so one failure does not skip the rest, and the script
exits non-zero if any dispatch failed.
"""

import argparse
import subprocess
import sys
from pathlib import Path

from update_dependabot import DEFAULT_MANIFEST, parse_manifest

WORKFLOW_FILE = "canton_nightly.yml"


def nightly_refs(manifest_text: str) -> list[str]:
    return ["main"] + [f"release-line-{v}" for v in parse_manifest(manifest_text)]


def dispatch(repo: str, ref: str) -> None:
    subprocess.run(
        ["gh", "workflow", "run", WORKFLOW_FILE, "--repo", repo, "--ref", ref], check=True
    )


def dispatch_all(repo: str, refs: list[str], dispatch=dispatch) -> list[str]:
    """Dispatch every ref, return the refs that failed."""
    failed = []
    for ref in refs:
        try:
            dispatch(repo, ref)
            print(f"Dispatched {WORKFLOW_FILE} on {ref}")
        except subprocess.CalledProcessError:
            print(f"::error::Dispatching {WORKFLOW_FILE} on {ref} failed")
            failed.append(ref)
    return failed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="owner/name of the repository")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args(argv)

    try:
        refs = nightly_refs(args.manifest.read_text())
        manifest_ok = True
    except SystemExit as err:
        # parse_manifest exits on a bad entry. Still run main's nightly.
        print(f"::error::{err.code}")
        refs, manifest_ok = ["main"], False
    print(f"Refs from {args.manifest.name}: {', '.join(refs)}")
    if dispatch_all(args.repo, refs) or not manifest_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
