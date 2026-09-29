#!/usr/bin/env python3
"""Sync .github/dependabot.yml to the set of supported release lines.

Supported release lines are listed, one `major.minor` per line, in
`release/supported-release-lines.txt` (the source of truth, also intended to
drive scheduled CI workflows). This rewrites dependabot.yml so the per-line
entries cover exactly those lines: for each supported line it appends a clone of
the covered base entries (PER_LINE_ECOSYSTEMS, docker for now) with
`target-branch: "release-line-X.Y"`, and drops per-line entries for lines that
are no longer listed (ramp-down).

To change coverage, edit the manifest and re-run this script. The file is edited
as text, not via a YAML library: the nix env ships no YAML lib, and a full
re-dump would reformat the file and drop its inline comment.
"""

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FILE = ROOT / ".github" / "dependabot.yml"
DEFAULT_MANIFEST = ROOT / "release" / "supported-release-lines.txt"

# A per-line entry carries a `target-branch:` *key* (4-space indent), not merely
# the substring somewhere in a comment or value.
TARGET_BRANCH_KEY = re.compile(r"^    target-branch:", re.MULTILINE)
INTERVAL_LINE = re.compile(r"^      interval:.*$", re.MULTILINE)
RELEASE_LINE = re.compile(r"^\d+\.\d+$")
ECOSYSTEM = re.compile(r'package-ecosystem:\s*"([^"]+)"')

# Ecosystems a release line is covered for. Docker only for now: stable lines
# want base-image security updates, not sbt/library or github-actions bumps that
# would change behaviour on a maintenance branch.
PER_LINE_ECOSYSTEMS = ("docker",)


def _ecosystem(entry: str) -> str:
    m = ECOSYSTEM.search(entry)
    return m.group(1) if m else ""


def parse_manifest(text: str) -> list[str]:
    """Return the supported release lines, de-duplicated and sorted numerically."""
    versions = []
    for raw in text.splitlines():
        item = raw.split("#", 1)[0].strip()
        if not item:
            continue
        if not RELEASE_LINE.match(item):
            sys.exit(f"error: invalid release line in manifest: {item!r} (expected major.minor)")
        versions.append(item)
    return sorted(set(versions), key=lambda v: tuple(int(p) for p in v.split(".")))


def rewrite(text: str, supported_lines: list[str]) -> str:
    """Return dependabot.yml content covering exactly ``supported_lines``."""
    head, sep, body = text.partition("\nupdates:\n")
    if not sep:
        sys.exit("error: dependabot.yml has no `updates:` list")

    entries = re.split(r"\n\n+(?=  - package-ecosystem:)", body.strip("\n"))
    base = [e for e in entries if not TARGET_BRANCH_KEY.search(e)]
    if not base:
        sys.exit("error: no base entries (without target-branch) found in dependabot.yml")

    # Release lines are covered only for PER_LINE_ECOSYSTEMS (docker for now).
    covered = [e for e in base if _ecosystem(e) in PER_LINE_ECOSYSTEMS]
    if supported_lines and not covered:
        sys.exit(
            f"error: none of the per-line ecosystems {PER_LINE_ECOSYSTEMS} "
            "found among the base entries"
        )

    # Rebuild the list, placing each supported line's clone right after the base
    # entry it copies, so per-line entries stay grouped with their ecosystem. A
    # function replacement keeps the branch value literal (no backslash/group
    # expansion), and injects target-branch right after the interval line.
    out_entries = []
    for entry in base:
        out_entries.append(entry)
        if _ecosystem(entry) not in PER_LINE_ECOSYSTEMS:
            continue
        for version in supported_lines:
            branch = f"release-line-{version}"
            clone = INTERVAL_LINE.sub(
                lambda m: f'{m.group(0)}\n    target-branch: "{branch}"',
                entry,
                count=1,
            )
            if not TARGET_BRANCH_KEY.search(clone):
                sys.exit("error: a base entry has no `interval:` line to anchor target-branch")
            out_entries.append(clone)

    return head + sep + "\n\n".join(out_entries) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="File listing supported release lines (one major.minor per line)",
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=DEFAULT_FILE,
        help="Path to dependabot.yml",
    )
    args = parser.parse_args(argv)

    supported = parse_manifest(args.manifest.read_text())
    args.file.write_text(rewrite(args.file.read_text(), supported))
    listed = ", ".join(f"release-line-{v}" for v in supported) or "none"
    print(f"Synced {args.file} to supported lines: {listed}.")


if __name__ == "__main__":
    main()
