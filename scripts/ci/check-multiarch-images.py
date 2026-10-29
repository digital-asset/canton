#!/usr/bin/env python3

# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Fail if any given container image does not cover all required platforms."""

import json
import subprocess
import sys

REQUIRED_PLATFORMS: frozenset[str] = frozenset({"linux/amd64", "linux/arm64"})


def main(argv: list[str] | None = None) -> int:
    refs = sys.argv[1:] if argv is None else list(argv)
    if not refs:
        print(
            "Usage: check-multiarch-images.py <image-ref> [<image-ref> ...]",
            file=sys.stderr,
        )
        return 2

    failures = 0
    for ref in refs:
        print(f"Inspecting {ref}")
        if _inspect(ref):
            print(f"  OK: {ref} covers {sorted(REQUIRED_PLATFORMS)}")
        else:
            print(f"ERROR: {ref} is not a multi-arch image covering {sorted(REQUIRED_PLATFORMS)}")
            failures += 1

    if failures > 0:
        print(f"FAIL: {failures} image(s) failed the multi-arch check")
        return 1

    print("OK: all inspected images are multi-arch")
    return 0


def _inspect(ref: str) -> bool:
    try:
        raw = subprocess.run(
            ["docker", "buildx", "imagetools", "inspect", "--raw", ref],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except FileNotFoundError:
        # failure-surfaced: printed to stderr and causes main() to return non-zero
        print("ERROR: `docker` not found on PATH", file=sys.stderr)
        return False
    except subprocess.CalledProcessError as e:
        # failure-surfaced: printed to stderr and causes main() to return non-zero
        print(f"ERROR: unable to inspect {ref}: {e.stderr.strip() or e}", file=sys.stderr)
        return False

    try:
        manifest = json.loads(raw)
    except json.JSONDecodeError as e:
        # failure-surfaced: printed to stderr and causes main() to return non-zero
        print(f"ERROR: invalid manifest JSON for {ref}: {e}", file=sys.stderr)
        return False

    return _is_multi_arch_manifest(manifest)


def _manifest_platforms(manifest: dict) -> set[str]:
    """Extract `os/arch` entries from a manifest list/index, ignoring attestations."""
    manifests = manifest.get("manifests")
    if not isinstance(manifests, list):
        return set()
    platforms: set[str] = set()
    for entry in manifests:
        if not isinstance(entry, dict):
            continue
        annotations = entry.get("annotations")
        if (
            isinstance(annotations, dict)
            and annotations.get("vnd.docker.reference.type") == "attestation-manifest"
        ):
            continue
        platform = entry.get("platform")
        if not isinstance(platform, dict):
            continue
        os_name = platform.get("os")
        arch = platform.get("architecture")
        if not os_name or not arch or os_name == "unknown" or arch == "unknown":
            continue
        platforms.add(f"{os_name}/{arch}")
    return platforms


def _is_multi_arch_manifest(manifest: dict) -> bool:
    return REQUIRED_PLATFORMS.issubset(_manifest_platforms(manifest))


if __name__ == "__main__":
    sys.exit(main())
