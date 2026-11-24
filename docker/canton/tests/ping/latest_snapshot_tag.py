"""Print the newest snapshot tag of a release line present in every given image.

Usage: latest_snapshot_tag.py <registry> <repo> <line> <image>...

Exits non-zero when that tag is older than MAX_AGE_DAYS, so a stalled snapshot
publish fails the nightly instead of re-testing the same old images.
"""

import http.client
import json
import sys
import time
import urllib.error
import urllib.request

# Upload gaps reached 7 days in 2026 with publishing healthy.
MAX_AGE_DAYS = 10


def fetch_manifests(registry: str, repo: str, image: str) -> list[dict]:
    """Return the image's manifests from the registry tags/list endpoint."""
    url = f"https://{registry}/v2/{repo}/{image}/tags/list"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                body = resp.read()
            return list(json.loads(body)["manifest"].values())
        except urllib.error.HTTPError as e:
            if e.code < 500 or attempt == 2:
                raise
        except (OSError, http.client.HTTPException):
            if attempt == 2:
                raise
        time.sleep(5)


def main() -> None:
    """Print the newest fresh snapshot tag, or exit non-zero."""
    registry, repo, line, *images = sys.argv[1:]
    prefix = line + "."
    uploaded = None
    for image in images:
        # Tag -> upload time in ms. Intersect so a half-finished publish is skipped.
        tags = {
            t: int(m["timeUploadedMs"])
            for m in fetch_manifests(registry, repo, image)
            for t in m["tag"]
            if t.startswith(prefix)
            and "-snapshot." in t
            and not t.endswith(("-pre", "-debug"))
        }
        uploaded = (
            tags
            if uploaded is None
            else {t: max(ms, uploaded[t]) for t, ms in tags.items() if t in uploaded}
        )
    if not uploaded:
        sys.exit(f"no {prefix}*-snapshot.* tag found in all of {images}")
    tag = max(uploaded, key=uploaded.get)
    age_days = (time.time() * 1000 - uploaded[tag]) / 86_400_000
    if age_days > MAX_AGE_DAYS:
        sys.exit(
            f"newest snapshot {tag} of {images} is {age_days:.1f} days old"
            f" (max {MAX_AGE_DAYS}), snapshot publishing may have stalled"
        )
    print(tag)


if __name__ == "__main__":
    main()
