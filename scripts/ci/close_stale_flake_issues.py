#!/usr/bin/env python3
"""Close flaky-test GitHub issues that have not recorded a failure in the last STALE_DAYS days."""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

from todo_refs import has_todo_reference

REPO = "DACH-NY/canton"
STALE_DAYS = 30
SKIP_LABELS = frozenset({"ignored", "disabled"})
_TRANSIENT_HTTP_CODES = frozenset({"502", "503", "504"})
_RETRY_ATTEMPTS = 3
_RETRY_DELAY_SECONDS = 5

# Inline paginated variant of scripts/list-flaky-test-issues.graphql.
# The original query is kept unchanged since other scripts depend on it.
_QUERY = """
query($after: String) {
  repository(owner: "DACH-NY", name: "canton") {
    milestone(number: 31) {
      issues(first: 100, states: [OPEN], after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          ... on Issue { title number body assignees(first: 10) { nodes { login } } labels(first: 20) { nodes { name } } }
        }
      }
    }
  }
}
"""


def run_gh(*args: str, retries: int = _RETRY_ATTEMPTS) -> subprocess.CompletedProcess:
    result = subprocess.run(["gh", *args], capture_output=True, text=True)
    if result.returncode != 0:
        if retries > 0 and any(c in result.stderr + result.stdout for c in _TRANSIENT_HTTP_CODES):
            print(f"Transient gh error, retrying in {_RETRY_DELAY_SECONDS}s... ({retries} left)")
            time.sleep(_RETRY_DELAY_SECONDS)
            return run_gh(*args, retries=retries - 1)
        raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
    return result


def list_open_flake_issues() -> list[dict]:
    issues: list[dict] = []
    after: str | None = None
    while True:
        args = ["api", "graphql", "-F", f"query={_QUERY}"]
        if after:
            args += ["-F", f"after={after}"]
        result = run_gh(*args)
        payload = json.loads(result.stdout)
        if payload.get("errors"):
            raise RuntimeError(f"GraphQL error: {payload['errors']}")
        milestone = payload["data"]["repository"]["milestone"]
        if milestone is None:
            raise RuntimeError("Milestone not found; check the milestone number in _QUERY.")
        page = milestone["issues"]
        issues.extend(page["nodes"])
        if not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]
    return issues


def last_flake_date(body: str | None) -> datetime | None:
    if not body:
        return None
    # New table format:  | 2026-03-01 10:23:45 | job | node | build | commit |
    new_fmt = re.findall(r'^\|\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|', body, re.MULTILINE)
    # Old format:        2024-05-10 13:34:53 job:sequential_test node_index:3 url:...
    old_fmt = re.findall(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+job:', body, re.MULTILINE)
    dates = new_fmt + old_fmt
    if not dates:
        return None
    parsed = [datetime.strptime(d, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc) for d in dates]
    return max(parsed)


def close_issue(number: int, last_date: datetime, assignees: list[str], dry_run: bool) -> None:
    mention = ""
    if assignees:
        handles = " ".join(f"@{a}" for a in assignees)
        mention = (
            f"({handles}: you are assigned to this issue. "
            f"If you are still investigating, feel free to reopen it.) "
        )
    comment = (
        f"Closing automatically: no flake recorded in the last {STALE_DAYS} days "
        f"(last failure: {last_date.date()}). {mention}"
        f"If this test flakes again it will be reopened by the CI system."
    )
    if dry_run:
        print(f"    [dry-run] would close with comment: {comment}")
        if assignees:
            print(f"    [dry-run] would unassign: {', '.join(assignees)}")
        return
    if assignees:
        run_gh(
            "issue", "edit", str(number), "--repo", REPO,
            "--remove-assignee", ",".join(assignees),
        )
    run_gh("issue", "close", str(number), "--repo", REPO, "--comment", comment)


def main(dry_run: bool) -> None:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=STALE_DAYS)
    issues = list_open_flake_issues()
    print(f"{len(issues)} open flake issues. Cutoff: {cutoff.date()} ({STALE_DAYS} days ago).")

    closed: list[str] = []
    skipped_label: list[str] = []
    skipped_no_date: list[str] = []
    skipped_todo: list[str] = []
    kept_open = 0
    for issue in issues:
        number = issue["number"]
        title = issue["title"]
        labels = {l["name"] for l in issue.get("labels", {}).get("nodes", [])}
        skip_matches = labels & SKIP_LABELS
        if skip_matches:
            print(f"  #{number} {title}: has label(s) {sorted(skip_matches)}, skipping")
            skipped_label.append(f"#{number} {title}")
            continue

        last_date = last_flake_date(issue["body"])

        if last_date is None:
            print(f"  #{number}: no failure date in body, skipping")
            skipped_no_date.append(f"#{number} {title}")
            continue

        age = (datetime.now(tz=timezone.utc) - last_date).days
        if last_date < cutoff:
            if has_todo_reference(number):
                print(f"  #{number} {title}: last flake {last_date.date()} ({age}d ago) → skipping (TODO reference found)")
                skipped_todo.append(f"#{number} {title}")
            else:
                assignees = [a["login"] for a in issue.get("assignees", {}).get("nodes", [])]
                assignee_note = f" (assigned to: {', '.join(assignees)})" if assignees else ""
                print(f"  #{number} {title}: last flake {last_date.date()} ({age}d ago) → closing{assignee_note}")
                close_issue(number, last_date, assignees, dry_run)
                closed.append(f"#{number} {title} (last flake {last_date.date()}, {age}d ago){assignee_note}")
        else:
            print(f"  #{number} {title}: last flake {last_date.date()} ({age}d ago) → keeping open")
            kept_open += 1

    action = "Would close" if dry_run else "Closed"
    total_skipped = len(skipped_label) + len(skipped_no_date) + len(skipped_todo)
    print(
        f"\n{action} {len(closed)}/{len(issues)} issues. "
        f"Skipped {total_skipped} (skip label, no date, or active TODO reference). "
        f"Kept open {kept_open}."
    )

    def print_section(header: str, items: list[str]) -> None:
        print(f"\n=== {header} ({len(items)}) ===")
        for item in items or ["(none)"]:
            print(f"  {item}")

    print_section("Would close" if dry_run else "Closed", closed)
    print_section("Skipped: skip label", skipped_label)
    print_section("Skipped: no failure date in body", skipped_no_date)
    print_section("Skipped: active TODO reference", skipped_todo)


if __name__ == "__main__":
    main(dry_run="--dry-run" in sys.argv)
