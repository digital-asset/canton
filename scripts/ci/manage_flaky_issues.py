#!/usr/bin/env python3
"""Creates or updates a GitHub tracking issue for each failing test.

Reads failing_tests.json and, for branches we track (see should_report_issues),
creates or updates one tracking issue per failing test, handling release-line
tagging, archive restore, consecutive-commit and nightly streak detection, and
broken-nightly labelling. Writes the streaks worth a Slack alert to
streaks.json, consumed by alert_slack.py.

Usage:
  manage_flaky_issues.py [--failing-tests-json PATH] [--streaks-json PATH] [--self-test]
"""

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import tempfile
from itertools import groupby
from typing import Optional
from unittest.mock import patch

from flaky_common import (
    branch,
    should_report_issues,
    is_nightly_job,
    is_unstable_job,
    format_issue_title,
    run_gh_with_retries,
    check_result,
    create_issue_table_header,
    create_issue_table_row,
    with_evidence_link,
    get_ci_commit_hash,
    get_ci_job_name,
    flaky_test_project,
    release_line_field,
    branches_to_report,
    milestone,
    CONSECUTIVE_FAILURES_THRESHOLD,
    CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE,
    NIGHTLY_CONSECUTIVE_FAILURES,
    NIGHTLY_BROKEN_LABEL,
    NIGHTLY_CRON_HOUR_UTC,
    read_failing_tests,
    read_failing_test_causes,
    DEFAULT_CAUSE,
    write_streaks,
    failing_tests_path,
    streaks_path,
    assert_required_env_vars,
    ci_required_context_vars,
    run_guarded_self_test,
    FAILING_TESTS_MAX,
    is_dry_run,
    artifact_dir,
    is_github_actions_ci,
    get_ci_build_url,
    get_ci_node_index,
    get_ci_parallel_run_url,
    get_ci_log_url,
    get_ci_commit_url,
)
from collect_flake_evidence import (
    FLAKE_PKG_RETENTION_DAYS,
    SHARD_BUNDLE_RETENTION_DAYS,
    evidence_label,
    read_evidence,
    render_evidence_comment,
    parse_evidence_marker,
)


# How long after an issue is assigned we stay quiet on Slack. Past this window we resume
# alerting on every consecutive-failure streak, so an assigned but still-breaking test does
# not go unnoticed for weeks.
ASSIGNMENT_SLACK_GRACE = datetime.timedelta(hours=24)


def _latest_assignment_time(search_node) -> Optional[datetime.datetime]:
    """Most recent AssignedEvent timestamp for the issue, or None if none is known."""
    nodes = search_node.get("timelineItems", {}).get("nodes", [])
    stamps = [_parse_iso(node["createdAt"]) for node in nodes if node.get("createdAt")]
    return max(stamps) if stamps else None


def _within_assignment_grace(assigned_at: Optional[datetime.datetime]) -> bool:
    """True while a freshly assigned issue is still inside its Slack grace window."""
    if assigned_at is None:
        return False
    return datetime.datetime.now(datetime.timezone.utc) - assigned_at < ASSIGNMENT_SLACK_GRACE


# First column of each history row, e.g. "| 2026-04-20 12:05:05 | ...". Written by
# create_issue_table_row via a naive datetime.now() on the CI runner, which runs in UTC.
_ROW_DATE_RE = re.compile(r'^\| (\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2} \|', re.MULTILINE)


def _failed_earlier_today(body: str) -> bool:
    """True if an existing history row already recorded a failure on today's UTC date.

    Once an assigned issue is past its grace window, this caps Slack alerts to once a day:
    a test that keeps streaking every run does not re-page the assignee on every single CI
    run, one reminder a day is enough. Reusing the history table's own Date column avoids
    adding separate notification-tracking state that could drift from what the table
    already records.
    """
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    return any(date == today for date in _ROW_DATE_RE.findall(body))


def report_issue(issue: str, cause: str = DEFAULT_CAUSE, evidence_comment: Optional[str] = None):
    title = format_issue_title(issue)
    idx = None
    project_item_id = None
    release_line = None
    is_archived = False
    has_assignee = False
    assigned_at = None

    # search issues by title. also returns partial matches
    result = run_gh_with_retries(
        [
            "api",
            "graphql",
            "-F",
            "query=@scripts/ci/findIssueByTitle.graphql",
            "-f",
            f"searchstr=repo:DACH-NY/canton in:title {title}",
        ]
    )
    check_result(result)
    search_result_json = json.loads(result.stdout)

    # let's look for the exact title in the search result
    for result in search_result_json["data"]["search"]["nodes"]:
        if result["title"] == title:
            idx = str(result["number"])
            body = result["body"]
            has_assignee = result["assignees"]["totalCount"] > 0
            assigned_at = _latest_assignment_time(result)
            # look at the projects the issue is linked to
            for project_relation in result["projectItems"]["nodes"]:
                # only extract data for the ticket if the linked project matches the flaky test project
                if project_relation["project"]["id"] == flaky_test_project:
                    project_item_id = project_relation["id"]

                    # remember whether the issue is archived or not, so we can unarchive later
                    is_archived = project_relation["isArchived"]

                    # extract the custom "Release Line" field
                    if project_relation["releaseLine"]:
                        release_line = project_relation["releaseLine"]["name"]

                    # we found the flaky test project. no need to look at others.
                    break

            # we found a ticket with the exact same title, no need to look at others.
            break

    if idx:
        # the project_item_id connected to the flaky test project should always be found, because of how the github automation is set up.
        # however, we still need to guard against possible None values and subsequent misleading errors
        if project_item_id:
            # if the issue was archived, let's unarchive it
            if is_archived:
                gh_unarchive_issue(idx, project_item_id)

            # if the issue is not yet tagged with a release line, update it accordingly.
            # this is typically the case for tickets that have only failed once, because we don't
            # set the release line during creation (it's not possible via the rest api)
            if not release_line:
                gh_assign_release_line(idx, project_item_id)

        # update the description and reopen if needed. update_issue owns the full notify
        # decision (grace period + once-a-day cap once assigned), since it is the only
        # place that builds and writes the body a Slack-alert marker would be stamped into.
        return update_issue(
            idx,
            title,
            body,
            cause,
            evidence_comment,
            has_assignee=has_assignee,
            assigned_at=assigned_at,
        )

    else:
        create_issue(title, cause, evidence_comment)


def gh_assign_release_line(idx: str, project_item_id: str):
    release_line_value = branches_to_report.get(branch, None)
    if release_line_value:
        result = run_gh_with_retries(
            [
                "api",
                "graphql",
                "-F",
                "query=@scripts/ci/assignReleaseLine.graphql",
                "-F",
                f"issue={project_item_id}",
                "-F",
                f"project={flaky_test_project}",
                "-F",
                f"field={release_line_field}",
                "-F",
                f"value={release_line_value}",
            ]
        )
        check_result(result)
        print(
            f"Assigned release line \"{release_line_value}\" to issue: https://github.com/DACH-NY/canton/issues/{idx}"
        )


def gh_issue_create_cmd(title: str, body: str):
    result = run_gh_with_retries(
        [
            "issue",
            "create",
            "--repo",
            "DACH-NY/canton",
            "--title",
            title,
            "--body-file",
            "-",
            "--milestone",
            milestone,
        ],
        stdin=body,
    )
    check_result(result)
    return result


def gh_issue_edit_cmd(idx: str, title: str, body: str):
    # gh issue edit has no --state flag; reopen separately first
    idx = str(idx)
    reopen_result = run_gh_with_retries(["issue", "reopen", idx, "--repo", "DACH-NY/canton"])
    if reopen_result.returncode != 0 and "already open" not in reopen_result.stderr.lower():
        check_result(reopen_result)
    result = run_gh_with_retries(
        ["issue", "edit", idx, "--repo", "DACH-NY/canton", "--title", title, "--body-file", "-"],
        stdin=body,
    )
    check_result(result)
    return result


def create_issue(
    title: str, cause: str = DEFAULT_CAUSE, evidence_comment: Optional[str] = None
) -> Optional[str]:
    row = create_issue_table_row(cause)
    intro = "This issue was created automatically by the CI system. Please fix the test before closing the issue."
    body = f"{intro}\n\n{create_issue_table_header()}\n{row}"
    result = gh_issue_create_cmd(title, body)
    print(f"Created issue: {result.stdout.strip()}")
    idx = issue_number_from_url(result.stdout)
    if idx and evidence_comment:
        # The comment needs the issue to exist, so the first row gets its
        # Evidence link in a second edit.
        evidence_url = post_evidence_comment(idx, evidence_comment)
        if evidence_url:
            linked = (
                f"{intro}\n\n{create_issue_table_header()}\n"
                f"{with_evidence_link(row, evidence_url, comment_label(evidence_comment))}"
            )
            try:
                gh_issue_edit_cmd(idx, title, linked)
            except Exception as e:
                # silent-ok: the evidence link is a nice-to-have, the issue itself
                # already exists, already surfaced via the ::warning:: annotation
                print(f"::warning::Could not link the evidence from issue #{idx}: {e}")
    return idx


def issue_number_from_url(gh_output: str) -> Optional[str]:
    """`gh issue create` prints the new issue's URL; returns its number."""
    match = re.search(r"/issues/(\d+)\s*$", gh_output.strip())
    return match.group(1) if match else None


# --- evidence comments ------------------------------------------------------
#
# collect_flake_evidence.py writes evidence.json before this step runs; each
# created or updated issue gets one comment with that evidence (stack traces,
# log problems, links to the CI log and the artifacts). Comments, not the body: the
# body table is parsed by the streak logic and close_stale_flake_issues.py.


def evidence_artifacts(package_dirs: list[str]) -> list:
    """The artifacts of this failure, as named and uploaded by
    .github/actions/test/cleanup_test_job/action.yml (FLAKE_PKG_ARTIFACT_*,
    SHARD_BUNDLE_ARTIFACT_*).

    The flake-pkg artifact holds the investigation packages of all failing suites
    of the shard, so it is only listed for an issue whose own suites have a
    package in it, naming the directories that are its own.
    """
    env = os.environ.get
    dirs = ", ".join(f"`{d}/`" for d in package_dirs)
    packages = "packages" if len(package_dirs) > 1 else "package"
    candidates = [
        (
            env("FLAKE_PKG_ARTIFACT_NAME") if package_dirs else None,
            env("FLAKE_PKG_ARTIFACT_ID"),
            env("FLAKE_PKG_ARTIFACT_URL"),
            f"{dirs} in it: the investigation {packages} of this failure (README, manifest,"
            " the suite's log, a log per failing test, JUnit reports, problem lines)",
            FLAKE_PKG_RETENTION_DAYS,
        ),
        (
            env("SHARD_BUNDLE_ARTIFACT_NAME"),
            env("SHARD_BUNDLE_ARTIFACT_ID"),
            env("SHARD_BUNDLE_ARTIFACT_URL"),
            "the shard bundle: all logs, JUnit reports, debug files",
            SHARD_BUNDLE_RETENTION_DAYS,
        ),
    ]
    return [
        {
            "name": name,
            "id": artifact_id or None,
            "url": url or None,
            "description": what,
            "retention_days": days,
        }
        for name, artifact_id, url, what, days in candidates
        if name
    ]


def evidence_context(title: str, cause: str, package_dirs: list[str]) -> dict:
    sha = get_ci_commit_hash()
    build_url = get_ci_build_url()
    node = get_ci_node_index()
    github = is_github_actions_ci()
    return {
        "title": title,
        "sha": sha,
        "commit_url": get_ci_commit_url(sha),
        "job": get_ci_job_name(),
        "shard": node,
        "job_url": get_ci_parallel_run_url(build_url, node),
        "log_url": get_ci_log_url(build_url, node),
        # The artifacts list is a GHA concept; CircleCI keeps them on the job page.
        "run_url": build_url if github else "",
        "run_id": os.environ.get("GITHUB_RUN_ID", "") if github else "",
        "repo": os.environ.get("GITHUB_REPOSITORY", "DACH-NY/canton"),
        "cause": cause,
        "date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "artifacts": evidence_artifacts(package_dirs) if github else [],
    }


def evidence_comments_by_title(failing_tests, cause_by_title, evidence) -> dict[str, str]:
    """{title: rendered comment} for every title with evidence for any of its variants."""
    records: dict[str, list] = {}
    for test_name in failing_tests:
        record = evidence.get(test_name)
        if record:
            records.setdefault(format_issue_title(test_name), []).append(record)
    return {
        title: render_evidence_comment(
            recs,
            evidence_context(
                title,
                cause_by_title[title],
                [r["package_dir"] for r in recs if r.get("package_dir")],
            ),
        )
        for title, recs in records.items()
    }


def comment_label(comment: str) -> str:
    """The text of the table's link to an evidence comment, e.g. "2 failing tests"."""
    meta = parse_evidence_marker(comment) or {}
    # Comments from before test_count only carry the names.
    return evidence_label(meta.get("test_count", len(meta.get("tests") or [])))


def _evidence_key(meta: dict) -> tuple:
    # "node" is what the shard was called in the first PR-test comments.
    return (meta.get("sha"), meta.get("job"), str(meta.get("shard", meta.get("node"))))


def existing_evidence(idx: str) -> dict:
    """{(sha, job, shard): comment URL} of the evidence comments already on the issue."""
    result = run_gh_with_retries(
        [
            "api",
            f"repos/DACH-NY/canton/issues/{idx}/comments",
            "--paginate",
            "--jq",
            '.[] | select(.body | startswith("<!-- flaky-evidence v1"))'
            ' | "\\(.html_url) \\(.body | split("\\n")[0])"',
        ]
    )
    check_result(result)
    found = {}
    for line in result.stdout.splitlines():
        url, _, marker = line.partition(" ")
        meta = parse_evidence_marker(marker)
        if meta:
            found[_evidence_key(meta)] = url
    return found


def post_evidence_comment(idx: str, comment: str) -> Optional[str]:
    """Posts the evidence once per (commit, job, shard) and returns the comment's URL,
    also when it was already there. Never fails the pipeline: returns None instead."""
    try:
        key = _evidence_key(parse_evidence_marker(comment) or {})
        existing_url = existing_evidence(idx).get(key)
        if existing_url:
            print(f"Issue #{idx} already has evidence for {key}, not posting it again.")
            return existing_url
        with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False) as f:
            f.write(comment)
            body_file = f.name
        try:
            result = run_gh_with_retries(
                ["issue", "comment", str(idx), "--repo", "DACH-NY/canton", "--body-file", body_file]
            )
            check_result(result)
        finally:
            os.unlink(body_file)
        # `gh issue comment` prints the new comment's URL.
        url = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else None
        print(
            f"Posted failure evidence: {url or f'https://github.com/DACH-NY/canton/issues/{idx}'}"
        )
        return url
    except Exception as e:
        # silent-ok: the evidence comment is a nice-to-have, already surfaced via the
        # ::warning:: annotation, callers already handle a None return gracefully
        print(f"::warning::Could not post failure evidence to #{idx}: {e}")
        return None


def write_dry_run_comments(comments: dict[str, str]) -> None:
    """FLAKY_DRY_RUN: write the comments to files and the job summary, call no GitHub API."""
    out_dir = os.path.join(artifact_dir(), "comments")
    os.makedirs(out_dir, exist_ok=True)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    for title, comment in comments.items():
        path = os.path.join(out_dir, re.sub(r"[^A-Za-z0-9_.-]", "_", title)[:150] + ".md")
        with open(path, "w") as f:
            f.write(comment)
        print(f"Dry run: wrote evidence comment for '{title}' to {path} ({len(comment)} chars)")
        if summary_path:
            with open(summary_path, "a") as f:
                f.write(f"\n## Dry run: evidence comment for {title}\n\n{comment}\n")


def extract_commit_hashes_from_body(body: str) -> list[str]:
    return re.findall(r'commit/([0-9a-f]{40})', body)


# --- nightly streak detection ---------------------------------------------
#
# Nightly runs are days apart and land on non-adjacent commits, so the per-commit
# adjacency check (are_consecutive_commits) never fires for them. Instead we ask:
# did this test fail on each of the last N nightly runs? The nightly runs are
# enumerated from `main` history at the nightly cron times (no CircleCI API token
# exists in the repo); the current run is always counted as the most recent one.

# Matches a failure-history table row, capturing (job, 40-char commit).
_NIGHTLY_ROW_RE = re.compile(
    r'^\|[^|]*\|\s*([^|]+?)\s*\|[^|]*\|[^|]*\|[^|]*commit/([0-9a-f]{40})[^|]*\|',
    re.MULTILINE,
)

_nightly_commits_cache: Optional[tuple] = None


def _git_commit_before(when: datetime.datetime) -> Optional[str]:
    # `when` is a naive UTC datetime; the trailing Z makes git interpret it as
    # UTC instead of the runner's local timezone (which would skew the cutoff).
    iso = when.strftime("%Y-%m-%dT%H:%M:%SZ")
    result = subprocess.run(
        ["git", "rev-list", "-1", f"--before={iso}", "origin/main"],
        capture_output=True,
        text=True,
    )
    sha = result.stdout.strip()
    return sha if result.returncode == 0 and len(sha) == 40 else None


def _previous_nightly_datetimes(count: int, before: datetime.datetime) -> list:
    """The `count` most recent nightly cron times strictly before `before` (Mon-Fri, UTC)."""
    out: list = []
    day = before.date()
    guard = 0
    while len(out) < count and guard < 30:
        guard += 1
        dt = datetime.datetime(day.year, day.month, day.day, NIGHTLY_CRON_HOUR_UTC, 0, 0)
        if dt < before and dt.weekday() < 5:  # Mon-Fri
            out.append(dt)
        day -= datetime.timedelta(days=1)
    return out


def recent_nightly_commits() -> tuple:
    """Commits of the last NIGHTLY_CONSECUTIVE_FAILURES nightly runs: the current run plus
    the previous ones, derived from `main` history at the nightly cron times. Cached per
    process."""
    global _nightly_commits_cache
    if _nightly_commits_cache is not None:
        return _nightly_commits_cache

    current = get_ci_commit_hash()
    if current == "unknown":
        _nightly_commits_cache = tuple()
        return _nightly_commits_cache

    fetch = subprocess.run(
        ["git", "fetch", "--quiet", "origin", "main"], capture_output=True, text=True
    )
    if fetch.returncode != 0:
        # Stale origin/main would mis-enumerate the nightly commits; log and carry on.
        print(
            f"recent_nightly_commits: 'git fetch origin main' failed, using local origin/main: {fetch.stderr.strip()}"
        )
    now = datetime.datetime.utcnow()
    # `now` is after the current run's own nightly cron slot, so the most recent
    # cron time enumerated below resolves to `current` again. We therefore request
    # NIGHTLY_CONSECUTIVE_FAILURES slots (not N-1): the newest collapses onto
    # `current` on dedup, leaving the current run plus the N-1 genuinely-prior
    # nightly commits. Requesting only N-1 here yielded N-1 distinct commits and
    # made nightly_streak's `len(last) < N` guard fail every time.
    commits = [current]
    for dt in _previous_nightly_datetimes(NIGHTLY_CONSECUTIVE_FAILURES, now):
        sha = _git_commit_before(dt)
        if sha:
            commits.append(sha)

    seen: set = set()
    deduped: list = []
    for c in commits:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    _nightly_commits_cache = tuple(deduped[:NIGHTLY_CONSECUTIVE_FAILURES])
    return _nightly_commits_cache


def extract_nightly_commits_from_body(body: str) -> list:
    """Commits from failure rows whose job is a nightly job."""
    return [commit for job, commit in _NIGHTLY_ROW_RE.findall(body) if is_nightly_job(job)]


def nightly_streak(body: str) -> bool:
    """True if the test failed on each of the last NIGHTLY_CONSECUTIVE_FAILURES nightly runs.

    The current (failing) run counts as the most recent nightly run, so its commit
    is added to the set of nightly failures parsed from the issue body.
    """
    last = recent_nightly_commits()
    if len(last) < NIGHTLY_CONSECUTIVE_FAILURES:
        return False
    failures = set(extract_nightly_commits_from_body(body))
    failures.add(get_ci_commit_hash())
    return set(last).issubset(failures)


def add_broken_nightly_label(idx: str) -> None:
    """Best-effort: label the issue. A missing label or transient error is logged, not fatal."""
    result = run_gh_with_retries(
        ["issue", "edit", str(idx), "--repo", "DACH-NY/canton", "--add-label", NIGHTLY_BROKEN_LABEL]
    )
    if result.returncode != 0:
        print(f"Could not add '{NIGHTLY_BROKEN_LABEL}' label to #{idx}: {result.stderr.strip()}")


def _parse_iso(value: str) -> datetime.datetime:
    # gh emits ISO-8601 with a trailing Z. fromisoformat needs an explicit offset.
    return datetime.datetime.fromisoformat(value.strip().replace("Z", "+00:00"))


def flake_predates_close(idx: str, commit_hash: str) -> bool:
    """True when closed issue #idx was closed after commit_hash was committed, i.e. a pre-fix stale flake. Fails open to False on any error."""
    query = (
        'query($n:Int!,$oid:GitObjectID!){repository(owner:"DACH-NY",name:"canton"){'
        'issue(number:$n){state closedAt} object(oid:$oid){...on Commit{committedDate}}}}'
    )
    try:
        result = run_gh_with_retries(
            ["api", "graphql", "-f", f"query={query}", "-F", f"n={idx}", "-f", f"oid={commit_hash}"]
        )
        if result.returncode != 0:
            print(
                f"flake_predates_close: gh api graphql failed for #{idx}/{commit_hash[:12]}: "
                f"{result.stderr.strip()}"
            )
            return False
        repo = json.loads(result.stdout)["data"]["repository"]
        issue, commit = repo["issue"], repo["object"]
        if issue["state"] != "CLOSED" or not issue["closedAt"] or not commit:
            return False
        return _parse_iso(commit["committedDate"]) < _parse_iso(issue["closedAt"])
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        # silent-ok: deliberate fail-open classification helper, see the docstring above
        print(f"flake_predates_close: could not evaluate #{idx}/{commit_hash[:12]}: {exc}")
        return False


def are_consecutive_commits(older_hash: str, newer_hash: str) -> bool:
    """Returns True if newer_hash is a direct child of older_hash in git history."""
    result = run_gh_with_retries(
        ["api", f"repos/DACH-NY/canton/commits/{newer_hash}", "--jq", ".parents[].sha"]
    )
    if result.returncode != 0:
        print(
            f"are_consecutive_commits: gh api failed for {newer_hash[:8]}: {result.stderr.strip()}"
        )
        return False
    return older_hash in result.stdout.splitlines()


# Header rows of the pre-Cause (five-column) failure-history table. Existing
# issues carry these verbatim, so we rewrite them in place when appending a row.
_HEADER_5 = "| Date | Job | Node | Build | Commit |"
_HEADER_6 = "| Date | Job | Node | Build | Commit | Cause |"
_OLD_HEADER_LINES = (_HEADER_5, _HEADER_6)
_OLD_SEPARATOR_LINES = ("|---|---|---|---|---|", "|---|---|---|---|---|---|")
_NEW_HEADER_LINE, _NEW_SEPARATOR_LINE = create_issue_table_header().split("\n")


def _migrate_row(row: str, old_header: str) -> str:
    """Moves an existing row's cells into the current column order (Evidence before Cause)."""
    cells = row.strip()[1:-1].split("|")
    if old_header == _HEADER_6 and len(cells) == 6:
        cells.insert(5, "  ")  # a Cause but no Evidence
    else:
        return row  # five-cell rows: no Evidence, no Cause, nothing to move
    return "|" + "|".join(cells) + "|"


def migrate_table_header(body: str) -> str:
    """Ensures the issue body has the current seven-column table (... | Evidence | Cause |).

    New issues get the header appended. Issues with an older header get it
    upgraded in place, and their existing rows are rewritten into the new
    column order, so a Cause never shows under Evidence. Five-cell rows from
    before the Cause column render their missing cells empty. Lines are
    compared whole: each older header and separator is a prefix of the next.
    """
    lines = body.split("\n")
    if _NEW_HEADER_LINE in lines:
        return body
    if not any(line in _OLD_HEADER_LINES for line in lines):
        return body + "\n" + create_issue_table_header()
    migrated = []
    old_header = None  # set while inside a table with an old header
    for line in lines:
        if line in _OLD_HEADER_LINES:
            old_header = line
            line = _NEW_HEADER_LINE
        elif line in _OLD_SEPARATOR_LINES:
            line = _NEW_SEPARATOR_LINE
        elif old_header and line.startswith("|"):
            line = _migrate_row(line, old_header)
        else:
            old_header = None
        migrated.append(line)
    return "\n".join(migrated)


# Caps on the failure-history table, so a test that keeps failing for months does not
# grow its issue body without bound. Rows are appended at the bottom, so the oldest are
# cut from the top; their evidence comments stay on the issue. Everything that parses
# the table (streaks, the daily Slack cap, close_stale_flake_issues.py) only looks at
# the most recent rows.
HISTORY_MAX_ROWS = 500
# Rows this recent are kept even past HISTORY_MAX_ROWS ...
HISTORY_KEEP_DAYS = 90
# ... unless the body would exceed this many characters. This hard cap is based on local tests,
# GitHub started rejecting input at around 260k characters in an issue body, the chosen value
# includes some safety margin
HISTORY_MAX_CHARS = 200_000

_TRUNCATED_RE = re.compile(r'^<!-- flaky-truncated (\d+) -->$')


def truncate_history(body: str, today: Optional[datetime.date] = None) -> str:
    """Cuts the oldest rows of the failure-history table, leaving a marker above it.

    Keeps at most HISTORY_MAX_ROWS rows, but never drops one younger than
    HISTORY_KEEP_DAYS, unless the body would exceed HISTORY_MAX_CHARS. The newest
    row is always kept. The marker counts every row truncated so far, across runs.
    """
    lines = body.split("\n")
    if _NEW_HEADER_LINE not in lines:
        return body
    header = lines.index(_NEW_HEADER_LINE)
    first = header + 2
    end = first
    while end < len(lines) and lines[end].startswith("|"):
        end += 1
    rows = lines[first:end]

    above = lines[:header]
    already = 0
    if len(above) >= 3 and (match := _TRUNCATED_RE.match(above[-3])):
        already = int(match.group(1))
        above = above[:-3]

    today = today or datetime.datetime.now(datetime.timezone.utc).date()
    cutoff = (today - datetime.timedelta(days=HISTORY_KEEP_DAYS)).isoformat()

    def row_date(row: str) -> Optional[str]:
        match = _ROW_DATE_RE.match(row)
        return match.group(1) if match else None

    def truncation_marker(total: int, oldest_kept: Optional[str]) -> list[str]:
        kept = f", oldest kept row is from {oldest_kept}" if oldest_kept else ""
        return [
            f"<!-- flaky-truncated {total} -->",
            f"_{total} earlier rows truncated{kept}. Any evidence comments for them are still on this issue._",
            "",
        ]

    def render(drop: int) -> str:
        if drop == 0:
            return body
        marker = truncation_marker(already + drop, row_date(rows[drop]))
        return "\n".join(above + marker + lines[header:first] + rows[drop:] + lines[end:])

    # Rows are oldest first, so the first recent row ends the count-based cut.
    drop = 0
    while len(rows) - drop > HISTORY_MAX_ROWS and (row_date(rows[drop]) or "") < cutoff:
        drop += 1

    out = render(drop)
    # Re-render until it fits: adding or growing the marker can push it back over.
    while len(out) > HISTORY_MAX_CHARS and drop < len(rows) - 1:
        excess = len(out) - HISTORY_MAX_CHARS
        while excess > 0 and drop < len(rows) - 1:
            excess -= len(rows[drop]) + 1
            drop += 1
        out = render(drop)
    return out


def update_issue(
    idx: str,
    title: str,
    body: str,
    cause: str = DEFAULT_CAUSE,
    evidence_comment: Optional[str] = None,
    has_assignee: bool = False,
    assigned_at: Optional[datetime.datetime] = None,
) -> Optional[tuple[str, str, str]]:
    job = get_ci_job_name()
    threshold = (
        CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE
        if is_unstable_job(job)
        else CONSECUTIVE_FAILURES_THRESHOLD
    )

    body = migrate_table_header(body)
    commit_hash = get_ci_commit_hash()

    if commit_hash != 'unknown' and flake_predates_close(idx, commit_hash):
        print(
            f"Issue #{idx} was closed after commit {commit_hash[:12]} was committed. "
            f"Skipping reopen for this stale flake."
        )
        return None

    consecutive_streak = False
    nightly = is_nightly_job(job)

    if commit_hash != 'unknown':
        if nightly:
            # Nightly runs are days apart on non-adjacent commits, so check whether
            # the test failed on each of the last N nightly runs instead.
            consecutive_streak = nightly_streak(body)
        else:
            # Collapse consecutive same-commit entries (multi-shard / retries) so they
            # don't shadow the check: are_consecutive_commits(X, X) is always False.
            distinct = [
                c for c, _ in groupby(extract_commit_hashes_from_body(body) + [commit_hash])
            ]
            recent = distinct[-threshold:]
            if len(recent) >= threshold and all(
                are_consecutive_commits(a, b) for a, b in zip(recent, recent[1:])
            ):
                consecutive_streak = True

    notify = False
    if consecutive_streak:
        if nightly:
            add_broken_nightly_label(idx)
        if not has_assignee:
            notify = True
        elif _within_assignment_grace(assigned_at):
            grace_hours = int(ASSIGNMENT_SLACK_GRACE.total_seconds() // 3600)
            print(
                f"Issue #{idx} was assigned within the last {grace_hours}h, skipping Slack notification for now."
            )
        elif _failed_earlier_today(body):
            print(f"Issue #{idx} already failed earlier today, skipping Slack notification.")
        else:
            notify = True

    # The comment first, so the new row can link to it.
    evidence_url = post_evidence_comment(idx, evidence_comment) if evidence_comment else None
    label = comment_label(evidence_comment) if evidence_comment else ""
    new_body = truncate_history(f"{body}\n{create_issue_table_row(cause, evidence_url, label)}")
    gh_issue_edit_cmd(idx, title, new_body)
    print(f"Updated issue: https://github.com/DACH-NY/canton/issues/{idx}")
    if notify:
        return (idx, title, commit_hash)
    return None


def gh_unarchive_issue(idx: str, project_item_id: str):
    result = run_gh_with_retries(
        [
            "api",
            "graphql",
            "-F",
            "query=@scripts/ci/unarchiveIssue.graphql",
            "-F",
            f"issue={project_item_id}",
            "-F",
            f"project={flaky_test_project}",
        ]
    )
    check_result(result)
    print(f"Unarchived issue: https://github.com/DACH-NY/canton/issues/{idx}")


def test_most_specific_cause_by_title():
    # Two variants of one test collapse to a single issue title. The most
    # specific cause across them (timeout) must win over the alphabetically
    # earlier regular variant, so the timeout is not silently dropped.
    failing = ["LedgerApiIntegrationTest/shard=0", "LedgerApiIntegrationTest/shard=1"]
    causes = {
        "LedgerApiIntegrationTest/shard=0": "regular",
        "LedgerApiIntegrationTest/shard=1": "timeout",
    }
    resolved = most_specific_cause_by_title(failing, causes)
    assert len(resolved) == 1, resolved
    ((_, cause),) = resolved.items()
    assert cause == "timeout", resolved

    # A log error beats a log warning for the same title, ranking on the bucket
    # token even with a " [Suite]" suffix.
    causes2 = {
        "FooIntegrationTest/a": "warn_other [FooIntegrationTest]",
        "FooIntegrationTest/b": "error_other [FooIntegrationTest]",
    }
    ((_, cause2),) = most_specific_cause_by_title(list(causes2), causes2).items()
    assert cause2 == "error_other [FooIntegrationTest]", cause2

    # A variant with no recorded cause defaults to regular and does not override
    # a real timeout on a sibling variant.
    causes3 = {"BarTest/y": "timeout"}  # BarTest/x absent -> DEFAULT_CAUSE
    ((_, cause3),) = most_specific_cause_by_title(["BarTest/x", "BarTest/y"], causes3).items()
    assert cause3 == "timeout", cause3


def self_test():
    test_migrate_table_header()
    test_most_specific_cause_by_title()
    test_update_issue_old_format()
    test_update_issue_new_format()
    test_update_issue_returns_consecutive_streak_info_stable()
    test_update_issue_returns_consecutive_streak_info_unstable()
    test_update_issue_returns_consecutive_streak_info_unstable_slow()
    test_update_issue_dedupes_shard_duplicates()
    test_report_issue_assignment_grace_period()
    test_report_issue_finds_flaky_project_among_other_projects()
    test_update_issue_assigned_daily_cap()
    test_nightly_streak_detection()
    test_recent_nightly_commits_counts_current_run_once()
    test_update_issue_nightly_streak_labels_and_returns()
    test_flake_predates_close()
    test_issue_number_from_url()
    test_package_artifact_only_for_issues_with_a_package()
    test_post_evidence_comment_dedupes_and_never_raises()
    test_create_issue_posts_evidence()
    test_update_issue_links_evidence_in_new_row()
    test_run_dry_run_writes_comments_without_github()
    test_run_reports_issues_even_if_evidence_is_broken()
    test_truncate_history()
    test_update_issue_truncates_history()
    print("manage_flaky_issues self-checks passed")


def test_run_reports_issues_even_if_evidence_is_broken():
    reported: list = []
    with tempfile.TemporaryDirectory() as tmp:
        failing, evidence, streaks = _write_pipeline_inputs(tmp)
        with open(evidence, "w") as f:
            f.write("{ not json")
        env = {
            "CIRCLECI": "true",
            "GITHUB_ACTIONS": "false",
            "CIRCLE_SHA1": "c" * 40,
            "CIRCLE_JOB": "test",
            "CIRCLE_NODE_INDEX": "0",
            "CIRCLE_BUILD_URL": "https://circleci.com/gh/DACH-NY/canton/1",
        }
        with (
            patch.dict(os.environ, env),
            patch(f"{__name__}.should_report_issues", return_value=True),
            patch(
                f"{__name__}.report_issue",
                side_effect=lambda name, cause, comment: reported.append((name, comment)),
            ),
        ):
            run(failing, streaks, evidence)
    assert reported == [("FooTest", None), ("NoEvidenceTest", None)], reported


def test_package_artifact_only_for_issues_with_a_package():
    env = {
        "GITHUB_ACTIONS": "true",
        "CIRCLECI": "false",
        "GITHUB_SHA": "a" * 40,
        "GITHUB_JOB": "test",
        "MATRIX_SHARD": "0",
        "GITHUB_RUN_ID": "7",
        "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_REPOSITORY": "DACH-NY/canton",
        "FLAKE_PKG_ARTIFACT_NAME": "flake-pkg-test-run-7",
        "FLAKE_PKG_ARTIFACT_ID": "11",
        "FLAKE_PKG_ARTIFACT_URL": "https://github.com/DACH-NY/canton/actions/runs/7/artifacts/11",
        "SHARD_BUNDLE_ARTIFACT_NAME": "test-bundle-run-7",
        "SHARD_BUNDLE_ARTIFACT_ID": "12",
        "SHARD_BUNDLE_ARTIFACT_URL": "https://github.com/DACH-NY/canton/actions/runs/7/artifacts/12",
    }

    def record(suite, package_dir):
        return {
            "suite": suite,
            "tests": [],
            "problems": ["WARN x:y - z"],
            "tids": [],
            "package_dir": package_dir,
        }

    evidence = {
        "FooTest": record("FooTest", "FooTest"),
        "WARN i.g.SomeLogger": record("WARN i.g.SomeLogger", None),
    }
    failing = list(evidence)
    causes = {name: DEFAULT_CAUSE for name in failing}
    with patch.dict(os.environ, env, clear=True):
        comments = evidence_comments_by_title(
            failing, most_specific_cause_by_title(failing, causes), evidence
        )
    with_log = comments[format_issue_title("FooTest")]
    without_log = comments[format_issue_title("WARN i.g.SomeLogger")]
    assert "`FooTest/` in it: the investigation package of this failure" in with_log, with_log
    # Another suite's package in the shard's artifact is no evidence for this issue.
    assert "flake-pkg-test-run-7" not in without_log, without_log
    # The shard bundle, with the whole log, is listed for both.
    assert "test-bundle-run-7" in with_log and "test-bundle-run-7" in without_log


def test_issue_number_from_url():
    assert issue_number_from_url("https://github.com/DACH-NY/canton/issues/31337\n") == "31337"
    assert issue_number_from_url("something else") is None


def _evidence_comment(sha="a" * 40, job="test", shard="0") -> str:
    record = {
        "suite": "FooTest",
        "tests": [{"name": "foo works", "type": "E", "text": "boom"}],
        "problems": [],
        "tids": [],
        "package_dir": None,
    }
    ctx = {
        "title": "[main] Flaky FooTest",
        "sha": sha,
        "commit_url": "u",
        "job": job,
        "shard": shard,
        "job_url": "j",
        "run_url": "",
        "cause": DEFAULT_CAUSE,
        "date": "d",
        "artifacts": [],
    }
    return render_evidence_comment([record], ctx)


def test_post_evidence_comment_dedupes_and_never_raises():
    comment = _evidence_comment()
    existing_url = "https://github.com/DACH-NY/canton/issues/42#issuecomment-1"
    new_url = "https://github.com/DACH-NY/canton/issues/42#issuecomment-2"
    existing_line = f"{existing_url} {comment.splitlines()[0]}"
    calls: list = []

    def fake_gh(args):
        calls.append(args)
        if args[0] == "api":
            return subprocess.CompletedProcess(args, 0, existing_line + "\n", "")
        return subprocess.CompletedProcess(args, 0, new_url + "\n", "")

    # Same (sha, job, shard) already on the issue: no second comment, its URL is reused.
    with patch(f"{__name__}.run_gh_with_retries", side_effect=fake_gh):
        assert post_evidence_comment("42", comment) == existing_url
    assert [c[0] for c in calls] == ["api"], calls

    # A different shard is new evidence: posted from a body file, its URL returned.
    calls.clear()
    with patch(f"{__name__}.run_gh_with_retries", side_effect=fake_gh):
        assert post_evidence_comment("42", _evidence_comment(shard="1")) == new_url
    assert [c[0] for c in calls] == ["api", "issue"], calls
    assert calls[1][:3] == ["issue", "comment", "42"] and "--body-file" in calls[1], calls

    # A gh failure is only a warning.
    with patch(
        f"{__name__}.run_gh_with_retries",
        return_value=subprocess.CompletedProcess([], 1, "", "HTTP 500"),
    ):
        assert post_evidence_comment("42", comment) is None  # must not raise


def test_create_issue_posts_evidence():
    comment = _evidence_comment()
    posted: list = []
    edits: list = []
    url = "https://github.com/DACH-NY/canton/issues/99#issuecomment-5"
    created = subprocess.CompletedProcess(
        [], 0, "https://github.com/DACH-NY/canton/issues/99\n", ""
    )

    def fake_post(idx, comment):
        posted.append((idx, comment))
        return url

    with (
        patch(f"{__name__}.gh_issue_create_cmd", return_value=created) as create,
        patch(f"{__name__}.create_issue_table_row", return_value="| row |  | regular |"),
        patch(f"{__name__}.post_evidence_comment", side_effect=fake_post),
        patch(f"{__name__}.gh_issue_edit_cmd", side_effect=lambda i, t, b: edits.append((i, b))),
    ):
        assert create_issue("[main] Flaky FooTest", DEFAULT_CAUSE, comment) == "99"
        assert create.call_args[0][1].endswith("\n| row |  | regular |"), create.call_args
        assert create_issue("[main] Flaky FooTest") == "99"
    assert posted == [("99", comment)], posted
    # Only the issue with evidence gets the second edit, linking the comment from its row.
    assert len(edits) == 1 and edits[0][0] == "99", edits
    assert edits[0][1].endswith(f"\n| row | [1 failing test]({url}) | regular |"), edits


def test_update_issue_links_evidence_in_new_row():
    url = "https://github.com/DACH-NY/canton/issues/42#issuecomment-7"
    env = {
        "CIRCLECI": "true",
        "GITHUB_ACTIONS": "false",
        "CIRCLE_SHA1": "unknown",
        "CIRCLE_JOB": "test",
        "CIRCLE_NODE_INDEX": "0",
        "CIRCLE_BUILD_URL": "https://circleci.com/gh/DACH-NY/canton/1",
    }
    order: list = []
    with (
        patch.dict(os.environ, env),
        patch(
            f"{__name__}.post_evidence_comment",
            side_effect=lambda i, c: order.append("comment") or url,
        ),
        patch(
            f"{__name__}.gh_issue_edit_cmd",
            side_effect=lambda i, t, b: order.append(("edit", b)),
        ),
    ):
        update_issue("42", "Flaky FooTest", "auto-created", DEFAULT_CAUSE, _evidence_comment())
    assert order[0] == "comment", "the comment is posted first so the row can link it"
    assert order[1][1].rstrip().endswith(f"| [1 failing test]({url}) | {DEFAULT_CAUSE} |"), order


def _write_pipeline_inputs(tmp: str) -> tuple:
    from flaky_common import write_failing_tests

    failing = os.path.join(tmp, "failing_tests.json")
    write_failing_tests(failing, {"FooTest": DEFAULT_CAUSE, "NoEvidenceTest": DEFAULT_CAUSE})
    evidence = os.path.join(tmp, "evidence.json")
    record = {
        "suite": "FooTest",
        "tests": [{"name": "foo works", "type": "E", "text": "boom"}],
        "problems": [],
        "tids": [],
        "package_dir": None,
    }
    with open(evidence, "w") as f:
        json.dump({"version": 1, "suites": {"FooTest": record}}, f)
    return failing, evidence, os.path.join(tmp, "streaks.json")


def test_run_dry_run_writes_comments_without_github():
    with tempfile.TemporaryDirectory() as tmp:
        failing, evidence, streaks = _write_pipeline_inputs(tmp)
        env = {"FLAKY_DRY_RUN": "1", "FLAKY_ARTIFACT_DIR": tmp, "GITHUB_ACTIONS": "false"}
        with (
            patch.dict(os.environ, env),
            patch(f"{__name__}.run_gh_with_retries", side_effect=AssertionError("no gh")),
            patch(f"{__name__}.report_issue", side_effect=AssertionError("no issues")),
        ):
            run(failing, streaks, evidence)
        written = os.listdir(os.path.join(tmp, "comments"))
        assert len(written) == 1 and "FooTest" in written[0], written
        with open(os.path.join(tmp, "comments", written[0])) as f:
            assert parse_evidence_marker(f.read())["tests"] == ["foo works"]


def test_flake_predates_close():
    def resp(state, closed_at, committed):
        payload = {
            "data": {
                "repository": {
                    "issue": {"state": state, "closedAt": closed_at},
                    "object": {"committedDate": committed} if committed else None,
                }
            }
        }
        return subprocess.CompletedProcess([], 0, json.dumps(payload), '')

    # Flake commit predates the close -> stale, skip the reopen.
    with patch(
        f"{__name__}.run_gh_with_retries",
        return_value=resp("CLOSED", "2026-09-07T15:33:49Z", "2026-09-07T15:02:52Z"),
    ):
        assert flake_predates_close("24208", "d85876fdd63a") is True
    # Flake commit lands after the close -> genuine regression, reopen.
    with patch(
        f"{__name__}.run_gh_with_retries",
        return_value=resp("CLOSED", "2026-09-07T15:33:49Z", "2026-09-07T16:00:00Z"),
    ):
        assert flake_predates_close("24208", "abc0000000de") is False
    # Issue still open -> never stale.
    with patch(
        f"{__name__}.run_gh_with_retries",
        return_value=resp("OPEN", None, "2026-09-07T15:02:52Z"),
    ):
        assert flake_predates_close("24208", "d85876fdd63a") is False
    # gh api itself fails -> fails open to False, and now prints why instead of silently.
    with (
        patch(
            f"{__name__}.run_gh_with_retries",
            return_value=subprocess.CompletedProcess([], 1, "", "boom"),
        ),
        patch("builtins.print") as mock_print,
    ):
        assert flake_predates_close("24208", "d85876fdd63a") is False
    printed = " ".join(str(c.args[0]) for c in mock_print.call_args_list)
    assert "24208" in printed and "d85876fdd63a" in printed and "boom" in printed, printed


def _nightly_body(commits: list) -> str:
    rows = "\n".join(
        f"| 2026-04-{20 + i} 00:00:00 | nightly_integration_test | 0 | [{i}](url) | "
        f"[{c[:8]}](https://github.com/DACH-NY/canton/commit/{c}) |"
        for i, c in enumerate(commits)
    )
    header = "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|"
    return "auto-created\n\n" + header + ("\n" + rows if rows else "")


def test_nightly_streak_detection():
    n = NIGHTLY_CONSECUTIVE_FAILURES
    commits = [format(i, '040x') for i in range(n)]  # n distinct commits
    current = commits[-1]
    prior = commits[:-1]  # the n-1 previous nightly commits

    env = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'nightly_integration_test',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }

    # failed on each of the last 3 nightly runs (priors in body + current) -> streak
    with (
        patch.dict(os.environ, env, clear=False),
        patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)),
    ):
        assert nightly_streak(_nightly_body(prior)) is True

    # one of the last-3 nightly commits is missing from the body -> no streak
    with (
        patch.dict(os.environ, env, clear=False),
        patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)),
    ):
        assert nightly_streak(_nightly_body(prior[:-1])) is False

    # the prior failures are non-nightly rows -> ignored -> no streak
    nonnightly = _nightly_body(prior).replace("nightly_integration_test", "sequential_test")
    with (
        patch.dict(os.environ, env, clear=False),
        patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)),
    ):
        assert nightly_streak(nonnightly) is False

    # fewer than N nightly runs known -> never flag
    with (
        patch.dict(os.environ, env, clear=False),
        patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits[:1])),
    ):
        assert nightly_streak(_nightly_body(prior)) is False


def test_recent_nightly_commits_counts_current_run_once():
    """Regression: the current run's own cron slot aliases `current`, so the
    enumeration must still produce N distinct commits (current + N-1 priors).
    Exercises the real recent_nightly_commits()/_previous_nightly_datetimes,
    so it fails if the slot count regresses to N-1."""
    global _nightly_commits_cache
    n = NIGHTLY_CONSECUTIVE_FAILURES
    current = format(0xC, '040x')
    priors = [format(i, '040x') for i in range(1, n)]
    env = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'nightly_integration_test',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }
    # _previous_nightly_datetimes returns slots newest-first; the newest resolves
    # to the current run's commit, each earlier one to a distinct prior commit.
    counter = {'i': 0}

    def fake_before(_dt):
        i = counter['i']
        counter['i'] += 1
        if i == 0:
            return current
        return priors[i - 1] if i - 1 < len(priors) else format(0xDEAD + i, '040x')

    saved_cache = _nightly_commits_cache
    _nightly_commits_cache = None
    try:
        with (
            patch.dict(os.environ, env, clear=False),
            patch(
                f'{__name__}.subprocess.run',
                return_value=subprocess.CompletedProcess([], 0, '', ''),
            ),
            patch(f'{__name__}._git_commit_before', side_effect=fake_before),
        ):
            result = recent_nightly_commits()
    finally:
        _nightly_commits_cache = saved_cache

    assert len(result) == n, f"expected {n} distinct nightly commits, got {len(result)}: {result}"
    assert current in result
    for p in priors:
        assert p in result, f"prior commit {p} missing from {result}"


def test_update_issue_nightly_streak_labels_and_returns():
    n = NIGHTLY_CONSECUTIVE_FAILURES
    commits = [format(i, '040x') for i in range(n)]
    current = commits[-1]
    prior = commits[:-1]
    env = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'nightly_integration_test',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }

    labelled: list = []
    # streak -> returns issue tuple and applies the broken-nightly label
    with (
        patch.dict(os.environ, env, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd'),
        patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)),
        patch(f'{__name__}.add_broken_nightly_label', side_effect=lambda idx: labelled.append(idx)),
    ):
        result = update_issue("77", "Flaky NightlyIntegrationTest", _nightly_body(prior))
        assert result is not None, "Expected a nightly streak to fire"
        assert result[0] == "77"
        assert labelled == ["77"], "Expected the broken-nightly label to be applied"

    # no streak -> no label, returns None
    labelled.clear()
    with (
        patch.dict(os.environ, env, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd'),
        patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)),
        patch(f'{__name__}.add_broken_nightly_label', side_effect=lambda idx: labelled.append(idx)),
    ):
        result = update_issue("77", "Flaky NightlyIntegrationTest", _nightly_body(prior[:-1]))
        assert result is None
        assert labelled == [], "Expected no label when there is no streak"


def test_migrate_table_header():
    # A body with no table at all gets the seven-column header appended.
    plain = "auto-created"
    migrated = migrate_table_header(plain)
    assert migrated.endswith(create_issue_table_header()), migrated
    assert migrated.count(_NEW_HEADER_LINE) == 1, migrated

    # Five- and six-column tables are upgraded in place: header and separator both
    # become seven-column, existing data rows are left untouched.
    old_row = "| 2026-04-14 | test | 1 | [1](url) | [a1b2c3d4](commit/x) |"
    for old_header, old_separator in zip(_OLD_HEADER_LINES, _OLD_SEPARATOR_LINES):
        old_table = f"intro\n\n{old_header}\n{old_separator}\n{old_row}"
        migrated = migrate_table_header(old_table)
        # Full-line membership, not substring: each old separator is a prefix of
        # the next one, so `old_sep in new_sep` is spuriously True as a substring.
        lines = migrated.splitlines()
        assert _NEW_HEADER_LINE in lines and _NEW_SEPARATOR_LINE in lines, migrated
        assert old_header not in lines, "old header line must be replaced"
        assert old_separator not in lines, "old separator must be replaced"
        assert old_row in lines, "existing data rows must be preserved"
        assert migrated.count(_NEW_HEADER_LINE) == 1, migrated

    # Existing rows are moved into the new column order.
    row6 = "| 2026-04-14 | test | 1 | [1](url) | [a1b2c3d4](commit/x) | timeout |"
    migrated = migrate_table_header(f"intro\n\n{_HEADER_6}\n|---|---|---|---|---|---|\n{row6}")
    assert migrated.splitlines()[-1] == (
        "| 2026-04-14 | test | 1 | [1](url) | [a1b2c3d4](commit/x) |  | timeout |"
    ), migrated
    migrated = migrate_table_header(
        f"intro\n\n{_HEADER_6}\n|---|---|---|---|---|---|\n{row6}\n\ntrailing text"
    )
    assert migrated.endswith("\n\ntrailing text"), "text after the table is untouched"

    # An already-migrated body is returned unchanged (idempotent).
    already = f"intro\n\n{_NEW_HEADER_LINE}\n{_NEW_SEPARATOR_LINE}\n{old_row}"
    assert migrate_table_header(already) == already


def test_update_issue_old_format():
    # Test CircleCI
    old_body = (
        "This issue was created automatically by the CI system. Please fix the test before closing the issue.\n\n"
        "2026-04-14 12:05:05 job:test node_index:1 url:https://circleci.com/gh/DACH-NY/canton/1234\n"
        "2026-04-14 13:00:00 job:sequential_test node_index:3 url:https://circleci.com/gh/DACH-NY/canton/5678"
    )
    env_cci = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': 'unknown_hash',
        'CIRCLE_JOB': 'test_job',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }
    with (
        patch.dict(os.environ, env_cci, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd') as mock_gh,
    ):
        update_issue("42", "Flaky test", old_body, "timeout")
        mock_gh.assert_called_once()
        msg = mock_gh.call_args[0][2]
        header = _NEW_HEADER_LINE
        assert header in msg, f"Table header was not injected into old-format body: {msg}"
        assert msg.count(header) == 1, "Table header must appear exactly once"
        assert msg.rstrip().endswith("| timeout |"), f"Cause cell missing from appended row: {msg}"

    # Test GitHub Actions
    env_gha = {
        'CIRCLECI': 'false',
        'GITHUB_ACTIONS': 'true',
        'GITHUB_SHA': 'unknown_hash',
        'GITHUB_JOB': 'test_job',
        'MATRIX_SHARD': '0',
        'GITHUB_RUN_ID': '1234567890',
        'GITHUB_REPOSITORY': 'DACH-NY/canton',
        'GITHUB_SERVER_URL': 'https://github.com',
    }
    with (
        patch.dict(os.environ, env_gha, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd') as mock_gh,
    ):
        update_issue("42", "Flaky test", old_body, "timeout")
        mock_gh.assert_called_once()
        msg = mock_gh.call_args[0][2]
        header = _NEW_HEADER_LINE
        assert header in msg, f"Table header was not injected into old-format body (GHA): {msg}"
        assert msg.count(header) == 1, "Table header must appear exactly once (GHA)"
        assert msg.rstrip().endswith("| timeout |"), (
            f"Cause cell missing from appended row (GHA): {msg}"
        )


def test_update_issue_new_format():
    # New issues already have the table header — the new row is simply appended, no duplicate header
    # Test CircleCI
    new_body = (
        "This issue was created automatically by the CI system. Please fix the test before closing the issue.\n\n"
        "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|\n"
        "| 2026-04-14 12:05:05 | test | 1 | [link](https://circleci.com/gh/DACH-NY/canton/1234) | [a1b2c3d4](https://github.com/DACH-NY/canton/commit/a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2) |"
    )
    env_cci = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': 'unknown_hash',
        'CIRCLE_JOB': 'test_job',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }
    with (
        patch.dict(os.environ, env_cci, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd') as mock_gh,
    ):
        update_issue("42", "Flaky test", new_body, "error_other")
        mock_gh.assert_called_once()
        msg = mock_gh.call_args[0][2]
        header = _NEW_HEADER_LINE
        assert msg.count(header) == 1, (
            f"Six-column header must appear exactly once in updated body: {msg}"
        )
        assert "|---|---|---|---|---|" not in msg.splitlines(), (
            f"Old five-column separator must be upgraded in place: {msg}"
        )
        assert msg.rstrip().endswith("| error_other |"), (
            f"Cause cell missing from appended row: {msg}"
        )

    # Test GitHub Actions
    env_gha = {
        'CIRCLECI': 'false',
        'GITHUB_ACTIONS': 'true',
        'GITHUB_SHA': 'unknown_hash',
        'GITHUB_JOB': 'test_job',
        'MATRIX_SHARD': '0',
        'GITHUB_RUN_ID': '1234567890',
        'GITHUB_REPOSITORY': 'DACH-NY/canton',
        'GITHUB_SERVER_URL': 'https://github.com',
    }
    with (
        patch.dict(os.environ, env_gha, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd') as mock_gh,
    ):
        update_issue("42", "Flaky test", new_body, "error_other")
        mock_gh.assert_called_once()
        msg = mock_gh.call_args[0][2]
        header = _NEW_HEADER_LINE
        assert msg.count(header) == 1, (
            f"Six-column header must appear exactly once in updated body (GHA): {msg}"
        )
        assert "|---|---|---|---|---|" not in msg.splitlines(), (
            f"Old five-column separator must be upgraded in place (GHA): {msg}"
        )
        assert msg.rstrip().endswith("| error_other |"), (
            f"Cause cell missing from appended row (GHA): {msg}"
        )


def _check_update_issue_returns_consecutive_streak_info(job: str, threshold: int):
    # Shared body exercised for both thresholds by the two test_* wrappers below.
    # Build exactly CONSECUTIVE_FAILURES_THRESHOLD commits: prior ones go in the body, last is current
    commits = [format(i, '040x') for i in range(threshold)]
    current = commits[-1]
    prior = commits[:-1]  # CONSECUTIVE_FAILURES_THRESHOLD - 1 commits

    def make_body(commit_list):
        rows = "\n".join(
            f"| 2026-04-{20 + i} | test | 1 | [{i}](url) | [{c[:8]}](https://github.com/DACH-NY/canton/commit/{c}) |"
            for i, c in enumerate(commit_list)
        )
        header = "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|"
        return (
            "This issue was created automatically by the CI system.\n\n"
            + header
            + ("\n" + rows if rows else "")
        )

    consecutive_pairs = set(zip(commits, commits[1:]))

    def all_consecutive(older, newer):
        return (older, newer) in consecutive_pairs

    def not_consecutive(older, newer):
        return False

    # Test CircleCI
    env_cci = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': current,
        'CIRCLE_JOB': job,
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }

    # exactly threshold-1 prior commits, all consecutive → streak fires (CCI)
    with (
        patch.dict(os.environ, env_cci, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd'),
        patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive),
    ):
        result = update_issue("42", "Flaky test", make_body(prior))
        assert result is not None, f"Expected streak with {threshold} consecutive commits (CCI)"
        idx, title, commit = result
        assert idx == "42", f"Expected issue idx '42', got: {idx}"
        assert commit == current, f"Expected current commit in result, got: {commit}"

    # one commit short → not enough history, no streak (CCI)
    with (
        patch.dict(os.environ, env_cci, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd'),
        patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive),
    ):
        result = update_issue("42", "Flaky test", make_body(prior[:-1]))
        assert result is None, f"Expected None with only {len(prior) - 1} prior commits (CCI)"

    # full history but not consecutive → no streak (CCI)
    with (
        patch.dict(os.environ, env_cci, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd'),
        patch(f'{__name__}.are_consecutive_commits', side_effect=not_consecutive),
    ):
        result = update_issue("42", "Flaky test", make_body(prior))
        assert result is None, "Expected None for non-consecutive commits (CCI)"

    # Test GitHub Actions
    env_gha = {
        'CIRCLECI': 'false',
        'GITHUB_ACTIONS': 'true',
        'GITHUB_SHA': current,
        'GITHUB_JOB': job,
        'MATRIX_SHARD': '0',
        'GITHUB_RUN_ID': '1234567890',
        'GITHUB_REPOSITORY': 'DACH-NY/canton',
        'GITHUB_SERVER_URL': 'https://github.com',
    }

    # exactly threshold-1 prior commits, all consecutive → streak fires (GHA)
    with (
        patch.dict(os.environ, env_gha, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd'),
        patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive),
    ):
        result = update_issue("42", "Flaky test", make_body(prior))
        assert result is not None, f"Expected streak with {threshold} consecutive commits (GHA)"
        idx, title, commit = result
        assert idx == "42", f"Expected issue idx '42', got: {idx}"
        assert commit == current, f"Expected current commit in result, got: {commit}"

    # one commit short → not enough history, no streak (GHA)
    with (
        patch.dict(os.environ, env_gha, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd'),
        patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive),
    ):
        result = update_issue("42", "Flaky test", make_body(prior[:-1]))
        assert result is None, f"Expected None with only {len(prior) - 1} prior commits (GHA)"

    # full history but not consecutive → no streak (GHA)
    with (
        patch.dict(os.environ, env_gha, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd'),
        patch(f'{__name__}.are_consecutive_commits', side_effect=not_consecutive),
    ):
        result = update_issue("42", "Flaky test", make_body(prior))
        assert result is None, "Expected None for non-consecutive commits (GHA)"


def test_update_issue_returns_consecutive_streak_info_stable():
    _check_update_issue_returns_consecutive_streak_info("test_job", CONSECUTIVE_FAILURES_THRESHOLD)


def test_update_issue_returns_consecutive_streak_info_unstable():
    _check_update_issue_returns_consecutive_streak_info(
        "unstable_test", CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE
    )


def test_update_issue_returns_consecutive_streak_info_unstable_slow():
    # The slow lane must get the lenient threshold too, not the strict per-commit one.
    _check_update_issue_returns_consecutive_streak_info(
        "unstable_test_slow", CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE
    )


def test_update_issue_dedupes_shard_duplicates():
    # Repeated same-commit rows (multi-shard failures or manual retries) must not
    # shadow the streak check. are_consecutive_commits(X, X) is always False, so
    # before the dedup fix the streak silently never fired for any multi-shard job.
    threshold = CONSECUTIVE_FAILURES_THRESHOLD
    commits = [format(i, '040x') for i in range(threshold)]
    current = commits[-1]
    # Each prior commit has 5 shard failure entries already in the body.
    rows = "\n".join(
        f"| 2026-04-{20 + i} | test | {i % 5} | [{i}](url) | [{c[:8]}](https://github.com/DACH-NY/canton/commit/{c}) |"
        for i, c in enumerate(c for c in commits[:-1] for _ in range(5))
    )
    body = (
        "This issue was created automatically.\n\n"
        "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|\n" + rows
    )
    consecutive_pairs = set(zip(commits, commits[1:]))
    env = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'test_job',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }
    with (
        patch.dict(os.environ, env, clear=False),
        patch(f'{__name__}.gh_issue_edit_cmd'),
        patch(
            f'{__name__}.are_consecutive_commits',
            side_effect=lambda a, b: (a, b) in consecutive_pairs,
        ),
    ):
        result = update_issue("42", "Flaky test", body)
        assert result is not None, "Expected streak to fire after deduping shard-duplicate rows"
        assert result[2] == current


def test_report_issue_assignment_grace_period():
    title = format_issue_title("SomeFlakyTest")
    commits = [format(i, '040x') for i in range(CONSECUTIVE_FAILURES_THRESHOLD)]
    current = commits[-1]
    prior = commits[:-1]
    rows = "\n".join(
        f"| 2026-04-{20 + i} | test | 1 | [{i}](url) | [{c[:8]}](https://github.com/DACH-NY/canton/commit/{c}) |"
        for i, c in enumerate(prior)
    )
    body_with_history = (
        "This issue was created automatically.\n\n"
        "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|\n" + rows
    )
    consecutive_pairs = set(zip(commits, commits[1:]))

    now = datetime.datetime.now(datetime.timezone.utc)
    recent_assignment = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    old_assignment = (now - datetime.timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def make_search_response(total_count, assigned_at=None):
        timeline_nodes = [{"createdAt": assigned_at}] if assigned_at else []
        return json.dumps(
            {
                "data": {
                    "search": {
                        "nodes": [
                            {
                                "title": title,
                                "number": 99,
                                "body": body_with_history,
                                "assignees": {"totalCount": total_count},
                                "timelineItems": {"nodes": timeline_nodes},
                                "projectItems": {"nodes": []},
                            }
                        ]
                    }
                }
            }
        )

    def run_report(env, total_count, assigned_at, assert_edit_called):
        with (
            patch.dict(os.environ, env, clear=False),
            patch(f'{__name__}.gh_issue_edit_cmd') as mock_edit,
            patch(
                f'{__name__}.are_consecutive_commits',
                side_effect=lambda a, b: (a, b) in consecutive_pairs,
            ),
            patch(f'{__name__}.run_gh_with_retries') as mock_gh,
        ):
            mock_gh.return_value.returncode = 0
            mock_gh.return_value.stdout = make_search_response(total_count, assigned_at)
            result = report_issue("SomeFlakyTest")
            if assert_edit_called:
                mock_edit.assert_called_once()
            return result

    # Test CircleCI
    env_cci = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'test_job',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }

    # Test GitHub Actions
    env_gha = {
        'CIRCLECI': 'false',
        'GITHUB_ACTIONS': 'true',
        'GITHUB_SHA': current,
        'GITHUB_JOB': 'test_job',
        'MATRIX_SHARD': '0',
        'GITHUB_RUN_ID': '1234567890',
        'GITHUB_REPOSITORY': 'DACH-NY/canton',
        'GITHUB_SERVER_URL': 'https://github.com',
    }

    for label, env in (("CCI", env_cci), ("GHA", env_gha)):
        # recently assigned issue: body is updated but Slack is suppressed during the grace window
        assert (
            run_report(env, total_count=1, assigned_at=recent_assignment, assert_edit_called=True)
            is None
        ), f"Expected None when assigned within the grace window (no Slack notification) ({label})"

        # assigned before the grace window and still streaking: Slack is not suppressed
        assert (
            run_report(env, total_count=1, assigned_at=old_assignment, assert_edit_called=True)
            is not None
        ), f"Expected streak result when assignment is older than the grace window ({label})"

        # unassigned issue with same streak: Slack is not suppressed
        assert (
            run_report(env, total_count=0, assigned_at=None, assert_edit_called=False) is not None
        ), f"Expected streak result when issue has no assignee ({label})"


def test_report_issue_finds_flaky_project_among_other_projects():
    # The issue is linked to another project before the flaky test project. The lookup must
    # skip the unrelated project and still unarchive the issue / assign its release line.
    title = format_issue_title("SomeFlakyTest")
    search_response = json.dumps(
        {
            "data": {
                "search": {
                    "nodes": [
                        {
                            "title": title,
                            "number": 99,
                            "body": "",
                            "assignees": {"totalCount": 0},
                            "timelineItems": {"nodes": []},
                            "projectItems": {
                                "nodes": [
                                    {
                                        "id": "other_item",
                                        "project": {"id": "other_project"},
                                        "isArchived": False,
                                        "releaseLine": {"name": "main"},
                                    },
                                    {
                                        "id": "flaky_item",
                                        "project": {"id": flaky_test_project},
                                        "isArchived": True,
                                        "releaseLine": None,
                                    },
                                ]
                            },
                        }
                    ]
                }
            }
        }
    )

    with (
        patch(f'{__name__}.run_gh_with_retries') as mock_gh,
        patch(f'{__name__}.gh_unarchive_issue') as mock_unarchive,
        patch(f'{__name__}.gh_assign_release_line') as mock_assign_release_line,
        patch(f'{__name__}.update_issue'),
    ):
        mock_gh.return_value.returncode = 0
        mock_gh.return_value.stdout = search_response
        report_issue("SomeFlakyTest")

    mock_unarchive.assert_called_once_with("99", "flaky_item")
    mock_assign_release_line.assert_called_once_with("99", "flaky_item")


def test_update_issue_assigned_daily_cap():
    # Past the assignment grace period, an assigned issue is alerted on at most once per
    # UTC calendar day, not on every consecutive-failure streak.
    commits = [format(i, '040x') for i in range(CONSECUTIVE_FAILURES_THRESHOLD)]
    current = commits[-1]
    prior = commits[:-1]
    header = "| Date | Job | Node | Build | Commit | Cause |\n|---|---|---|---|---|---|"
    yesterday = (
        datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=1)
    ).isoformat()

    def make_body(row_date):
        rows = "\n".join(
            f"| {row_date} 12:00:00 | test | 1 | [{i}](url) | [{c[:8]}](https://github.com/DACH-NY/canton/commit/{c}) | regular |"
            for i, c in enumerate(prior)
        )
        return "This issue was created automatically.\n\n" + header + ("\n" + rows if rows else "")

    consecutive_pairs = set(zip(commits, commits[1:]))
    old_assignment = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=3)

    env = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'test_job',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }

    def run(body, has_assignee=True):
        with (
            patch.dict(os.environ, env, clear=False),
            patch(f'{__name__}.gh_issue_edit_cmd') as mock_edit,
            patch(
                f'{__name__}.are_consecutive_commits',
                side_effect=lambda a, b: (a, b) in consecutive_pairs,
            ),
        ):
            result = update_issue(
                "42", "Flaky test", body, has_assignee=has_assignee, assigned_at=old_assignment
            )
            return result, mock_edit.call_args[0][2]

    # no failure recorded yet today (history is all from yesterday): first failure of the
    # day, alerts. The written body now carries today's own row.
    result, todays_body = run(make_body(yesterday))
    assert result is not None, "Expected an alert on the first failure of the day"

    # same streak, later the same UTC day: today's row from the previous call already shows
    # a failure today, so this one is suppressed.
    result, _ = run(todays_body)
    assert result is None, "Expected no repeat alert later the same UTC day"

    # unassigned issue: never rate-limited by day, keeps alerting regardless of same-day history.
    result, _ = run(todays_body, has_assignee=False)
    assert result is not None, "Expected unassigned issues to keep alerting on every streak"


def _history_body(dates: list, pad: str = "") -> str:
    rows = "\n".join(
        f"| {d} 12:00:00 | test | 0 | [1](url{pad}) | "
        f"[{i:08x}](https://github.com/DACH-NY/canton/commit/{i:040x}) |  | regular |"
        for i, d in enumerate(dates)
    )
    return f"intro\n\n{create_issue_table_header()}\n{rows}"


def _data_rows(body: str) -> list:
    return [line for line in body.split("\n") if _ROW_DATE_RE.match(line)]


def test_truncate_history():
    today = datetime.date(2026, 10, 22)
    old, recent = "2026-01-05", "2026-10-01"
    n = HISTORY_MAX_ROWS

    # At the row cap: unchanged.
    at_cap = _history_body([old] * n)
    assert truncate_history(at_cap, today) == at_cap

    # Over the cap: the oldest rows go and a marker above the header counts them.
    body = _history_body([old] * (n + 50))
    cut = truncate_history(body, today)
    assert _data_rows(cut) == _data_rows(body)[50:]
    lines = cut.split("\n")
    h = lines.index(_NEW_HEADER_LINE)
    assert lines[h - 3] == "<!-- flaky-truncated 50 -->", lines[h - 3]
    assert lines[h - 2].startswith(
        "_50 earlier rows truncated, oldest kept row is from 2026-01-05."
    ), lines[h - 2]
    assert lines[h - 1] == "" and cut.startswith("intro\n\n"), cut[:200]
    assert truncate_history(cut, today) == cut, "truncating twice changes nothing"

    # The next run adds to the count instead of stacking a second marker.
    again = truncate_history(f"{cut}\n{_data_rows(body)[-1]}", today)
    assert again.count("<!-- flaky-truncated") == 1 and "<!-- flaky-truncated 51 -->" in again

    # Rows inside the keep window survive past the cap; the boundary day is kept.
    boundary = (today - datetime.timedelta(days=HISTORY_KEEP_DAYS)).isoformat()
    just_out = (today - datetime.timedelta(days=HISTORY_KEEP_DAYS + 1)).isoformat()
    body = _history_body([old] * 10 + [just_out] + [boundary] + [recent] * n)
    assert _data_rows(truncate_history(body, today)) == _data_rows(body)[11:]

    # The character cap wins over the keep window; the newest row is always kept.
    body = _history_body([recent] * 400, pad="x" * 500)
    cut = truncate_history(body, today)
    assert len(body) > HISTORY_MAX_CHARS >= len(cut), (len(body), len(cut))
    assert _data_rows(cut)[-1] == _data_rows(body)[-1]
    assert truncate_history(cut, today) == cut

    # Text after the table is kept, a body without the table is left alone.
    assert truncate_history(_history_body([old] * (n + 1)) + "\n\nafter", today).endswith(
        "\n\nafter"
    )
    assert truncate_history("auto-created", today) == "auto-created"

    # Streak detection still sees the most recent commits.
    body = _history_body([old] * (n + 50))
    assert (
        extract_commit_hashes_from_body(truncate_history(body, today))[-n:]
        == extract_commit_hashes_from_body(body)[-n:]
    )


def test_update_issue_truncates_history():
    env = {
        "CIRCLECI": "true",
        "GITHUB_ACTIONS": "false",
        "CIRCLE_SHA1": "unknown",
        "CIRCLE_JOB": "test",
        "CIRCLE_NODE_INDEX": "0",
        "CIRCLE_BUILD_URL": "https://circleci.com/gh/DACH-NY/canton/1",
    }
    edits: list = []

    today = datetime.datetime.now(datetime.timezone.utc).date()
    d = today - datetime.timedelta(days=(HISTORY_KEEP_DAYS + 1))

    with (
        patch.dict(os.environ, env),
        patch(f"{__name__}.gh_issue_edit_cmd", side_effect=lambda i, t, b: edits.append(b)),
    ):
        update_issue("42", "Flaky FooTest", _history_body([str(d)] * (HISTORY_MAX_ROWS + 5)))
    (written,) = edits
    # 5 over the cap plus the row this run appended.
    assert "<!-- flaky-truncated 6 -->" in written, written[:500]
    assert len(_data_rows(written)) == HISTORY_MAX_ROWS


# Ordered most specific first. A timeout is more telling than a regular
# failure, which is more telling than a log error, which beats a log warning.
_CAUSE_PRECEDENCE = ('timeout', 'regular', 'error_other', 'warn_other')


def _cause_rank(cause: str) -> int:
    # Lower rank wins. Log causes may carry a " [Suite]" suffix, so rank on the
    # leading bucket token. An unknown cause sorts last so it never overrides one
    # we recognize.
    bucket = cause.split(' ', 1)[0]
    try:
        return _CAUSE_PRECEDENCE.index(bucket)
    except ValueError:
        # silent-ok: an unknown cause sorts last so it never overrides one we recognize
        return len(_CAUSE_PRECEDENCE)


def most_specific_cause_by_title(failing_tests, causes) -> dict[str, str]:
    """Resolves one cause per issue title across the variants that share it.

    Shard and parameter variants of a test collapse to a single issue title, so
    the cause recorded there must be the most specific across those variants. A
    timeout must not be lost behind an alphabetically earlier regular variant.
    """
    best: dict[str, str] = {}
    for test_name in failing_tests:
        title = format_issue_title(test_name)
        cause = causes.get(test_name, DEFAULT_CAUSE)
        if title not in best or _cause_rank(cause) < _cause_rank(best[title]):
            best[title] = cause
    return best


def run(failing_tests_json=None, streaks_json=None, evidence_json=None):
    out_path = streaks_json or streaks_path()

    if not (should_report_issues() or is_dry_run()):
        print(f"Skipping GitHub issue updates: branch '{branch}' is not a tracked branch.")
        write_streaks(out_path, [])
        return

    in_path = failing_tests_json or failing_tests_path()
    failing_tests = read_failing_tests(in_path)
    causes = read_failing_test_causes(in_path)
    if not failing_tests:
        print("No failing tests to report to GitHub.")
        write_streaks(out_path, [])
        return

    cause_by_title = most_specific_cause_by_title(failing_tests, causes)
    try:
        comments = evidence_comments_by_title(
            failing_tests, cause_by_title, read_evidence(evidence_json)
        )
        print(
            f"Found failure evidence for {len(comments)} of {len(cause_by_title)} issue title(s)."
        )
    except Exception as e:
        # silent-ok: the evidence is an add-on, a broken evidence.json must never
        # stop the issues and their tables from being updated
        print(f"::warning::Could not render failure evidence, posting none: {e}")
        comments = {}

    if is_dry_run():
        write_dry_run_comments(comments)
        write_streaks(out_path, [])
        return

    assert_required_env_vars(ci_required_context_vars())

    # sometimes a fundamental problem would lead to a large number of issues being reported, in that case we limit
    failing_tests_max = FAILING_TESTS_MAX
    num = len(failing_tests)
    if num > failing_tests_max:
        print(f"Found too many failed tests {num}, only report {failing_tests_max}")
    else:
        print(f"Reporting {num} failed tests to github")

    # Deduplicate by formatted name to avoid updating the same issue multiple times
    # when variants of the same test fail (e.g. SomeTest/param=a and SomeTest/param=b
    # both map to the same issue title after format_test_name strips the slash suffix).
    # The cause per title was resolved above so the most specific one across the
    # variants is recorded, not just the cause of the alphabetically first variant.
    seen_titles: set = set()
    duplicates = []
    for test_name in failing_tests[:failing_tests_max]:
        title = format_issue_title(test_name)
        if title in seen_titles:
            print(f"Skipping duplicate issue title: {title}")
            continue
        seen_titles.add(title)
        result = report_issue(test_name, cause_by_title[title], comments.get(title))
        if result is not None:
            duplicates.append(result)

    write_streaks(out_path, duplicates)
    print("Finished updating github issues.")


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Create/update GitHub flaky-test issues.")
    parser.add_argument(
        "--failing-tests-json",
        default=None,
        help="Input path for the failing-tests JSON (default: shared CI temp dir).",
    )
    parser.add_argument(
        "--streaks-json",
        default=None,
        help="Output path for the streaks JSON (default: shared CI temp dir).",
    )
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.self_test:
        run_guarded_self_test(self_test)
        sys.exit(0)
    run(args.failing_tests_json, args.streaks_json)
