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
from itertools import groupby
from typing import Optional
from unittest.mock import patch

from flaky_common import (
    branch,
    should_report_issues,
    is_nightly_job,
    format_issue_title,
    run_gh_with_retries,
    check_result,
    create_issue_table_header,
    create_issue_table_row,
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
    write_streaks,
    failing_tests_path,
    streaks_path,
    assert_required_env_vars,
    ci_required_context_vars,
    run_guarded_self_test,
)


def report_issue(issue: str):
    title = format_issue_title(issue)
    idx = None
    project_item_id = None
    release_line = None
    is_archived = False
    has_assignee = False

    # search issues by title. also returns partial matches
    result = run_gh_with_retries(["api", "graphql", "-F", "query=@scripts/ci/findIssueByTitle.graphql", "-f", f"searchstr=repo:DACH-NY/canton in:title {title}"])
    check_result(result)
    search_result_json = json.loads(result.stdout)

    # let's look for the exact title in the search result
    for result in search_result_json["data"]["search"]["nodes"]:
        if result["title"] == title:
            idx = str(result["number"])
            body = result["body"]
            has_assignee = result["assignees"]["totalCount"] > 0
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

        # update the description and reopen if needed
        streak = update_issue(idx, title, body)
        if has_assignee and streak is not None:
            print(f"Issue #{idx} has an assignee; skipping Slack notification.")
            return None
        return streak

    else:
        create_issue(title)

def gh_assign_release_line(idx: str, project_item_id: str):
    release_line_value = branches_to_report.get(branch, None)
    if release_line_value:
        result = run_gh_with_retries(["api", "graphql",
                                  "-F", "query=@scripts/ci/assignReleaseLine.graphql",
                                  "-F", f"issue={project_item_id}",
                                  "-F", f"project={flaky_test_project}",
                                  "-F", f"field={release_line_field}",
                                  "-F", f"value={release_line_value}"])
        check_result(result)
        print(f"Assigned release line \"{release_line_value}\" to issue: https://github.com/DACH-NY/canton/issues/{idx}")


def gh_issue_create_cmd(title: str, body: str):
    result = run_gh_with_retries(["issue", "create", "--repo", "DACH-NY/canton",
                               "--title", title, "--body", body, "--milestone", milestone])
    check_result(result)
    return result

def gh_issue_edit_cmd(idx: str, title: str, body: str):
    # gh issue edit has no --state flag; reopen separately first
    idx = str(idx)
    reopen_result = run_gh_with_retries(["issue", "reopen", idx, "--repo", "DACH-NY/canton"])
    if reopen_result.returncode != 0 and "already open" not in reopen_result.stderr.lower():
        check_result(reopen_result)
    result = run_gh_with_retries(["issue", "edit", idx, "--repo", "DACH-NY/canton",
                  "--title", title, "--body", body])
    check_result(result)
    return result

def create_issue(title: str):
    body = f"""This issue was created automatically by the CI system. Please fix the test before closing the issue.

{create_issue_table_header()}
{create_issue_table_row()}"""
    result = gh_issue_create_cmd(title, body)
    print(f"Created issue: {result.stdout.strip()}")


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
        capture_output=True, text=True,
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

    fetch = subprocess.run(["git", "fetch", "--quiet", "origin", "main"], capture_output=True, text=True)
    if fetch.returncode != 0:
        # Stale origin/main would mis-enumerate the nightly commits; log and carry on.
        print(f"recent_nightly_commits: 'git fetch origin main' failed, using local origin/main: {fetch.stderr.strip()}")
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


def are_consecutive_commits(older_hash: str, newer_hash: str) -> bool:
    """Returns True if newer_hash is a direct child of older_hash in git history."""
    result = run_gh_with_retries(
        ["api", f"repos/DACH-NY/canton/commits/{newer_hash}", "--jq", ".parents[].sha"]
    )
    if result.returncode != 0:
        print(f"are_consecutive_commits: gh api failed for {newer_hash[:8]}: {result.stderr.strip()}")
        return False
    return older_hash in result.stdout.splitlines()

def update_issue(idx: str, title: str, body: str) -> Optional[tuple[str, str, str]]:
    job = get_ci_job_name()
    threshold = CONSECUTIVE_FAILURES_THRESHOLD if job != 'unstable_test' else CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE

    header = "| Date | Job | Node | Build | Commit |"
    if header not in body:
        body = body + "\n" + create_issue_table_header()
    commit_hash = get_ci_commit_hash()

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
            distinct = [c for c, _ in groupby(extract_commit_hashes_from_body(body) + [commit_hash])]
            recent = distinct[-threshold:]
            if len(recent) >= threshold and all(
                are_consecutive_commits(a, b) for a, b in zip(recent, recent[1:])
            ):
                consecutive_streak = True

    new_body = f"{body}\n{create_issue_table_row()}"
    gh_issue_edit_cmd(idx, title, new_body)
    print(f"Updated issue: https://github.com/DACH-NY/canton/issues/{idx}")
    if consecutive_streak:
        if nightly:
            add_broken_nightly_label(idx)
        return (idx, title, commit_hash)
    return None

def gh_unarchive_issue(idx: str, project_item_id: str):
    result = run_gh_with_retries(["api", "graphql", "-F", "query=@scripts/ci/unarchiveIssue.graphql", "-F", f"issue={project_item_id}", "-F", f"project={flaky_test_project}"])
    check_result(result)
    print(f"Unarchived issue: https://github.com/DACH-NY/canton/issues/{idx}")


def self_test():
    test_update_issue_old_format()
    test_update_issue_new_format()
    test_update_issue_returns_consecutive_streak_info('test_job', CONSECUTIVE_FAILURES_THRESHOLD)
    test_update_issue_returns_consecutive_streak_info('unstable_test', CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE)
    test_update_issue_dedupes_shard_duplicates()
    test_report_issue_skips_slack_if_assignee()
    test_nightly_streak_detection()
    test_recent_nightly_commits_counts_current_run_once()
    test_update_issue_nightly_streak_labels_and_returns()
    print("manage_flaky_issues self-checks passed")


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
        'CIRCLECI': 'true', 'GITHUB_ACTIONS': 'false', 'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'nightly_integration_test', 'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }

    # failed on each of the last 3 nightly runs (priors in body + current) -> streak
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)):
        assert nightly_streak(_nightly_body(prior)) is True

    # one of the last-3 nightly commits is missing from the body -> no streak
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)):
        assert nightly_streak(_nightly_body(prior[:-1])) is False

    # the prior failures are non-nightly rows -> ignored -> no streak
    nonnightly = _nightly_body(prior).replace("nightly_integration_test", "test_with_java17")
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)):
        assert nightly_streak(nonnightly) is False

    # fewer than N nightly runs known -> never flag
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits[:1])):
        assert nightly_streak(_nightly_body(prior)) is False


def test_recent_nightly_commits_counts_current_run_once():
    """Regression: the current run's own cron slot aliases `current`, so the
    enumeration must still produce N distinct commits (current + N-1 priors).
    Exercises the real recent_nightly_commits()/_previous_nightly_datetimes,
    so it fails if the slot count regresses to N-1."""
    global _nightly_commits_cache
    n = NIGHTLY_CONSECUTIVE_FAILURES
    current = format(0xc, '040x')
    priors = [format(i, '040x') for i in range(1, n)]
    env = {
        'CIRCLECI': 'true', 'GITHUB_ACTIONS': 'false', 'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'nightly_integration_test', 'CIRCLE_NODE_INDEX': '0',
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
        return priors[i - 1] if i - 1 < len(priors) else format(0xdead + i, '040x')

    saved_cache = _nightly_commits_cache
    _nightly_commits_cache = None
    try:
        with patch.dict(os.environ, env, clear=False), \
             patch(f'{__name__}.subprocess.run',
                   return_value=subprocess.CompletedProcess([], 0, '', '')), \
             patch(f'{__name__}._git_commit_before', side_effect=fake_before):
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
        'CIRCLECI': 'true', 'GITHUB_ACTIONS': 'false', 'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'nightly_integration_test', 'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }

    labelled: list = []
    # streak -> returns issue tuple and applies the broken-nightly label
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)), \
         patch(f'{__name__}.add_broken_nightly_label', side_effect=lambda idx: labelled.append(idx)):
        result = update_issue("77", "Flaky NightlyIntegrationTest", _nightly_body(prior))
        assert result is not None, "Expected a nightly streak to fire"
        assert result[0] == "77"
        assert labelled == ["77"], "Expected the broken-nightly label to be applied"

    # no streak -> no label, returns None
    labelled.clear()
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.recent_nightly_commits', return_value=tuple(commits)), \
         patch(f'{__name__}.add_broken_nightly_label', side_effect=lambda idx: labelled.append(idx)):
        result = update_issue("77", "Flaky NightlyIntegrationTest", _nightly_body(prior[:-1]))
        assert result is None
        assert labelled == [], "Expected no label when there is no streak"


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
    with patch.dict(os.environ, env_cci, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd') as mock_gh:
        update_issue("42", "Flaky test", old_body)
        mock_gh.assert_called_once()
        msg = mock_gh.call_args[0][2]
        header = "| Date | Job | Node | Build | Commit |"
        assert header in msg, f"Table header was not injected into old-format body: {msg}"
        assert msg.count(header) == 1, "Table header must appear exactly once"

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
    with patch.dict(os.environ, env_gha, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd') as mock_gh:
        update_issue("42", "Flaky test", old_body)
        mock_gh.assert_called_once()
        msg = mock_gh.call_args[0][2]
        header = "| Date | Job | Node | Build | Commit |"
        assert header in msg, f"Table header was not injected into old-format body (GHA): {msg}"
        assert msg.count(header) == 1, "Table header must appear exactly once (GHA)"

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
    with patch.dict(os.environ, env_cci, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd') as mock_gh:
        update_issue("42", "Flaky test", new_body)
        mock_gh.assert_called_once()
        msg = mock_gh.call_args[0][2]
        header = "| Date | Job | Node | Build | Commit |"
        assert msg.count(header) == 1, f"Table header must appear exactly once in updated body: {msg}"

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
    with patch.dict(os.environ, env_gha, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd') as mock_gh:
        update_issue("42", "Flaky test", new_body)
        mock_gh.assert_called_once()
        msg = mock_gh.call_args[0][2]
        header = "| Date | Job | Node | Build | Commit |"
        assert msg.count(header) == 1, f"Table header must appear exactly once in updated body (GHA): {msg}"

def test_update_issue_returns_consecutive_streak_info(job: str, threshold: int):
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
        return "This issue was created automatically by the CI system.\n\n" + header + ("\n" + rows if rows else "")

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
    with patch.dict(os.environ, env_cci, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive):
        result = update_issue("42", "Flaky test", make_body(prior))
        assert result is not None, f"Expected streak with {threshold} consecutive commits (CCI)"
        idx, title, commit = result
        assert idx == "42", f"Expected issue idx '42', got: {idx}"
        assert commit == current, f"Expected current commit in result, got: {commit}"

    # one commit short → not enough history, no streak (CCI)
    with patch.dict(os.environ, env_cci, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive):
        result = update_issue("42", "Flaky test", make_body(prior[:-1]))
        assert result is None, f"Expected None with only {len(prior) - 1} prior commits (CCI)"

    # full history but not consecutive → no streak (CCI)
    with patch.dict(os.environ, env_cci, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=not_consecutive):
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
    with patch.dict(os.environ, env_gha, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive):
        result = update_issue("42", "Flaky test", make_body(prior))
        assert result is not None, f"Expected streak with {threshold} consecutive commits (GHA)"
        idx, title, commit = result
        assert idx == "42", f"Expected issue idx '42', got: {idx}"
        assert commit == current, f"Expected current commit in result, got: {commit}"

    # one commit short → not enough history, no streak (GHA)
    with patch.dict(os.environ, env_gha, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive):
        result = update_issue("42", "Flaky test", make_body(prior[:-1]))
        assert result is None, f"Expected None with only {len(prior) - 1} prior commits (GHA)"

    # full history but not consecutive → no streak (GHA)
    with patch.dict(os.environ, env_gha, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=not_consecutive):
        result = update_issue("42", "Flaky test", make_body(prior))
        assert result is None, "Expected None for non-consecutive commits (GHA)"


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
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=lambda a, b: (a, b) in consecutive_pairs):
        result = update_issue("42", "Flaky test", body)
        assert result is not None, "Expected streak to fire after deduping shard-duplicate rows"
        assert result[2] == current


def test_report_issue_skips_slack_if_assignee():
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

    def make_search_response(total_count):
        return json.dumps({"data": {"search": {"nodes": [{
            "title": title,
            "number": 99,
            "body": body_with_history,
            "assignees": {"totalCount": total_count},
            "projectItems": {"nodes": []},
        }]}}})

    # Test CircleCI
    env_cci = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'test_job',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }

    # assigned issue: body is updated but Slack is suppressed (CCI)
    with patch.dict(os.environ, env_cci, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd') as mock_edit, \
         patch(f'{__name__}.are_consecutive_commits', side_effect=lambda a, b: (a, b) in consecutive_pairs), \
         patch(f'{__name__}.run_gh_with_retries') as mock_gh:
        mock_gh.return_value.returncode = 0
        mock_gh.return_value.stdout = make_search_response(total_count=1)
        result = report_issue("SomeFlakyTest")
        assert result is None, "Expected None when issue has an assignee (no Slack notification) (CCI)"
        mock_edit.assert_called_once()

    # unassigned issue with same streak: Slack is not suppressed (CCI)
    with patch.dict(os.environ, env_cci, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=lambda a, b: (a, b) in consecutive_pairs), \
         patch(f'{__name__}.run_gh_with_retries') as mock_gh:
        mock_gh.return_value.returncode = 0
        mock_gh.return_value.stdout = make_search_response(total_count=0)
        result = report_issue("SomeFlakyTest")
        assert result is not None, "Expected streak result when issue has no assignee (CCI)"

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

    # assigned issue: body is updated but Slack is suppressed (GHA)
    with patch.dict(os.environ, env_gha, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd') as mock_edit, \
         patch(f'{__name__}.are_consecutive_commits', side_effect=lambda a, b: (a, b) in consecutive_pairs), \
         patch(f'{__name__}.run_gh_with_retries') as mock_gh:
        mock_gh.return_value.returncode = 0
        mock_gh.return_value.stdout = make_search_response(total_count=1)
        result = report_issue("SomeFlakyTest")
        assert result is None, "Expected None when issue has an assignee (no Slack notification) (GHA)"
        mock_edit.assert_called_once()

    # unassigned issue with same streak: Slack is not suppressed (GHA)
    with patch.dict(os.environ, env_gha, clear=False), \
         patch(f'{__name__}.gh_issue_edit_cmd'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=lambda a, b: (a, b) in consecutive_pairs), \
         patch(f'{__name__}.run_gh_with_retries') as mock_gh:
        mock_gh.return_value.returncode = 0
        mock_gh.return_value.stdout = make_search_response(total_count=0)
        result = report_issue("SomeFlakyTest")
        assert result is not None, "Expected streak result when issue has no assignee (GHA)"


def run(failing_tests_json=None, streaks_json=None):
    out_path = streaks_json or streaks_path()

    if not should_report_issues():
        print(f"Skipping GitHub issue updates: branch '{branch}' is not a tracked branch.")
        write_streaks(out_path, [])
        return

    in_path = failing_tests_json or failing_tests_path()
    failing_tests = read_failing_tests(in_path)
    if not failing_tests:
        print("No failing tests to report to GitHub.")
        write_streaks(out_path, [])
        return

    assert_required_env_vars(ci_required_context_vars())

    # sometimes a fundamental problem would lead to a large number of issues being reported, in that case we limit
    failing_tests_max = 20
    num = len(failing_tests)
    if num > failing_tests_max:
        print(f"Found too many failed tests {num}, only report {failing_tests_max}")
    else:
        print(f"Reporting {num} failed tests to github")

    # Deduplicate by formatted name to avoid updating the same issue multiple times
    # when variants of the same test fail (e.g. SomeTest/param=a and SomeTest/param=b
    # both map to the same issue title after format_test_name strips the slash suffix).
    seen_titles: set = set()
    duplicates = []
    for test_name in failing_tests[:failing_tests_max]:
        title = format_issue_title(test_name)
        if title in seen_titles:
            print(f"Skipping duplicate issue title: {title}")
            continue
        seen_titles.add(title)
        result = report_issue(test_name)
        if result is not None:
            duplicates.append(result)

    write_streaks(out_path, duplicates)
    print("Finished updating github issues.")


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Create/update GitHub flaky-test issues.")
    parser.add_argument("--failing-tests-json", default=None,
                        help="Input path for the failing-tests JSON (default: shared CI temp dir).")
    parser.add_argument("--streaks-json", default=None,
                        help="Output path for the streaks JSON (default: shared CI temp dir).")
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.self_test:
        run_guarded_self_test(self_test)
        sys.exit(0)
    run(args.failing_tests_json, args.streaks_json)
