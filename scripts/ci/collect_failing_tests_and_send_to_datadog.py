import os
import xml.etree.ElementTree as ET
from typing import Final, Optional, Set
from xml.etree.ElementTree import Element, ElementTree
import re
import sys
import subprocess
import datetime
import time
from itertools import groupby
from pathlib import Path
import json
import urllib.request
from unittest.mock import patch

metric_short_version = "canton.failed_test_grouped"

# The number of consecutive git commits a test must fail on before triggering a Slack alert.
# Set to 3 to avoid alerting on single transient failures or manual CircleCI job retries.
# Set to 10 for unstable tests. The trade off is about false alarms vs not detecting broken test.
CONSECUTIVE_FAILURES_THRESHOLD: Final[int] = 3
CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE: Final[int] = 10

# A test that fails on this many consecutive *nightly* runs is treated as broken
# (not merely flaky) and the CI rota is pinged. Nightly runs are days apart and
# land on non-adjacent commits, so the per-commit streak check never fires for
# them; this is the nightly-specific equivalent.
NIGHTLY_CONSECUTIVE_FAILURES: Final[int] = 3
# Label applied to the tracking issue when a nightly streak is detected.
NIGHTLY_BROKEN_LABEL: Final[str] = "broken-nightly"
# Nightly cron from .circleci/config/workflows/canton_nightly.yml: "0 22 * * 1-5".
NIGHTLY_CRON_HOUR_UTC: Final[int] = 22

milestone = "Flaky Tests" # flaky tests milestone M97 (milestone number 31)
flaky_test_project = "PVT_kwDOAJX-Fc4AbncN" # https://github.com/orgs/DACH-NY/projects/38/

release_line_field = "PVTSSF_lADOAJX-Fc4AbncNzgcWE1k" # ID of the custom field "Release Line"

# The value of this dictionary points to the ID of the field value of the `Release Line` field in the
# flaky test kanban board.
# If you add a new field, execute listReleaseLineFields.graphql to find the new ID
branches_to_report = {
    "main" : "733298b6", # ID of the value "main" for the Release Line field in the project
    "main-2.x" : "073b4df3" # ID of the value "main-2.x" for the Release Line field in the project
}

# gha-migration: changed getting branch name, build url, node index, job name
# and commit hash to account for CircleCI and GHA
def is_github_actions_ci() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

def is_circle_ci() -> bool:
    return os.environ.get("CIRCLECI", "").lower() == "true"

def get_ci_branch() -> str:
    if is_github_actions_ci():
        return os.environ.get("GITHUB_HEAD_REF") or os.environ.get("GITHUB_REF_NAME", "")
    if is_circle_ci():
        return os.environ.get("CIRCLE_BRANCH", "")
    return os.environ.get("CIRCLE_BRANCH", "")

def get_ci_build_url() -> str:
    if is_github_actions_ci():
        server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        repository = os.environ.get("GITHUB_REPOSITORY", "")
        run_id = os.environ.get("GITHUB_RUN_ID", "")
        if repository and run_id:
            return f"{server}/{repository}/actions/runs/{run_id}"
        return ""
    if is_circle_ci():
        return os.environ.get("CIRCLE_BUILD_URL", "")
    return os.environ.get("CIRCLE_BUILD_URL", "")

def get_ci_node_index() -> str:
    if is_github_actions_ci():
        return os.environ.get("MATRIX_SHARD", "0")
    if is_circle_ci():
        return os.environ.get("CIRCLE_NODE_INDEX", "0")
    return os.environ.get("CIRCLE_NODE_INDEX", "0")

def get_ci_job_name() -> str:
    if is_circle_ci():
        return os.environ.get("CIRCLE_JOB", "unknown")
    if is_github_actions_ci():
        return os.environ.get("GITHUB_JOB", "unknown")
    return os.environ.get("CIRCLE_JOB", "unknown")

def get_ci_commit_hash() -> str:
    if is_github_actions_ci():
        return os.environ.get("GITHUB_SHA", "unknown")
    if is_circle_ci():
        return os.environ.get("CIRCLE_SHA1", "unknown")
    return os.environ.get("CIRCLE_SHA1", "unknown")

def get_ci_parallel_run_url(build_url: str, node_index: str) -> str:
    if is_github_actions_ci():
        return build_url
    if is_circle_ci():
        return f"{build_url.replace('circleci.com/gh/', 'app.circleci.com/jobs/github/')}/parallel-runs/{node_index}"
    return f"{build_url.replace('circleci.com/gh/', 'app.circleci.com/jobs/github/')}/parallel-runs/{node_index}"


branch = get_ci_branch()

# replace GITHUB_TOKEN with GITHUB_FLAKY_TEST_TOKEN, which also is permitted to use the project scope.
# this is required to use the gh command to unarchive/restore issues
gh_flaky_test_env = os.environ.copy()
if "GITHUB_FLAKY_TEST_TOKEN" in gh_flaky_test_env:
    gh_flaky_test_env["GITHUB_TOKEN"] = os.environ["GITHUB_FLAKY_TEST_TOKEN"]

def should_report_issues():
    return branch in branches_to_report

# sbt generates test-reports in junit xml style that we aggregate into test-reports/<subproject>/<reports> in CircleCI
# through the CircleCI command `upload_test_reports`
# This methods reads those test reports to find out which tests failed
# see also https://www.scala-sbt.org/1.x/docs/Testing.html#Test+Reports
def iterate_through_test_reports():
    failing_tests: Set[str] = set()
    for entry in Path('./test-reports').rglob("*.xml"):
        if entry.is_file():
            process_test_report(entry, failing_tests)
    return failing_tests


def process_test_report(path: Path, failing_tests: Set[str]):
    tree: ElementTree = ET.parse(path)
    root: Element = tree.getroot()
    # To understand this XML parsing I recommend to just go through this code with a debugger on
    # an example test report; alternatively see e.g. https://stackoverflow.com/a/26661423
    for child in root:
        if 'name' not in child.attrib: continue
        contains_fail = any(childchild.tag == 'failure' for childchild in child)
        contains_error = any(childchild.tag == 'error' for childchild in child)
        if contains_fail or contains_error:
            # Example value: LedgerAPIParticipantPruningTestPostgres
            test_name = child.attrib.get('classname', child.attrib.get('name', 'unknown')).split('.')[-1]
            print(f"Found failing test '{test_name}'")
            failing_tests.add(test_name)
    return failing_tests


def check_for_log_failures(failing_tests_result: Set[str]):
    failure = None
    if os.path.exists("found_problems.txt"):
        with open("found_problems.txt", "r") as f:
            lines = f.read().splitlines() # splitlines() strips trailing newlines unlike readlines()
            failure = compute_single_log_failure(lines)

    if failure:
        print(f"Reporting following failure to datadog: '{failure}'")
        failing_tests_result.add(failure)

    return failing_tests_result

def compute_single_log_failure(lines: list[str]):
    if not lines:
        return None
    failures = []
    for line in lines:
        failure = None
        try:
            # If WARN or ERROR (case sensitive) is in the line, we report the line from WARN/ERROR until the end of the logger name
            failure = re.search("((WARN|ERROR).*?)(:| -| tid)", line).group(1)
        except (IndexError, AttributeError):
            pass

        try:
            # Likely an SBT failure
            failure = " ".join(re.search("(warn|error)\\][ ]+\t*(.+)", line).group(1, 2))
        except (IndexError, AttributeError):
            pass

        if failure:
            failures.append(failure)
        else: # Give up and append the untouched line
            failures.append(line)

    # Prefer the longest line as it's likely the most informative
    failures.sort(key=len)
    # Prefer errors over warnings
    errors = [f for f in failures if "error" in f.lower()]
    if errors:
        return errors[-1]
    return failures[-1]

# Datadog only accepts ASCII characters in their tag names
def remove_non_ascii_characters(test_name: str):
    chars = []
    for char in test_name:
        if ord(char) >= 128:
            print(f"Non-ASCII character {char} found in test named {test_name}. Converting to '_'")
            char = '_'
        chars.append(char)
    return ''.join(chars)


def remove_everything_after_first_slash(test_name: str) -> str:
    """
    Useful for flaky tests with variable context, e.g.:
    WARN c.d.c.r.DbStorageSingle:BftOrderingGetConnectedSynchronizersIntegrationTest/sequencerx=sequencer1
    WARN c.d.c.r.DbStorageSingle:BftOrderingGetConnectedSynchronizersIntegrationTest/sequencerx=sequencer2
    will be turned into:
    WARN c.d.c.r.DbStorageSingle:BftOrderingGetConnectedSynchronizersIntegrationTest
    """
    if "/" in test_name:
        return test_name.split("/")[0]
    return test_name


def format_issue_title(test_name: str) -> str:
    return f"[{branch}] Flaky {format_test_name(test_name)}"

def format_test_name(test_name: str):
    test_name = remove_everything_after_first_slash(remove_non_ascii_characters(test_name))
    # Collapse shard partitions so all `FooShard0Test`, `FooShard1Test`, ... map to the same
    # name, since they are the same logical test split across parallel CI runners.
    test_name = re.sub(r'Shard\d+', '', test_name)
    if len(test_name) > 200:
        print(f"Truncating {test_name} to 200 characters.")
    return test_name[:200]


def report_to_datadog(metric_name: str, test_name: str):
    from datadog import initialize, api
    api_key = os.environ.get('DATADOG_API_KEY', '').strip()
    if not api_key:
        raise RuntimeError("DATADOG_API_KEY is empty or missing")
    options = {
        'api_key': api_key,
        'api_host': 'https://api.datadoghq.com/'
    }
    initialize(**options)
    send_args = {
        'metric': metric_name,
        'type': 'count',
        'points': 1,
        'tags': [f"name:{format_test_name(test_name)}",
                 f"branch:{branch}",
                 f"url:{get_ci_build_url()}",
                 f"container_index:{get_ci_node_index()}",
                 f"job:{get_ci_job_name()}",
                 ]
    }
    resp = api.Metric.send(**send_args)
    if resp.get('status') != 'ok':
        print(f"Received error response while reporting test '{test_name}': \n {resp}")
        resp2 = api.Metric.send(**send_args)
        if resp2.get('status') != 'ok':
            raise Exception(f"Failed to report test '{test_name}' to Datadog after retry: {resp2}")
        print(f"Received following response upon retrying: \n {resp2}")

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

def create_issue_table_header():
    return "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|"

def create_issue_table_row():
    date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit_hash = get_ci_commit_hash()
    if is_github_actions_ci():
        repo_owner = os.environ.get('GITHUB_REPOSITORY', 'DACH-NY/canton').split('/', 1)[0]
    else:
        repo_owner = os.environ.get('CIRCLE_PROJECT_USERNAME', 'DACH-NY')
    commit_url = f"https://github.com/{repo_owner}/canton/commit/{commit_hash}"
    build_url = get_ci_build_url()
    build_number = build_url.rstrip('/').rsplit('/', 1)[-1]
    job = get_ci_job_name()
    node_index = get_ci_node_index()
    parallel_run_url = get_ci_parallel_run_url(build_url, node_index)
    return f"| {date_str} | {job} | {node_index} | [{build_number}]({parallel_run_url}) | [{commit_hash[:8]}]({commit_url}) |"

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


GH_RETRY_ATTEMPTS: Final[int] = 3
GH_RETRY_DELAY_SECONDS: Final[int] = 5
TRANSIENT_HTTP_CODES: Final[frozenset] = frozenset({"502", "503", "504"})

def is_transient_gh_error(result) -> bool:
    combined = result.stderr + result.stdout
    return any(code in combined for code in TRANSIENT_HTTP_CODES)

def run_gh_with_retries(args: list, attempts: int = GH_RETRY_ATTEMPTS) -> subprocess.CompletedProcess:
    result = subprocess.run(["gh"] + args, capture_output=True, text=True, env=gh_flaky_test_env)
    if result.returncode != 0 and is_transient_gh_error(result) and attempts > 1:
        print(f"Transient gh error (attempt {GH_RETRY_ATTEMPTS - attempts + 1}/{GH_RETRY_ATTEMPTS}), retrying in {GH_RETRY_DELAY_SECONDS}s...")
        time.sleep(GH_RETRY_DELAY_SECONDS)
        return run_gh_with_retries(args, attempts - 1)
    return result

def check_result(result):
    if result.returncode != 0:
        print(f"### ERROR while executing gh command!")
        print(f"Command was {result.args}")
        print(f"stderr was {result.stderr}")
        print(f"stdout was {result.stdout}")
        raise Exception("gh command failed")

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

def select_volunteer() -> str:
    script = Path(__file__).resolve().parent / 'select_volunteer.sh'
    result = subprocess.run(['bash', str(script)], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"select_volunteer.sh failed: {result.stderr}. No volunteer selected.")
        return ""
    volunteer = result.stdout.strip()
    if not re.match(r'^U[A-Z0-9]+$', volunteer):
        print(f"Unexpected output from select_volunteer.sh: {volunteer!r}. No volunteer selected.")
        return ""
    return volunteer

def send_duplicate_summary(duplicates: list[tuple[str, str, str]]):
    """Post a single summary to the team Slack channel when tests fail repeatedly."""
    token = os.environ.get('SLACK_BOT_QA_NOTIFICATIONS', '')
    channel = os.environ.get('SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS', '')
    if not token:
        print("SLACK_BOT_QA_NOTIFICATIONS not set. Skipping Slack channel summary.")
        return
    if not channel:
        print("SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS not set. Skipping Slack channel summary.")
        return
    volunteer = select_volunteer()
    commit_hash = get_ci_commit_hash()
    commit_link = f"<https://github.com/DACH-NY/canton/commit/{commit_hash}|{commit_hash[:8]}>"
    mention = f"<@{volunteer}> " if volunteer else ""
    nightly = is_nightly_job(get_ci_job_name())
    if nightly:
        alert = f"Broken nightly test alert on {branch}"
        description = (
            f"The following test(s) failed on the last {NIGHTLY_CONSECUTIVE_FAILURES} nightly runs "
            f"on `{branch}` (latest at {commit_link}). Likely genuinely broken, not flaky."
        )
        prompt = "Can you have a look?"
    else:
        alert = f"Flaky test alert on {branch}"
        description = f"Flaky tests have failed on several consecutive commits ending at {commit_link} on branch `{branch}`."
        prompt = "Main may be broken, can you have a look?"
    issue_lines = []
    for idx, title, _ in duplicates:
        issue_url = f"https://github.com/DACH-NY/canton/issues/{idx}"
        issue_lines.append(f"• <{issue_url}|#{idx}> {title}")
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f":this-is-fine-fire: {alert}", "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"{mention}{description}\n{prompt}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Affected tests:*\n" + "\n".join(issue_lines)},
        },
    ]
    payload = json.dumps({
        "channel": channel,
        "blocks": blocks,
        "icon_emoji": ":rotating_light:",
        "text": alert,  # fallback summary for push notifications and screen readers
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            warnings = result.get("response_metadata", {}).get("warnings", [])
            if warnings:
                print(f"Slack response warnings: {warnings}")
            if not result.get("ok"):
                print(f"Slack API error: {result.get('error', 'unknown')}")
            else:
                print(f"Flaky test duplicate summary sent to Slack channel, mentioning volunteer {volunteer}." if volunteer else "Flaky test duplicate summary sent to Slack channel.")
    except Exception as e:
        print(f"Failed to send Slack channel summary: {e}")


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


def is_nightly_job(job: str) -> bool:
    """Nightly test jobs are the `nightly_*` jobs in canton_nightly.yml.

    `unstable_test` also runs nightly but is intentionally allowed to fail, so it
    is excluded: broken-nightly is for genuinely broken tests, not unstable ones.
    """
    return bool(job) and job.startswith("nightly_")


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
    test_gh_flags()
    test_compute_single_log_failure()
    test_format_test_name_collapses_shards()
    test_create_issue_table_row()
    test_update_issue_old_format()
    test_update_issue_new_format()
    test_update_issue_returns_consecutive_streak_info('test_job', CONSECUTIVE_FAILURES_THRESHOLD)
    test_update_issue_returns_consecutive_streak_info('unstable_test', CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE)
    test_update_issue_dedupes_shard_duplicates()
    test_report_issue_skips_slack_if_assignee()
    test_is_nightly_job()
    test_nightly_streak_detection()
    test_recent_nightly_commits_counts_current_run_once()
    test_update_issue_nightly_streak_labels_and_returns()
    print("All self-checks passed")


def _nightly_body(commits: list) -> str:
    rows = "\n".join(
        f"| 2026-04-{20 + i} 00:00:00 | nightly_integration_test | 0 | [{i}](url) | "
        f"[{c[:8]}](https://github.com/DACH-NY/canton/commit/{c}) |"
        for i, c in enumerate(commits)
    )
    header = "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|"
    return "auto-created\n\n" + header + ("\n" + rows if rows else "")


def test_is_nightly_job():
    assert is_nightly_job("nightly_integration_test")
    assert is_nightly_job("nightly_test_upgrades_matrix")
    assert not is_nightly_job("test_with_java17")
    assert not is_nightly_job("unstable_test")  # intentionally unstable, excluded
    assert not is_nightly_job("")


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

def test_format_test_name_collapses_shards():
    # Shard partitions of the same logical test must collapse to the same name
    assert format_test_name("LedgerApiShard0ConformanceTestPostgres") == "LedgerApiConformanceTestPostgres"
    assert format_test_name("LedgerApiShard5ConformanceTestPostgres") == "LedgerApiConformanceTestPostgres"
    # Two-digit shard indices
    assert format_test_name("LedgerApiShard10ConformanceTest") == "LedgerApiConformanceTest"
    # Non-shard names are unchanged
    assert format_test_name("RegularFlakyTest") == "RegularFlakyTest"
    # "Shard" without a digit suffix must not be stripped
    assert format_test_name("ShardingLayerTest") == "ShardingLayerTest"

def test_gh_flags():
    checks = [
        (["issue", "create", "--help"], ["--title", "--body", "--milestone", "--repo"]),
        (["issue", "edit",   "--help"], ["--title", "--body", "--repo"]),
        (["issue", "reopen", "--help"], ["--repo"]),
    ]
    for subcommand, flags in checks:
        result = subprocess.run(["gh"] + subcommand, capture_output=True, text=True)
        help_text = result.stdout + result.stderr
        for flag in flags:
            assert flag in help_text, f"`gh {' '.join(subcommand[:-1])}` help does not mention flag `{flag}` — was it renamed?"

def test_create_issue_table_row():
    # Test CircleCI (DACH-NY)
    env_circleci_dach = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_JOB': 'test_with_java17',
        'CIRCLE_NODE_INDEX': '5',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/3196076',
        'CIRCLE_SHA1': 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2',
        'CIRCLE_PROJECT_USERNAME': 'DACH-NY',
    }
    with patch.dict(os.environ, env_circleci_dach, clear=False):
        header = create_issue_table_header()
        assert '| Date | Job | Node | Build | Commit |' in header, f"Missing header in: {header}"
        line = create_issue_table_row()
        assert '| test_with_java17 |' in line, f"Missing job in: {line}"
        assert '| 5 |' in line, f"Missing node_index in: {line}"
        assert '[3196076](https://app.circleci.com/jobs/github/DACH-NY/canton/3196076/parallel-runs/5)' in line, f"Missing parallel-run build link in: {line}"
        assert '[a1b2c3d4](https://github.com/DACH-NY/canton/commit/a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2)' in line, f"Missing commit in: {line}"

    # Test CircleCI (OSS digital-asset)
    env_circleci_oss = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_JOB': 'test_protocol_version_dev',
        'CIRCLE_NODE_INDEX': '6',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/digital-asset/canton/1491',
        'CIRCLE_SHA1': 'b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3',
        'CIRCLE_PROJECT_USERNAME': 'digital-asset',
    }
    with patch.dict(os.environ, env_circleci_oss, clear=False):
        line = create_issue_table_row()
        assert '[1491](https://app.circleci.com/jobs/github/digital-asset/canton/1491/parallel-runs/6)' in line, f"Missing OSS canton build link in: {line}"
        assert '[b2c3d4e5](https://github.com/digital-asset/canton/commit/b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3)' in line, f"Missing OSS canton commit link in: {line}"

    # Test GitHub Actions
    env_gha = {
        'CIRCLECI': 'false',
        'GITHUB_ACTIONS': 'true',
        'GITHUB_JOB': 'integration-tests-shard-2',
        'MATRIX_SHARD': '2',
        'GITHUB_RUN_ID': '9876543210',
        'GITHUB_REPOSITORY': 'DACH-NY/canton',
        'GITHUB_SERVER_URL': 'https://github.com',
        'GITHUB_SHA': 'c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4',
    }
    with patch.dict(os.environ, env_gha, clear=False):
        line = create_issue_table_row()
        assert '| integration-tests-shard-2 |' in line, f"Missing GHA job name in: {line}"
        assert '| 2 |' in line, f"Missing GHA matrix shard in: {line}"
        assert '[9876543210](https://github.com/DACH-NY/canton/actions/runs/9876543210)' in line, f"Missing GHA build link in: {line}"
        assert '[c3d4e5f6](https://github.com/DACH-NY/canton/commit/c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4)' in line, f"Missing GHA commit link in: {line}"

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

def test_compute_single_log_failure():
    # Logs
    lines = [
        "ERROR i.g.i.ManagedChannelOrphanWrapper - *~*~*~ Channel ManagedChannelImpl{logId=6714, target=localhost:15272} was not shutdown properly!!! ~*~*~*",
        "2021-05-05 12:43:03,509 [...] WARN  c.d.l.p.s.v.SeedService$ - Trying to gather entropy from the underlying operating system to initialized the contract ID seeding, but the entropy pool seems empty.",
        "WARN  c.d.c.p.p.v.ConfirmationResponseFactory:BroadcastPackageUsageIntegrationTest/participant=participant3/synchronizer=da tid:8c53516be1ff7a431b87322eebf2d4ae - Malformed request RequestId(2021-05-05T11:00:30.588840Z). DAMLeError(Error(Contract could not be found with id ContractId(00c0d9eb114b6eec91c8837bad7975c19e0739e7d25cde0c8b9c4446b0cff1a81fca001220a20c40f20f8a329e3874819a5af2e9107e808e8c091d3dd2a4f7aceff24cfcee)))",
        "ERROR c.d.c.p.a.BroadcastPackageUsageService:BroadcastPackageUsageIntegrationTest/participant=participant3 tid:ef410c4f0c6bbc7664a1cd07f94644d7 - An unexpected exception occurred while updating UsePackage contracts."
    ]
    expected = "ERROR c.d.c.p.a.BroadcastPackageUsageService"
    actual = compute_single_log_failure(lines)
    assert actual == expected, f"Expected '{expected}', got '{actual}'"

    # SBT failures
    lines = [
        "[warn] ",
        "[warn] 	Note: Some unresolved dependencies have extra attributes.  Check that these dependencies exist with the requested attributes.",
        "[warn] 		io.spray:sbt-revolver:0.9.1 (sbtVersion=1.0, scalaVersion=2.12)",
        "[warn] ",
        "[warn] 	Note: Unresolved dependencies path:",
        "[error] sbt.librarymanagement.ResolveException: Error downloading io.spray:sbt-revolver;sbtVersion=1.0;scalaVersion=2.12:0.9.1"
    ]
    expected = "error sbt.librarymanagement.ResolveException: Error downloading io.spray:sbt-revolver;sbtVersion=1.0;scalaVersion=2.12:0.9.1"
    actual = compute_single_log_failure(lines)
    assert actual == expected, f"Expected '{expected}', got '{actual}'"

    # General
    lines = [
        "unmatched",
        "longer unmatched"
    ]
    expected = "longer unmatched"
    actual = compute_single_log_failure(lines)
    assert actual == expected, f"Expected '{expected}', got '{actual}'"

def assert_required_env_vars():
    """Ensures all expected CI environment variables are present before execution."""
    if is_github_actions_ci():
        required = ['DATADOG_API_KEY', 'GITHUB_SHA', 'GITHUB_JOB', 'GITHUB_RUN_ID', 'GITHUB_REPOSITORY', 'MATRIX_SHARD']
    elif is_circle_ci():
        required = ['DATADOG_API_KEY', 'CIRCLE_SHA1', 'CIRCLE_BUILD_URL', 'CIRCLE_JOB', 'CIRCLE_NODE_INDEX']
    else:
        # Keep backward compatibility in unknown CI contexts with CircleCI-style vars.
        required = ['DATADOG_API_KEY', 'CIRCLE_SHA1', 'CIRCLE_BUILD_URL', 'CIRCLE_JOB', 'CIRCLE_NODE_INDEX']
    missing = [var for var in required if not os.environ.get(var)]
    if missing:
        raise RuntimeError(f"Cannot execute CI script. Missing required environment variables: {', '.join(missing)}")

if __name__ == "__main__":
    env_before = dict(os.environ)
    try:
        self_test()
    finally:
        env_after = dict(os.environ)
        if env_before != env_after:
            leaked = {k: (env_before.get(k), env_after.get(k)) for k in env_before.keys() | env_after.keys() if env_before.get(k) != env_after.get(k)}
            raise RuntimeError(f"self_test() polluted os.environ (before -> after): {leaked}")
    assert_required_env_vars()
    if len(sys.argv) == 2:
        failure_name = sys.argv[1]
        print(f"Reporting following CI failure as a failed test to datadog: {failure_name}")
        report_to_datadog(metric_name=metric_short_version, test_name=failure_name)
        if should_report_issues():
            result = report_issue(failure_name)
            if result is not None:
                send_duplicate_summary([result])
    else:
        print("Starting to iterate through generated test reports.")
        failing_tests = iterate_through_test_reports()
        print("Now checking if any log problems were found.")
        failing_tests = check_for_log_failures(failing_tests)
        failing_tests_num = len(failing_tests)
        print(
            f"Finished iterating through test reports and log problems. "
            f"In total, found {failing_tests_num} different failed tests after slight deduplication."
            )
        print(f"Starting to report failed tests to datadog")
        for test_name in failing_tests:
            report_to_datadog(metric_short_version, test_name)
        print("Finished reporting failed tests to datadog.")

        # sometimes a fundamental problem would lead to a large number of issues being reported, in that case we limit
        failing_tests_max = 20

        if failing_tests_num > 0 and not should_report_issues():
            print(f"Skipping GitHub issue updates: branch '{branch}' is not a tracked branch.")
        elif failing_tests_num > 0:
            if failing_tests_num > failing_tests_max:
                print(f"Found too many failed tests {failing_tests_num}, only report {failing_tests_max}")
            else:
                print(f"Reporting {failing_tests_num} failed tests to github")

            # Deduplicate by formatted name to avoid updating the same issue multiple times
            # when variants of the same test fail (e.g. SomeTest/param=a and SomeTest/param=b
            # both map to the same issue title after format_test_name strips the slash suffix).
            seen_titles: set = set()
            duplicates = []
            for test_name in list(failing_tests)[:failing_tests_max]:
                title = format_issue_title(test_name)
                if title in seen_titles:
                    print(f"Skipping duplicate issue title: {title}")
                    continue
                seen_titles.add(title)
                result = report_issue(test_name)
                if result is not None:
                    duplicates.append(result)

            if duplicates:
                send_duplicate_summary(duplicates)

            print(f"Finished updating github issues.")

