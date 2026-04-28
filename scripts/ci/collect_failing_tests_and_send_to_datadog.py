import os
import xml.etree.ElementTree as ET
from typing import Final, Optional, Set
from xml.etree.ElementTree import Element, ElementTree
import re
import sys
import subprocess
import datetime
import tempfile
from pathlib import Path
import json
import urllib.request
from unittest.mock import patch

metric_short_version = "canton.failed_test_grouped"

# The number of consecutive git commits a test must fail on before triggering a Slack alert.
# Set to 3 to avoid alerting on single transient failures or manual CircleCI job retries.
CONSECUTIVE_FAILURES_THRESHOLD: Final[int] = 3

hub_milestone = "31" # flaky tests milestone M97
flaky_test_project = "PVT_kwDOAJX-Fc4AbncN" # https://github.com/orgs/DACH-NY/projects/38/

release_line_field = "PVTSSF_lADOAJX-Fc4AbncNzgcWE1k" # ID of the custom field "Release Line"

# The value of this dictionary points to the ID of the field value of the `Release Line` field in the
# flaky test kanban board.
# If you add a new field, execute listReleaseLineFields.graphql to find the new ID
branches_to_report = {
    "main" : "733298b6", # ID of the value "main" for the Release Line field in the project
    "main-2.x" : "073b4df3" # ID of the value "main-2.x" for the Release Line field in the project
}

hub_cmd = ["hub"]
gh_cmd = ["gh"]

branch = os.environ.get('CIRCLE_BRANCH', '')

# replace GITHUB_TOKEN with GITHUB_FLAKY_TEST_TOKEN, which also is permitted to use the project scope.
# this is required to use the gh command to unarchive/restore issues
gh_flaky_test_env = os.environ.copy()
if "GITHUB_FLAKY_TEST_TOKEN" in gh_flaky_test_env:
    gh_flaky_test_env["GITHUB_TOKEN"] = os.environ["GITHUB_FLAKY_TEST_TOKEN"]

def report_issues_to_hub():
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
            lines = f.readlines() # Assumes that the file is not too big
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
    if len(test_name) > 200:
        print(f"Truncating {test_name} to 200 characters.")
    return test_name[:200]


def report_to_datadog(metric_name: str, test_name: str):
    from datadog import initialize, api
    options = {
        'api_key': os.environ['DATADOG_API_KEY'],
        'api_host': 'https://api.datadoghq.com/'
    }
    initialize(**options)
    send_args = {
        'metric': metric_name,
        'type': 'count',
        'points': 1,
        'tags': [f"name:{format_test_name(test_name)}",
                 f"branch:{branch}",
                 f"url:{os.environ['CIRCLE_BUILD_URL']}",
                 f"container_index:{os.environ['CIRCLE_NODE_INDEX']}",
                 f"job:{os.environ['CIRCLE_JOB']}",
                 ]
    }
    resp = api.Metric.send(**send_args)
    if resp.get('status') != 'ok':
        print(f"Received error response while reporting test '{test_name}': \n {resp}")
        resp2 = api.Metric.send(**send_args)
        if resp2.get('status') != 'ok':
            raise Exception(f"Failed to report test '{test_name}' to Datadog after retry: {resp2}")
        print(f"Received following response upon retrying: \n {resp2}")

def hub_report_issue(issue: str):
    title = format_issue_title(issue)
    idx = None
    project_item_id = None
    release_line = None
    is_archived = False

    # search issues by title. also returns partial matches
    result = subprocess.run(gh_cmd + ["api", "graphql", "-F", "query=@scripts/ci/findIssueByTitle.graphql", "-f", f"searchstr=repo:DACH-NY/canton in:title {title}"], capture_output=True, text=True, env=gh_flaky_test_env)
    check_result(result)
    search_result_json = json.loads(result.stdout)

    # let's look for the exact title in the search result
    for result in search_result_json["data"]["search"]["nodes"]:
        if result["title"] == title:
            idx = result["number"]
            body = result["body"]
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
        return hub_update_issue(idx, title, body)

    else:
        hub_create_issue(title)

def hub_create_issue_table_header():
    return "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|"

def hub_create_issue_line():
    date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit_hash = os.environ['CIRCLE_SHA1']
    commit_url = f"https://github.com/DACH-NY/canton/commit/{commit_hash}"
    build_url = os.environ['CIRCLE_BUILD_URL']
    build_number = build_url.rstrip('/').rsplit('/', 1)[-1]
    job = os.environ['CIRCLE_JOB']
    node_index = os.environ['CIRCLE_NODE_INDEX']
    return f"| {date_str} | {job} | {node_index} | [{build_number}]({build_url}) | [{commit_hash[:8]}]({commit_url}) |"

def gh_assign_release_line(idx: str, project_item_id: str):
    release_line_value = branches_to_report.get(branch, None)
    if release_line_value:
        result = subprocess.run(gh_cmd + ["api", "graphql",
                                          "-F", "query=@scripts/ci/assignReleaseLine.graphql",
                                          "-F", f"issue={project_item_id}",
                                          "-F", f"project={flaky_test_project}",
                                          "-F", f"field={release_line_field}",
                                          "-F", f"value={release_line_value}"], capture_output=True, text=True, env=gh_flaky_test_env)
        check_result(result)
        print(f"Assigned release line \"{release_line_value}\" to issue: {idx}")


def check_result(result):
    if result.returncode != 0:
        print(f"### ERROR while executing hub command!")
        print(f"Command was {result.args}")
        print(f"stderr was {result.stderr}")
        print(f"stdout was {result.stdout}")
        raise Exception("hub command failed")

def hub_cmd_with_input(cmd, body):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(body)
        path = f.name
    try:
        result = subprocess.run(hub_cmd + cmd + ["-F", path], capture_output=True, text=True)
    finally:
        os.unlink(path)
    check_result(result)
    return result

def hub_create_issue(title: str):
    msg = f"""{title}

This issue was created automatically by the CI system. Please fix the test before closing the issue.

{hub_create_issue_table_header()}
{hub_create_issue_line()}
    """
    result = hub_cmd_with_input(["issue", "create", "-M", hub_milestone], msg)
    print(f"Created issue: {result.stdout}")

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
    commit_hash = os.environ['CIRCLE_SHA1']
    commit_link = f"<https://github.com/DACH-NY/canton/commit/{commit_hash}|{commit_hash[:8]}>"
    mention = f"<@{volunteer}> " if volunteer else ""
    description = f"Flaky tests have failed on {CONSECUTIVE_FAILURES_THRESHOLD} consecutive commits ending at {commit_link} on branch `{branch}`."
    issue_lines = []
    for idx, title, _ in duplicates:
        issue_url = f"https://github.com/DACH-NY/canton/issues/{idx}"
        issue_lines.append(f"• <{issue_url}|#{idx}> {title}")
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f":rotating_light: Flaky test alert on {branch}", "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"{mention}{description}\nMain may be broken, can you have a look?"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Affected tests:*\n" + "\n".join(issue_lines)},
        },
    ]
    payload = json.dumps({"channel": channel, "blocks": blocks}).encode("utf-8")
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
            if not result.get("ok"):
                print(f"Slack API error: {result.get('error', 'unknown')}")
            else:
                print(f"Flaky test duplicate summary sent to Slack channel, mentioning volunteer {volunteer}." if volunteer else "Flaky test duplicate summary sent to Slack channel.")
    except Exception as e:
        print(f"Failed to send Slack channel summary: {e}")


def extract_commit_hashes_from_body(body: str) -> list[str]:
    return re.findall(r'commit/([0-9a-f]{40})', body)

def are_consecutive_commits(older_hash: str, newer_hash: str) -> bool:
    """Returns True if newer_hash is a direct child of older_hash in git history."""
    result = subprocess.run(
        gh_cmd + ["api", f"repos/DACH-NY/canton/commits/{newer_hash}", "--jq", ".parents[].sha"],
        capture_output=True, text=True, env=gh_flaky_test_env
    )
    if result.returncode != 0:
        return False
    return older_hash in result.stdout.splitlines()

def hub_update_issue(idx: str, title: str, body: str, threshold: Optional[int] = None) -> Optional[tuple[str, str, str]]:
    if threshold is None:
        threshold = CONSECUTIVE_FAILURES_THRESHOLD
    header = "| Date | Job | Node | Build | Commit |"
    if header not in body:
        body = body + "\n" + hub_create_issue_table_header()
    commit_hash = os.environ['CIRCLE_SHA1']

    consecutive_streak = False
    job = os.environ.get('CIRCLE_JOB', '')
    if commit_hash != 'unknown' and job != 'unstable_test':
        existing_commits = extract_commit_hashes_from_body(body)
        recent = existing_commits[-(threshold - 1):]
        if len(recent) >= threshold - 1:
            all_commits = recent + [commit_hash]
            if all(are_consecutive_commits(a, b) for a, b in zip(all_commits, all_commits[1:])):
                consecutive_streak = True

    msg = f"{title}\n\n{body}\n{hub_create_issue_line()}"
    hub_cmd_with_input(["issue", "update",  "-s", "open", f"{idx}"], msg)
    print(f"Updated issue: {idx} for {title}")
    if consecutive_streak:
        return (idx, title, commit_hash)
    return None

def gh_unarchive_issue(idx: str, project_item_id: str):
    result = subprocess.run(gh_cmd + ["api", "graphql", "-F", "query=@scripts/ci/unarchiveIssue.graphql", "-F", f"issue={project_item_id}", "-F", f"project={flaky_test_project}"], capture_output=True, text=True, env=gh_flaky_test_env)
    check_result(result)
    print(f"Unarchived issue: {idx}")

def self_test():
    test_compute_single_log_failure()
    test_hub_create_issue_line()
    test_hub_update_issue_old_format()
    test_hub_update_issue_new_format()
    test_hub_update_issue_returns_consecutive_streak_info()
    print("All self-checks passed")

def test_hub_create_issue_line():
    env = {
        'CIRCLE_JOB': 'test_with_java17',
        'CIRCLE_NODE_INDEX': '5',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/3196076',
        'CIRCLE_SHA1': 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2',
    }
    with patch.dict(os.environ, env, clear=False):
        header = hub_create_issue_table_header()
        assert '| Date | Job | Node | Build | Commit |' in header, f"Missing header in: {header}"
        line = hub_create_issue_line()
        assert '| test_with_java17 |' in line, f"Missing job in: {line}"
        assert '| 5 |' in line, f"Missing node_index in: {line}"
        assert '[3196076](https://circleci.com/gh/DACH-NY/canton/3196076)' in line, f"Missing build link in: {line}"
        assert '[a1b2c3d4](https://github.com/DACH-NY/canton/commit/a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2)' in line, f"Missing commit in: {line}"

def test_hub_update_issue_old_format():
    # Old issues have flat-text lines with no table header — the header must be injected before the new row
    old_body = (
        "This issue was created automatically by the CI system. Please fix the test before closing the issue.\n\n"
        "2026-04-14 12:05:05 job:test node_index:1 url:https://circleci.com/gh/DACH-NY/canton/1234\n"
        "2026-04-14 13:00:00 job:sequential_test node_index:3 url:https://circleci.com/gh/DACH-NY/canton/5678"
    )
    env = {
        'CIRCLE_SHA1': 'unknown_hash',
        'CIRCLE_JOB': 'test_job',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.hub_cmd_with_input') as mock_hub:
        hub_update_issue("42", "Flaky test", old_body)
        mock_hub.assert_called_once()
        msg = mock_hub.call_args[0][1]
        header = "| Date | Job | Node | Build | Commit |"
        assert header in msg, f"Table header was not injected into old-format body: {msg}"
        assert msg.count(header) == 1, "Table header must appear exactly once"

def test_hub_update_issue_new_format():
    # New issues already have the table header — the new row is simply appended, no duplicate header
    new_body = (
        "This issue was created automatically by the CI system. Please fix the test before closing the issue.\n\n"
        "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|\n"
        "| 2026-04-14 12:05:05 | test | 1 | [link](https://circleci.com/gh/DACH-NY/canton/1234) | [a1b2c3d4](https://github.com/DACH-NY/canton/commit/a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2) |"
    )
    env = {
        'CIRCLE_SHA1': 'unknown_hash',
        'CIRCLE_JOB': 'test_job',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.hub_cmd_with_input') as mock_hub:
        hub_update_issue("42", "Flaky test", new_body)
        mock_hub.assert_called_once()
        msg = mock_hub.call_args[0][1]
        header = "| Date | Job | Node | Build | Commit |"
        assert msg.count(header) == 1, f"Table header must appear exactly once in updated body: {msg}"

def test_hub_update_issue_returns_consecutive_streak_info():
    # Build exactly CONSECUTIVE_FAILURES_THRESHOLD commits: prior ones go in the body, last is current
    commits = [format(i, '040x') for i in range(CONSECUTIVE_FAILURES_THRESHOLD)]
    current = commits[-1]
    prior = commits[:-1]  # CONSECUTIVE_FAILURES_THRESHOLD - 1 commits

    env = {
        'CIRCLE_SHA1': current,
        'CIRCLE_JOB': 'test_job',
        'CIRCLE_NODE_INDEX': '0',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/0000',
    }

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

    # exactly threshold-1 prior commits, all consecutive → streak fires
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.hub_cmd_with_input'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive):
        result = hub_update_issue("42", "Flaky test", make_body(prior))
        assert result is not None, f"Expected streak with {CONSECUTIVE_FAILURES_THRESHOLD} consecutive commits"
        idx, title, commit = result
        assert idx == "42", f"Expected issue idx '42', got: {idx}"
        assert commit == current, f"Expected current commit in result, got: {commit}"

    # one commit short → not enough history, no streak
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.hub_cmd_with_input'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive):
        result = hub_update_issue("42", "Flaky test", make_body(prior[:-1]))
        assert result is None, f"Expected None with only {len(prior) - 1} prior commits"

    # full history but not consecutive → no streak
    with patch.dict(os.environ, env, clear=False), \
         patch(f'{__name__}.hub_cmd_with_input'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=not_consecutive):
        result = hub_update_issue("42", "Flaky test", make_body(prior))
        assert result is None, "Expected None for non-consecutive commits"

    # consecutive streak from unstable_test → no alert
    unstable_env = {**env, 'CIRCLE_JOB': 'unstable_test'}
    with patch.dict(os.environ, unstable_env, clear=False), \
         patch(f'{__name__}.hub_cmd_with_input'), \
         patch(f'{__name__}.are_consecutive_commits', side_effect=all_consecutive):
        result = hub_update_issue("42", "Flaky test", make_body(prior))
        assert result is None, "Expected None when job is unstable_test"


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
    required = ['DATADOG_API_KEY', 'CIRCLE_SHA1', 'CIRCLE_BUILD_URL', 'CIRCLE_JOB', 'CIRCLE_NODE_INDEX']
    missing = [var for var in required if var not in os.environ]
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
        if report_issues_to_hub():
            result = hub_report_issue(failure_name)
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

        if failing_tests_num > 0 and report_issues_to_hub():
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
                result = hub_report_issue(test_name)
                if result is not None:
                    duplicates.append(result)

            if duplicates:
                send_duplicate_summary(duplicates)

            print(f"Finished updating github issues.")

