import os
import xml.etree.ElementTree as ET
from typing import Set, Tuple
from xml.etree.ElementTree import Element, ElementTree
import re
import sys
import subprocess
import datetime
from pathlib import Path
import json

metric_short_version = "canton.failed_test_grouped"

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

hub_cmd = [".ci/nix-exec", "hub"]
gh_cmd = [".ci/nix-exec", "gh"]

branch = os.environ['CIRCLE_BRANCH']

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
        if entry.name.endswith(".xml") and entry.is_file():
            process_test_report(entry, failing_tests)
    return failing_tests


def process_test_report(path: Path, failing_tests: Set[str]):
    tree: ElementTree = ET.parse(path)
    root: Element = tree.getroot()
    # To understand this XML parsing I recommend to just go through this code with a debugger on
    # an example test report; alternatively see e.g. https://stackoverflow.com/a/26661423
    for child in root:
        if 'name' not in child.attrib: continue
        contains_fail = any([childchild.tag == 'failure' for childchild in child])
        contains_error = any([childchild.tag == 'error' for childchild in child])
        if contains_fail or contains_error:
            # Example value: LedgerAPIParticipantPruningTestPostgres
            test_name = child.attrib['classname'].split('.')[-1]
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
        failure = failure[:100]
        print(f"Reporting following failure to datadog: '{failure}'")
        failing_tests_result.add(failure)

    return failing_tests_result

def compute_single_log_failure(lines: list[str]):
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

# Datadog only accepts unicode characters in their tag names
def remove_non_unicode_characters(test_name: str):
    chars = []
    for char in test_name:
        if ord(char) >= 128:
            print(f"Non-unicode character {char} found in test named {test_name}. Converting to '_'")
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


def format_test_name(test_name: str):
    test_name = remove_everything_after_first_slash(remove_non_unicode_characters(test_name))
    if len(test_name) > 200:
        print(f"Truncating {test_name} to 200 characters.")
    return test_name[:200]


def report_to_datadog(metric_name: str, test_name: str):
    # Requires Python 3.7+ (f-strings & type hints)
    from datadog import initialize, api
    options = {
        'api_key': os.environ['DATADOG_API_KEY'],
        'api_host': 'https://api.datadoghq.com/'
    }
    initialize(**options)
    send_args = {
        'metric': f'{metric_name}',
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
    if resp != {'status': 'ok'}:
        print(f"Received error response while reporting test '{test_name}': \n {resp}")
        resp2 = api.Metric.send(**send_args)
        print(f"Received following response upon retrying: \n {resp2} ")

def hub_report_issue(issue: str):
    formatted = format_test_name(issue)
    title = f"[{branch}] Flaky {formatted}"
    idx = None
    project_item_id = None
    release_line = None
    is_archived = False

    # search issues by title. also returns partial matches
    result = subprocess.run(gh_cmd + ["api", "graphql", "-F", "query=@scripts/ci/findIssueByTitle.graphql", "-f", f"searchstr=repo:DACH-NY/canton in:title {title}"], capture_output=True, text=True, env=gh_flaky_test_env)
    check_result(result)
    search_result_json = json.loads(result.stdout)

    # let's look for the exact title in the search result
    for issue in search_result_json["data"]["search"]["nodes"]:
        if issue["title"] == title:
            idx = issue["number"]
            body = issue["body"]
            # look at the projects the issue is linked to
            for project_relation in issue["projectItems"]["nodes"]:

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
        hub_update_issue(idx, title, body)

    else:
        hub_create_issue(title)

def hub_create_issue_line():
    date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{date_str} job:{os.environ['CIRCLE_JOB']} node_index:{os.environ['CIRCLE_NODE_INDEX']} url:{os.environ['CIRCLE_BUILD_URL']}"

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

def hub_cmd_with_input(cmd, input):
    with open("message.txt", "w") as f:
        f.write(input)
        f.close()
        result = subprocess.run(hub_cmd + cmd + ["-F", "message.txt"], capture_output=True, text=True)
    check_result(result)
    return result

def hub_create_issue(title: str):
    msg = f"""{title}

This issue was created automatically by the CI system. Please fix the test before closing the issue.

{hub_create_issue_line()}
    """
    result = hub_cmd_with_input(["issue", "create", "-M", hub_milestone], msg)
    print(f"Created issue: {result.stdout}")

def hub_update_issue(idx: str, title: str, body: str):
    msg = title + "\n\n" + body + "\n" + hub_create_issue_line()
    hub_cmd_with_input(["issue", "update",  "-s", "open", f"{idx}"], msg)
    print(f"Updated issue: {idx} for {title}")

def gh_unarchive_issue(idx: str, project_item_id: str):
    result = subprocess.run(gh_cmd + ["api", "graphql", "-F", "query=@scripts/ci/unarchiveIssue.graphql", "-F", f"issue={project_item_id}", "-F", f"project={flaky_test_project}"], capture_output=True, text=True, env=gh_flaky_test_env)
    check_result(result)
    print(f"Unarchived issue: {idx}")

def self_test():
    test_compute_single_log_failure()
    print("All self-checks passed")

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

if __name__ == "__main__":
    self_test()
    if len(sys.argv) == 2:
        failure_name = sys.argv[1]
        print(f"Reporting following CI failure as a failed test to datadog: {failure_name}")
        report_to_datadog(metric_name=metric_short_version, test_name=failure_name)
        if report_issues_to_hub():
            hub_report_issue(failure_name)
    else:
        print("Starting to iterate through generated test reports")
        failing_tests = iterate_through_test_reports()
        print("Now checking if any log problems were found")
        failing_tests = check_for_log_failures(failing_tests)
        failing_tests_num = len(failing_tests)
        print(
            f"Finished iterating through test reports and log problems. "
            f"In total, found {failing_tests_num} different failed tests after slight deduplication."
            )
        print(f"Starting to report failed tests to datadog")
        for test_name in failing_tests:
            report_to_datadog(metric_short_version, test_name)
        print("Finished reporting failed tests to datadog")

        # sometimes a fundamental problem would lead to a large number of issues being reported, in that case we limit
        failing_tests_max = 20

        if failing_tests_num > 0 and report_issues_to_hub():
            if failing_tests_num > failing_tests_max:
                print(f"Found too many failed tests {failing_tests_num}, only report {failing_tests_max}")
            else:
                print(f"Reporting {failing_tests_num} failed tests to github")

            for test_name in list(failing_tests)[:failing_tests_max]:
                hub_report_issue(test_name)

            print(f"Finished updating github issues")

