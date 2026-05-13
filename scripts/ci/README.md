# CI scripts

## Flaky test notification system

`collect_failing_tests_and_send_to_datadog.py` tracks test failures across CI runs. For every failing test it posts a metric to Datadog, maintains a GitHub issue on `main` and `main-2.x`, and sends a Slack alert when the same test fails on several consecutive commits.

It is invoked via the `collect_failing_test_data_and_send_to_datadog` CircleCI command, which runs after every test job, always, even on failure.

### What do we check?

Two sources of failures are collected:

1. **Failing tests** from sbt test runs
2. **Log-based failures** from CI checks that do not produce a test report (e.g. `check-logs.sh`)

#### How failures are detected

**Failing tests:** sbt writes one XML file per test class under `<subproject>/target/test-reports/`. CircleCI aggregates these into `test-reports/<subproject>/` via the `upload_test_reports` command. The script walks that tree, parses each file, and extracts the class name of any `<testcase>` that contains a `<failure>` or `<error>` child element.

**Log-based failures:** some CI checks write structured problem lines to `found_problems.txt` rather than emitting a JUnit report. If that file exists, the script picks the single most informative line from it (preferring ERROR over WARN, longer lines over shorter ones) and treats it as an additional failing test name.

### Why do we check that?

A test that fails once might be a flake or a manual retry. The goals of the system are:

- **Visibility**: every failure, transient or not, is counted in Datadog under `canton.failed_test_grouped`. This powers dashboards and lets us spot trends across branches and jobs.
- **Accountability**: for `main` and `main-2.x`, each distinct failing test gets a GitHub issue in [DACH-NY/canton](https://github.com/DACH-NY/canton), labelled under the "Flaky Tests" milestone and tracked on [the flaky test kanban board](https://github.com/orgs/DACH-NY/projects/38/). The issue body accumulates a table of every failure with a link to the specific parallel run, the job name, and the commit. Closed issues are automatically reopened on the next failure; archived issues on the kanban board are unarchived.
- **Alerting**: on `main` and `main-2.x`, a Slack message is sent to `#team-canton-notifications` when a test fails on **3 consecutive commits**. This threshold (defined as `CONSECUTIVE_FAILURES_THRESHOLD`) avoids noise from one-off failures or manual retries while ensuring that a genuinely broken test gets human attention quickly. The message mentions a rotating volunteer chosen deterministically from `select_volunteer.sh`. If the same alert keeps appearing, it means the test is still broken and has not been fixed yet, not that the notification system is misbehaving.

### Issue lifecycle

```
First failure on main/main-2.x
  → gh issue create  (issue opened, row appended, added to kanban)

Subsequent failures
  → gh issue reopen  (if closed)
  → gh issue edit    (new row appended to the table)
  → if 3 consecutive commits all fail: Slack alert sent

OSS (digital-asset/canton) or other branches
  → Datadog metric only, no GitHub issue
```

The "Release Line" custom field on the kanban board is set to `main` or `main-2.x` depending on `CIRCLE_BRANCH`. If a new release line branch is added, append it to `branches_to_report` in the script and run `listReleaseLineFields.graphql` to find the field value ID.

All `gh` API calls are wrapped in `run_gh_with_retries`, which retries up to 3 times with a 5-second delay on transient 5xx errors (502, 503, 504) before giving up.

### Required secrets

| Secret                                       | Used for                                       |
|----------------------------------------------|------------------------------------------------|
| `DATADOG_API_KEY`                            | Posting metrics                                |
| `GITHUB_FLAKY_TEST_TOKEN`                    | GitHub issue create/edit (needs project scope) |
| `SLACK_BOT_QA_NOTIFICATIONS`                 | Posting the Slack alert                        |
| `SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS` | Target channel for the alert                   |

### Running locally

The script runs its own self-tests on startup before doing any real work:

```sh
python3 ./scripts/ci/collect_failing_tests_and_send_to_datadog.py
```

This will print `All self-checks passed` and then fail on missing env vars; that is expected outside CI. To pass a single failure name directly (e.g. for manual testing of the Datadog path):

```sh
DATADOG_API_KEY=... CIRCLE_SHA1=... CIRCLE_BUILD_URL=... CIRCLE_JOB=... CIRCLE_NODE_INDEX=... \
  python3 ./scripts/ci/collect_failing_tests_and_send_to_datadog.py MyFlakyTest
```
