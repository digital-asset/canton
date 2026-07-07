# CI scripts

## Flaky test notification system

The flaky-test notification system tracks test failures across CI runs. For every failing test it posts a metric to Datadog, maintains a GitHub issue on `main` and `main-2.x`, and sends a Slack alert when the same test fails on several consecutive commits.

It is split into four focused scripts that pass data via JSON artifacts in a shared directory (`FLAKY_ARTIFACT_DIR`), run in sequence by the `report_failing_tests.py` orchestrator:

| Script | Concern |
|---|---|
| `parse_failing_tests.py` | collect failing tests from `test-reports/` (+ `found_problems.txt`) → `failing_tests.json` |
| `report_to_datadog.py` | post a metric per failing test (all branches) |
| `manage_flaky_issues.py` | create/update the GitHub issue on tracked branches → `streaks.json` |
| `alert_slack.py` | post the Slack summary for any streaks |

Shared code (CI-env detection, the `gh` wrapper, formatting, project/field config, the JSON contracts) lives in `flaky_common.py`. Each script also runs its own inline self-tests on startup and supports `--self-test` to run them and exit.

It is invoked via the `collect_failing_test_data_and_send_to_datadog` CircleCI command (and the GitHub Actions composite action of the same name), which runs `report_failing_tests.py` after every test job, always, even on failure.

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
- **Alerting**: on `main` and `main-2.x`, a Slack message is sent to `#team-canton-notifications` when a test fails on **3 distinct consecutive commits** (`CONSECUTIVE_FAILURES_THRESHOLD`); multi-shard failures or manual retries on the same commit collapse to one entry for this count. The threshold avoids noise from one-off failures or manual retries while ensuring that a genuinely broken test gets human attention quickly. The message mentions a rotating volunteer chosen deterministically from `select_volunteer.sh`. If the same alert keeps appearing, it means the test is still broken and has not been fixed yet, not that the notification system is misbehaving. The alert is suppressed if the issue already has an assignee, to avoid Slack noise on issues that are actively being worked on.

### Issue lifecycle

```
First failure on main/main-2.x
  → gh issue create  (issue opened, row appended, added to kanban)

Subsequent failures
  → gh issue reopen  (if closed)
  → gh issue edit    (new row appended to the table)
  → if 3 consecutive commits all fail AND issue has no assignee: Slack alert sent

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

Each step runs its own self-tests on startup; run just the self-tests (no real work, no env needed) with `--self-test`. To run them for the whole pipeline:

```sh
python3 ./scripts/ci/report_failing_tests.py --self-test
```

To run the full pipeline (parse → datadog → issues → slack), reading `./test-reports`:

```sh
python3 ./scripts/ci/report_failing_tests.py
```

To pass a single failure name directly (e.g. the sbt-crash path, or manual testing of the Datadog path):

```sh
DATADOG_API_KEY=... CIRCLE_SHA1=... CIRCLE_BUILD_URL=... CIRCLE_JOB=... CIRCLE_NODE_INDEX=... \
  python3 ./scripts/ci/report_failing_tests.py MyFlakyTest
```

Individual steps can also be run on their own; they exchange data via JSON files under `FLAKY_ARTIFACT_DIR` (default: a CI temp dir), overridable with `--failing-tests-json` / `--streaks-json`.
