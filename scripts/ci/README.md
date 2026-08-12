# CI scripts

## TLDR

- **Who gets pinged, and why.** Two alerts @-mention people, both resolving the on-duty name from the rota Google sheet via `select_rota.py` and matching a Slack ID in `roster_people.json`:
  - **Flaky/nightly alert** (`alert_slack.py`), when the same test fails on several consecutive commits on a tracked branch:
    - a **flaky streak** pings the **Flaky Canton** rotation (two people),
    - a **broken nightly** pings the **CI rota**.
  - **Red-main alert** (`slack_red_main_with_volunteer`), when a job fails on `main`: it pings the rota mapped to that job, so a red **blackduck** job pings the **Release & Blackduck** rota and everything else pings the **CI rota**.
- **Fallback when the lookup fails.** If the sheet cannot be read (`GCLOUD_SHEETS_SA_KEY` unset, the service account is not a Viewer), nobody is on rota that week, or the name has no Slack ID in `roster_people.json`, the alert @-mentions `fallback_pool` from `roster_people.json` instead of pinging no one.
- **Weekly Slack topics.** A separate scheduled workflow, `update_rota_topics.yml`, runs every Monday and rewrites the team channel topics from the same rota sheet, so each channel topic always names the current on-duty person.

## Flaky test notification system

The flaky-test notification system tracks test failures across CI runs.
For every failing test it posts a metric to Datadog, maintains a GitHub issue on `main` and `main-2.x`, and sends a Slack alert when the same test fails on several consecutive commits.

It is split into four focused scripts that pass data via JSON artifacts in a shared directory (`FLAKY_ARTIFACT_DIR`), run in sequence by the `report_failing_tests.py` orchestrator:

| Script | Concern |
|---|---|
| `parse_failing_tests.py` | collect failing tests from `test-reports/` (+ `found_problems.txt`) → `failing_tests.json` |
| `report_to_datadog.py` | post a metric per failing test (all branches) |
| `manage_flaky_issues.py` | create/update the GitHub issue on tracked branches → `streaks.json` |
| `alert_slack.py` | post the Slack summary for any streaks |

Shared code (CI-env detection, the `gh` wrapper, formatting, project/field config, the JSON contracts) lives in `flaky_common.py`.
Each script carries its own inline self-tests, runnable with `--self-test`, which runs them and exits.

It is invoked via the `collect_failing_test_data_and_send_to_datadog` CircleCI command (and the GitHub Actions composite action of the same name), which runs `report_failing_tests.py` after every test job, always, even on failure.

### What do we check?

Two sources of failures are collected:

1. **Failing tests** from sbt test runs
2. **Log-based failures** from CI checks that do not produce a test report (e.g. `check-logs.sh`)

#### How failures are detected

**Failing tests:** sbt writes one XML file per test class under `<subproject>/target/test-reports/`.
CircleCI aggregates these into `test-reports/<subproject>/` via the `upload_test_reports` command.
The script walks that tree, parses each file, and extracts the class name of any `<testcase>` that contains a `<failure>` or `<error>` child element.

**Log-based failures:** some CI checks write structured problem lines to `found_problems.txt` rather than emitting a JUnit report.
If that file exists, the script picks the single most informative line from it (preferring ERROR over WARN, longer lines over shorter ones) and treats it as an additional failing test name.

### Why do we check that?

A test that fails once might be a flake or a manual retry.
The goals of the system are:

- **Visibility**: every failure, transient or not, is counted in Datadog under `canton.failed_test_grouped`.
  This powers dashboards and lets us spot trends across branches and jobs.
- **Accountability**: for `main` and `main-2.x`, each distinct failing test gets a GitHub issue in [DACH-NY/canton](https://github.com/DACH-NY/canton), labelled under the "Flaky Tests" milestone and tracked on [the flaky test kanban board](https://github.com/orgs/DACH-NY/projects/38/).
  The issue body accumulates a table of every failure with a link to the specific parallel run, the job name, and the commit.
  Closed issues are automatically reopened on the next failure, and archived issues on the kanban board are unarchived.
- **Alerting**: on `main` and `main-2.x`, a Slack message is sent to `#team-canton-notifications` when a test fails on **3 distinct consecutive commits** (`CONSECUTIVE_FAILURES_THRESHOLD`).
  Multi-shard failures or manual retries on the same commit collapse to one entry for this count.
  The threshold avoids noise from one-off failures or manual retries while ensuring that a genuinely broken test gets human attention quickly.
  The message @-mentions whoever is on the relevant rota shift that week, read from the [rota Google Sheet](https://docs.google.com/spreadsheets/d/1PEmLKqoB2DpokVhao5PNxznMI5ufZZbXgUju7Npn0BU) via `select_rota.py` (falling back to the roster pool in `roster_people.json` if the sheet is unreadable).
  If the same alert keeps appearing, it means the test is still broken and has not been fixed yet, not that the notification system is misbehaving.
  The alert is suppressed if the issue already has an assignee, to avoid Slack noise on issues that are actively being worked on.

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

The "Release Line" custom field on the kanban board is set to `main` or `main-2.x` depending on `CIRCLE_BRANCH`.
If a new release line branch is added, append it to `branches_to_report` in the script and run `listReleaseLineFields.graphql` to find the field value ID.

All `gh` API calls are wrapped in `run_gh_with_retries`, which retries up to 3 times with a 5-second delay on transient 5xx errors (502, 503, 504) before giving up.

### Required secrets

| Secret                                       | Used for                                       |
|----------------------------------------------|------------------------------------------------|
| `DATADOG_API_KEY`                            | Posting metrics                                |
| `GITHUB_FLAKY_TEST_TOKEN`                    | GitHub issue create/edit (needs project scope) |
| `SLACK_BOT_QA_NOTIFICATIONS`                 | Posting the Slack alert                        |
| `SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS` | Target channel for the alert                   |
| `GCLOUD_SHEETS_SA_KEY`                       | Reading the rota sheet to @-mention the person on duty in the alert (the service account must be a Viewer on the sheet). If unset or unreadable, the alert falls back to `fallback_pool` in `roster_people.json` |

The rota lookup also honors an optional `ROTA_SHEET_ID` variable to point at a different sheet, defaulting to the roster sheet baked into `select_rota.py`.

## Rota Slack channel topics

`update_rota_topics.py` keeps three channel topics in sync with the people on duty this week, reading the same [rota Google Sheet](https://docs.google.com/spreadsheets/d/1PEmLKqoB2DpokVhao5PNxznMI5ufZZbXgUju7Npn0BU) and roster (`roster_people.json`) as `select_rota.py`:

| Channel | Rotations shown |
|---|---|
| `#team-canton` | L3 and L4 support duty |
| `#team-canton-ci` | CI rota and Release & Blackduck rota |
| `#team-canton-flaky-tests` | flaky rota (1 or 2 Canton people plus 1 SDK person) |

The date in each topic is the current week's Monday. It is refreshed every week even when the person on duty is unchanged, so the topic always reflects the running week.

By default the script is a **dry run**: it logs the topics it would set and writes nothing. Pass `--apply` to call `conversations.setTopic`. Any slot that cannot be resolved (unreadable sheet, missing header, unknown name, absent week) falls back to a `(nobody on rota)` placeholder, so a topic is always composed and set rather than skipped, and never renders as a broken mention. A run with any placeholder still exits non-zero so the degraded state is noticed.

Whenever a rotation cannot be resolved, the script also direct-messages the usual suspects (the roster's `fallback_pool`, override with the `ROTA_TOPICS_ADMIN_SLACK_ID` variable) asking them to fix the rota sheet. Like the topic writes, the DMs are only sent in apply mode.

It runs weekly via the `Update rota Slack topics` GitHub Actions workflow (`.github/workflows/update_rota_topics.yml`), scheduled every Monday at 04:00 UTC (06:00 CEST). The scheduled run writes topics for real. A manual dispatch defaults to a dry run so you can preview, tick the `apply` box to write from a manual run too.

### Running it manually

To trigger a run off-schedule, for example to apply topics right after fixing the sheet or to preview a dry run:

- **From the GitHub UI:** open the [Update rota Slack topics workflow](https://github.com/DACH-NY/canton/actions/workflows/update_rota_topics.yml), click **Run workflow**, pick the `main` branch, tick **Actually set the Slack topics** for a real write or leave it unchecked for a dry run, then click **Run workflow**.
- **From the `gh` CLI:**

  ```sh
  # dry run (logs only, writes nothing)
  gh workflow run "Update rota Slack topics" --ref main

  # actually set the topics
  gh workflow run "Update rota Slack topics" --ref main -f apply=true
  ```

`workflow_dispatch` runs the workflow definition from the branch you pick, so use `main` unless you are deliberately testing a change to the workflow on another branch.

### Required secrets and variables

| Name | Kind | Used for |
|---|---|---|
| `GCLOUD_SHEETS_SA_KEY` | secret | Reading the rota sheet (SA must be a Viewer on it) |
| `SLACK_BOT_QA_NOTIFICATIONS` | secret | Setting the topics and sending the fix-me DM (bot needs `channels:manage` for public channels or `groups:write` for private ones plus `chat:write`, and must be a member of each channel) |
| `SLACK_CHANNEL_ID_TEAM_CANTON` | variable | Target channel id |
| `SLACK_CHANNEL_ID_TEAM_CANTON_CI` | variable | Target channel id |
| `SLACK_CHANNEL_ID_TEAM_CANTON_FLAKY_TESTS` | variable | Target channel id |
| `ROTA_TOPICS_ADMIN_SLACK_ID` | variable | Optional comma-separated Slack ids to DM when a rotation cannot be resolved (defaults to the roster's `fallback_pool`, the usual suspects) |
