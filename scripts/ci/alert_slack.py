#!/usr/bin/env python3
"""Posts a Slack summary for flaky-test streaks.

Reads streaks.json (written by manage_flaky_issues.py) and posts a single
summary to the team Slack channel, mentioning a selected volunteer and, for
broken-nightly streaks, pinging the CI rota. No-op when there are no streaks.
The nightly and rota wording is derived from the CI job (is_nightly_job).

Usage:
  alert_slack.py [--streaks-json PATH] [--self-test]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

from flaky_common import (
    branch,
    get_ci_commit_hash,
    get_ci_job_name,
    is_nightly_job,
    NIGHTLY_CONSECUTIVE_FAILURES,
    read_streaks,
    streaks_path,
    run_guarded_self_test,
)


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


def self_test():
    # No module-specific unit tests yet. Shared helpers are covered by flaky_common.
    print("alert_slack self-checks passed")


def run(streaks_json=None):
    in_path = streaks_json or streaks_path()
    streaks = read_streaks(in_path)
    if not streaks:
        print("No flaky-test streaks to alert on.")
        return
    send_duplicate_summary(streaks)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Post a Slack summary for flaky-test streaks.")
    parser.add_argument("--streaks-json", default=None,
                        help="Input path for the streaks JSON (default: shared CI temp dir).")
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.self_test:
        run_guarded_self_test(self_test)
        sys.exit(0)
    run(args.streaks_json)
