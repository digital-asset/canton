#!/usr/bin/env python3
"""Posts a Slack summary for flaky-test streaks.

Reads the streaks JSON artifact written by manage_flaky_issues.py and posts a
single summary to the team Slack channel, @-mentioning whoever is on the
relevant rota shift (via select_rota.py): a flaky streak pings the Flaky Canton
rotation (two people), a genuinely-broken nightly streak pings the CI rota.
Posts nothing when there are no streaks. The nightly/flaky split is derived from the CI
job (is_nightly_job).

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

REPO_URL = "https://github.com/DACH-NY/canton"


def select_rota(rotation: str) -> list[str]:
    """Return the Slack user ID(s) on the current rota shift for ``rotation``.

    Delegates to select_rota.py, which reads the rota Google Sheet and falls
    back to a deterministic pick from the roster when the sheet is unreadable,
    so it always prints at least one id and exits 0. Returns [] only if the
    helper itself crashes or emits nothing usable, the alert still posts then,
    just without a mention.
    """
    script = Path(__file__).resolve().parent / 'select_rota.py'
    result = subprocess.run(
        [sys.executable, str(script), '--rotation', rotation],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"select_rota.py failed: {result.stderr.strip()}. No one selected.")
        return []
    ids = [tok for tok in result.stdout.split() if re.match(r'^U[A-Z0-9]+$', tok)]
    if not ids:
        print(f"Unexpected output from select_rota.py: {result.stdout.strip()!r}. No one selected.")
    return ids


def send_duplicate_summary(duplicates: list[tuple[str, str, str]]) -> None:
    """Post a single summary to the team Slack channel when tests fail repeatedly."""
    token = os.environ.get('SLACK_BOT_QA_NOTIFICATIONS', '')
    channel = os.environ.get('SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS', '')
    if not token:
        print("SLACK_BOT_QA_NOTIFICATIONS not set. Skipping Slack channel summary.")
        return
    if not channel:
        print("SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS not set. Skipping Slack channel summary.")
        return
    commit_hash = get_ci_commit_hash()
    commit_link = f"<{REPO_URL}/commit/{commit_hash}|{commit_hash[:8]}>"
    nightly = is_nightly_job(get_ci_job_name())
    # A genuinely-broken nightly is the CI rota's, a flaky streak is the Flaky
    # Canton rotation's (two people share it, so both get pinged).
    responders = select_rota("ci" if nightly else "flaky-canton")
    mention = "".join(f"<@{uid}> " for uid in responders)
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
        issue_url = f"{REPO_URL}/issues/{idx}"
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
    payload = json.dumps(
        {
            "channel": channel,
            "blocks": blocks,
            "icon_emoji": ":rotating_light:",
            "text": alert,  # fallback summary for push notifications and screen readers
        }
    ).encode("utf-8")
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
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            warnings = result.get("response_metadata", {}).get("warnings", [])
            if warnings:
                print(f"Slack response warnings: {warnings}")
            if not result.get("ok"):
                print(f"Slack API error: {result.get('error', 'unknown')}")
            else:
                print(
                    f"Flaky test duplicate summary sent to Slack channel, mentioning {' '.join(responders)}."
                    if responders
                    else "Flaky test duplicate summary sent to Slack channel."
                )
    except Exception as e:
        print(f"Failed to send Slack channel summary: {e}")


def self_test() -> None:
    # select_rota() parsing, with the select_rota.py subprocess faked out.
    # Shared helpers are covered once by flaky_common via the orchestrator.
    def _fake_run(rc, out, err=""):
        return lambda *a, **k: subprocess.CompletedProcess(a[0] if a else [], rc, out, err)

    orig_run = subprocess.run
    try:
        subprocess.run = _fake_run(0, "U123 U456\n")
        assert select_rota("flaky-canton") == ["U123", "U456"]
        subprocess.run = _fake_run(0, "noise U999 more")  # non-id tokens are filtered out
        assert select_rota("ci") == ["U999"]
        subprocess.run = _fake_run(0, "")  # empty output -> no mention
        assert select_rota("ci") == []
        subprocess.run = _fake_run(1, "", "boom")  # non-zero exit -> no mention
        assert select_rota("ci") == []
    finally:
        subprocess.run = orig_run
    print("alert_slack self-checks passed")


def run(streaks_json=None) -> None:
    in_path = streaks_json or streaks_path()
    streaks = read_streaks(in_path)
    if not streaks:
        print("No flaky-test streaks to alert on.")
        return
    send_duplicate_summary(streaks)


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Post a Slack summary for flaky-test streaks.")
    parser.add_argument(
        "--streaks-json",
        default=None,
        help="Input path for the streaks JSON (default: shared CI temp dir).",
    )
    parser.add_argument("--self-test", action="store_true", help="Run self-tests and exit.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.self_test:
        run_guarded_self_test(self_test)
        sys.exit(0)
    run(args.streaks_json)
