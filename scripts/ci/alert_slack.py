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
from unittest.mock import patch

import select_rota as select_rota_lib
from flaky_common import (
    branch,
    get_ci_commit_hash,
    get_ci_job_name,
    is_circle_ci,
    is_github_actions_ci,
    is_nightly_job,
    NIGHTLY_CONSECUTIVE_FAILURES,
    read_streaks,
    streaks_path,
    run_guarded_self_test,
)

REPO_URL = "https://github.com/DACH-NY/canton"


def _redact(text: str) -> str:
    """Strip known secret env values from helper output before it reaches the log.

    select_rota.py's diagnostics are audited to carry only failure reasons, never
    credentials, but this is defense in depth so echoing its stderr stays safe if
    that helper ever changes.
    """
    for var in ('GCLOUD_SHEETS_SA_KEY', 'SLACK_BOT_QA_NOTIFICATIONS'):
        secret = os.environ.get(var)
        if secret:
            text = text.replace(secret, '***')
    return text


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
        print(f"select_rota.py failed: {_redact(result.stderr.strip())}. No one selected.")
        return []
    # select_rota.py exits 0 even when it degrades to the fallback pool, writing the
    # reason (sheet auth/lookup failure) to stderr. Surface it so a fallback ping is
    # explained in the log instead of looking like a normal rota pick.
    if result.stderr.strip():
        print(f"select_rota.py diagnostics: {_redact(result.stderr.strip())}")
    ids = [tok for tok in result.stdout.split() if re.match(r'^U[A-Z0-9]+$', tok)]
    if not ids:
        print(f"Unexpected output from select_rota.py: {result.stdout.strip()!r}. No one selected.")
    return ids


def _notify_alert_post_failure(token: str, reason: str) -> None:
    """Best-effort: page the CI rota when the alert itself fails to post.

    Otherwise a broken Slack integration silently drops every flaky/red-main alert
    with nothing but a CI log line nobody reads. Uses the same bot token as the failed
    post, so this can't help if the token itself is what's broken (e.g. revoked), still
    strictly better than nothing for every other failure mode (bad channel id, network
    blip, oversized payload, ...).
    """
    try:
        roster = json.loads(select_rota_lib.ROSTER_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Could not load roster to report the post failure either: {exc!r}")
        return
    select_rota_lib.notify_tooling_failure(
        f":warning: alert_slack.py failed to post the flaky/red-main Slack alert on "
        f"`{branch}`: {reason}. The underlying test failure(s) may be going unreported, "
        "can someone check the Slack app/credentials/channel?",
        roster,
        token,
    )


def send_duplicate_summary(duplicates: list[tuple[str, str, str]]) -> None:
    """Post a single summary to the team Slack channel when tests fail repeatedly."""
    token = os.environ.get('SLACK_BOT_QA_NOTIFICATIONS', '')
    channel = os.environ.get('SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS', '')
    # We only get here with streaks worth alerting on, and streaks are written
    # only on tracked branches (main / main-2.x, see should_report_issues), never
    # on PRs or forks. So missing creds here means a branch that must alert has
    # not been wired up. Fail loudly instead of swallowing the alert, which is the
    # silent no-op that left main un-alerted. This also self-enforces wiring: a new
    # test job that forgets the creds goes red on its first real streak.
    missing = [
        name
        for name, value in (
            ('SLACK_BOT_QA_NOTIFICATIONS', token),
            ('SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS', channel),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"{' and '.join(missing)} not set, but {len(duplicates)} streak(s) need a Slack alert. "
            "Wire the credential(s) into this workflow's job-level env (see the other _*_test.yml)."
        )
    commit_hash = get_ci_commit_hash()
    commit_link = f"<{REPO_URL}/commit/{commit_hash}|{commit_hash[:8]}>"
    nightly = is_nightly_job(get_ci_job_name())
    # A genuinely-broken nightly is the CI rota's, a flaky streak is the Flaky
    # Canton rotation's (two people share it, so both get pinged).
    responders = select_rota("ci" if nightly else "flaky-canton")
    mention = "".join(f"<@{uid}> " for uid in responders)
    # Label the alert with the CI that produced it so the reader can tell GHA from
    # CircleCI at a glance (they route the rota mention differently). Matches the
    # `*[CircleCI]*` convention in .circleci/config/commands/@slack.yml.
    source = "GHA" if is_github_actions_ci() else "CircleCI" if is_circle_ci() else "CI"
    if nightly:
        alert = f"*[{source}]* Broken nightly test alert on {branch}"
        description = (
            f"The following test(s) failed on the last {NIGHTLY_CONSECUTIVE_FAILURES} nightly runs "
            f"on `{branch}` (latest at {commit_link}). Likely genuinely broken, not flaky."
        )
        prompt = "Can you have a look?"
    else:
        alert = f"*[{source}]* Flaky test alert on {branch}"
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
                error = result.get("error", "unknown")
                print(f"Slack API error: {error}")
                _notify_alert_post_failure(token, f"Slack API error: {error}")
            else:
                print(
                    f"Flaky test duplicate summary sent to Slack channel, mentioning {' '.join(responders)}."
                    if responders
                    else "Flaky test duplicate summary sent to Slack channel."
                )
    except Exception as e:
        print(f"Failed to send Slack channel summary: {e}")
        _notify_alert_post_failure(token, repr(e))


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

    # _notify_alert_post_failure: pages the CI rota via select_rota_lib.notify_tooling_failure
    # whenever the alert itself fails to post. Fake that helper out (network-free) to
    # check it's called with the right token/message, and that a broken roster read
    # doesn't raise either.
    real_notify = select_rota_lib.notify_tooling_failure
    calls: list[tuple[str, str]] = []
    try:
        select_rota_lib.notify_tooling_failure = lambda message, roster, token, **kw: calls.append(
            (message, token)
        )
        _notify_alert_post_failure("xoxb-test", "boom")
        assert len(calls) == 1
        assert calls[0][1] == "xoxb-test"
        assert "boom" in calls[0][0]

        real_roster_path = select_rota_lib.ROSTER_PATH
        select_rota_lib.ROSTER_PATH = Path("/nonexistent/roster.json")
        try:
            _notify_alert_post_failure("xoxb-test", "boom")  # must not raise
        finally:
            select_rota_lib.ROSTER_PATH = real_roster_path
        assert len(calls) == 1  # unreachable roster -> no additional (broken) call
    finally:
        select_rota_lib.notify_tooling_failure = real_notify

    # send_duplicate_summary itself (not just the helper above in isolation) must call
    # _notify_alert_post_failure on both Slack failure shapes: a non-"ok" API response
    # and the post request raising. Stub the network and select_rota() so this runs
    # without touching Slack or subprocess.
    class _FakeResp:
        def __init__(self, body: bytes):
            self._body = body

        def read(self):
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    saved_env = {
        k: os.environ.get(k)
        for k in ("SLACK_BOT_QA_NOTIFICATIONS", "SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS")
    }
    post_failure_calls: list[tuple[str, str]] = []
    try:
        os.environ["SLACK_BOT_QA_NOTIFICATIONS"] = "xoxb-test"
        os.environ["SLACK_CHANNEL_ID_TEAM_CANTON_NOTIFICATIONS"] = "C123"
        duplicates = [("123", "Some Test", "cause")]

        with (
            patch(f"{__name__}.select_rota", return_value=[]),
            patch(
                f"{__name__}._notify_alert_post_failure",
                side_effect=lambda t, r: post_failure_calls.append((t, r)),
            ),
        ):
            with patch(
                "urllib.request.urlopen",
                return_value=_FakeResp(
                    json.dumps({"ok": False, "error": "channel_not_found"}).encode()
                ),
            ):
                send_duplicate_summary(duplicates)
            assert len(post_failure_calls) == 1, post_failure_calls
            assert post_failure_calls[0][0] == "xoxb-test"
            assert "channel_not_found" in post_failure_calls[0][1]
            post_failure_calls.clear()

            with patch("urllib.request.urlopen", side_effect=OSError("network down")):
                send_duplicate_summary(duplicates)
            assert len(post_failure_calls) == 1, post_failure_calls
            assert post_failure_calls[0][0] == "xoxb-test"
            assert "network down" in post_failure_calls[0][1]
    finally:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    print("alert_slack self-checks passed")


def test_self_test() -> None:
    """Pytest entry point so lint_and_test_py_scripts.yml's plain `pytest` run exercises
    these self-checks too, not just `--self-test` (which nothing else invokes in CI)."""
    run_guarded_self_test(self_test)


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
