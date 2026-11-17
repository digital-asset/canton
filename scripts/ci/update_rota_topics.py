#!/usr/bin/env python3
"""Update the rota Slack channel topics with this week's duty assignments.

Reads the current week's row from the rota Google Sheet (reusing select_rota's helpers)
and rewrites three channel topics with whoever is on duty:

    #team-canton              L3 and L4 support duty
    #team-canton-ci           CI rota and Release & Blackduck rota
    #team-canton-flaky-tests  flaky rota (up to 3 Canton people plus up to 2 SDK people)

The date shown is the current week's Monday, refreshed every week. By default the script
is a dry run (it logs the topics and writes nothing). Pass --apply to call
conversations.setTopic. Any slot that cannot be resolved falls back to a "(nobody on
rota)" placeholder so a topic is always well-formed. A run with any placeholder logs which
rotations failed and exits non-zero, so the workflow's failure alert fires.

Environment: GCLOUD_SHEETS_SA_KEY (Viewer on the sheet), ROTA_SHEET_ID (optional
override), SLACK_BOT_QA_NOTIFICATIONS (bot token, needs channels:manage or groups:write
and channel membership), SLACK_CHANNEL_ID_TEAM_CANTON[_CI|_FLAKY_TESTS] (target channel
ids, apply mode only).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

import select_rota

Resolver = Callable[[str], list[str]]

SLACK_SET_TOPIC_URL = "https://slack.com/api/conversations.setTopic"
SLACK_TOKEN_ENV = select_rota.SLACK_TOKEN_ENV

# Rotations shown in the topics. A sheet column count other than select_rota.expected_columns()
# degrades to a placeholder rather than pinging the wrong set. This order is also the order the
# failure log lists failed rotations in.
TOPIC_ROTATIONS = ("l3", "l4", "ci", "release-blackduck", "flaky-canton", "flaky-sdk")


def _mentions(slack_ids: list[str]) -> str:
    """Join Slack ids as linked mentions, or a placeholder when nobody is on rota."""
    return ", ".join(f"<@{sid}>" for sid in slack_ids) or "(nobody on rota)"


def _duty_topic(resolve: Resolver, date_str: str, segments: tuple[tuple[str, str], ...]) -> str:
    """Middot-join '<label> from <date>: <mentions>' parts (team-canton, team-canton-ci)."""
    return "  ·  ".join(
        f"{label} from {date_str}: {_mentions(resolve(r))}" for label, r in segments
    )


def _flaky_topic(resolve: Resolver, date_str: str) -> str:
    canton = _mentions(resolve("flaky-canton"))
    return f"Rota from {date_str}: {canton} (Canton), {_mentions(resolve('flaky-sdk'))} (SDK)"


# (label, channel-id env var, builder taking (resolve, date_str) -> topic string)
# The builders are lambdas so each topic is composed lazily: the one shared resolver is
# threaded through every channel, letting it accumulate the failed rotations across all of
# them before the single failure log line is written.
CHANNELS = (
    (
        "#team-canton",
        "SLACK_CHANNEL_ID_TEAM_CANTON",
        lambda resolve, d: _duty_topic(
            resolve, d, (("L3 support duty", "l3"), ("L4 support duty", "l4"))
        ),
    ),
    (
        "#team-canton-ci",
        "SLACK_CHANNEL_ID_TEAM_CANTON_CI",
        lambda resolve, d: _duty_topic(
            resolve, d, (("CI rota", "ci"), ("Release & Blackduck rota", "release-blackduck"))
        ),
    ),
    ("#team-canton-flaky-tests", "SLACK_CHANNEL_ID_TEAM_CANTON_FLAKY_TESTS", _flaky_topic),
)


def make_resolver(
    values: list[list[Any]], monday: dt.date, roster: dict[str, Any]
) -> tuple[Resolver, set[str]]:
    """Build a rotation-name -> Slack-ids resolver for the week. Any lookup failure degrades
    to an empty list (logged), and the failed rotation names accumulate in the returned
    ``failures`` set, which fails the run once all channels are built."""
    id_by_name = roster["slack_id_by_name"]
    failures: set[str] = set()

    def resolve(rotation: str) -> list[str]:
        try:
            row, cols = select_rota.find_header_columns(
                values,
                select_rota.ROTATION_HEADERS[rotation],
                select_rota.expected_columns(rotation),
            )
            slack_ids = select_rota.resolve_slack_ids(
                select_rota.names_for_week(values, row, cols, monday), id_by_name
            )
        except Exception as exc:
            # failure-surfaced: adds rotation to failures below, main() then logs it and exits 1
            select_rota.log(f"warning: could not resolve rotation {rotation!r}: {exc!r}")
            slack_ids = []
        if not slack_ids:
            failures.add(rotation)
        return slack_ids

    return resolve, failures


def build_gap_message(failed_rotations: set[str], date_str: str, sheet_url: str) -> str:
    """Log line naming the unresolved rotations, in topic order, and where to fix them."""
    ordered = sorted(failed_rotations, key=TOPIC_ROTATIONS.index)
    labels = ", ".join(select_rota.ROTATION_HEADERS[r][0] for r in ordered)
    return (
        f"could not resolve the rota for the week of {date_str}: {labels}. "
        'The affected Slack topics show a "(nobody on rota)" placeholder. '
        f"Fill in the rota sheet: {sheet_url}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Update the rota Slack channel topics with this week's duty."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually set the topics. Without it the script only logs.",
    )
    parser.add_argument("--date", help="ISO date to evaluate (default today UTC), for testing.")
    parser.add_argument(
        "--values-file", help="Local JSON of sheet rows instead of fetching, for testing."
    )
    args = parser.parse_args(argv)

    # Read the roster unwrapped: unlike select_rota's CI-alert path (which must never break a
    # test run), a corrupt roster here should crash loudly. Nothing is written when it does.
    roster = json.loads(select_rota.ROSTER_PATH.read_text(encoding="utf-8"))
    today = (
        dt.date.fromisoformat(args.date) if args.date else dt.datetime.now(dt.timezone.utc).date()
    )
    monday = select_rota.current_monday(today)
    date_str = select_rota.format_week_date(monday)
    sheet_id = os.environ.get("ROTA_SHEET_ID", select_rota.DEFAULT_SHEET_ID)

    exit_code = 0
    sheet_read_failed = False
    if args.values_file:
        values = json.loads(Path(args.values_file).read_text(encoding="utf-8"))
    else:
        try:
            values = select_rota.fetch_week_values(monday, sheet_id)
        except Exception as exc:
            # failure-surfaced: sets exit_code = 1 below, does not overwrite live topics either
            select_rota.log(f"ERROR: could not read the rota sheet ({exc!r})")
            values, exit_code, sheet_read_failed = [], 1, True

    resolve, failures = make_resolver(values, monday, roster)
    token = os.environ.get(SLACK_TOKEN_ENV)

    for label, channel_env, build in CHANNELS:
        topic = build(resolve, date_str)
        channel_id = os.environ.get(channel_env)
        select_rota.log(
            f"{label} ({channel_env}={channel_id or 'unset'}) "
            f"{'setting' if args.apply else 'would set'} topic:"
        )
        select_rota.log(f"    {topic}")
        if not args.apply:
            continue
        if sheet_read_failed:
            select_rota.log("    skipped: not overwriting topics after a total sheet-read failure")
        elif not channel_id:
            select_rota.log(f"    skipped: {channel_env} is not set")
            exit_code = 1
        elif not token:
            select_rota.log(f"    skipped: {SLACK_TOKEN_ENV} is not set")
            exit_code = 1
        else:
            try:
                select_rota.slack_post(
                    SLACK_SET_TOPIC_URL, token, {"channel": channel_id, "topic": topic}
                )
                select_rota.log("    done")
            except Exception as exc:
                # failure-surfaced: sets exit_code = 1 below
                select_rota.log(f"    ERROR: {exc!r}")
                exit_code = 1

    # Any rotation that resolved to nobody fails the run. The workflow's failure alert is the
    # notification, the log says which rotations and where to fix them. A total sheet-read
    # failure was already logged above.
    if failures:
        exit_code = 1
        if not sheet_read_failed:
            sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
            select_rota.log(f"ERROR: {build_gap_message(failures, date_str, sheet_url)}")

    return exit_code


def self_test() -> None:
    """In-memory checks of the pure helpers. No network, clock, or roster file."""
    assert _mentions([]) == "(nobody on rota)"
    assert _mentions(["U1", "U2"]) == "<@U1>, <@U2>"

    # every topic rotation must have a sheet header, so a new rotation cannot ship without one
    for rotation in TOPIC_ROTATIONS:
        assert rotation in select_rota.ROTATION_HEADERS

    monday = dt.date(2025, 6, 30)
    serial = (monday - select_rota.SHEETS_EPOCH).days
    header = [
        "Week",
        "L3",
        "L4",
        "CI",
        "Release & Blackduck",
        "Flaky Canton",
        "Flaky Canton",
        "Flaky Canton",
        "Flaky SDK",
        "Flaky SDK",
    ]
    roster = {
        "slack_id_by_name": {
            "Al": "UL3",
            "Bo": "UL4",
            "Ci": "UCI",
            "Rb": "URB",
            "Ca": "UC1",
            "Cb": "UC2",
            "Sd": "USDK",
        },
    }

    # a fully populated week: every channel resolves, no failures
    resolve, failures = make_resolver(
        [header, [serial, "Al", "Bo", "Ci", "Rb", "Ca", "Cb", "", "Sd", ""]], monday, roster
    )
    topics = {label: build(resolve, "June 30") for label, _, build in CHANNELS}
    assert failures == set()
    assert topics["#team-canton"] == (
        "L3 support duty from June 30: <@UL3>  ·  L4 support duty from June 30: <@UL4>"
    )
    assert topics["#team-canton-ci"] == (
        "CI rota from June 30: <@UCI>  ·  Release & Blackduck rota from June 30: <@URB>"
    )
    assert topics["#team-canton-flaky-tests"] == (
        "Rota from June 30: <@UC1>, <@UC2> (Canton), <@USDK> (SDK)"
    )

    # a single Canton person is valid (empty slots are skipped), not a failure
    one, one_failures = make_resolver(
        [header, [serial, "Al", "Bo", "Ci", "Rb", "Ca", "", "", "Sd", ""]], monday, roster
    )
    assert one("flaky-canton") == ["UC1"]
    assert _flaky_topic(one, "June 30") == "Rota from June 30: <@UC1> (Canton), <@USDK> (SDK)"
    assert one_failures == set()

    # Flaky SDK has two slots: either may be empty, both filled lists both, and a sheet with
    # a single Flaky SDK column is a lookup failure (placeholder) rather than a partial read
    second_only, second_failures = make_resolver(
        [header, [serial, "Al", "Bo", "Ci", "Rb", "Ca", "Cb", "", "", "Sd"]], monday, roster
    )
    assert second_only("flaky-sdk") == ["USDK"] and second_failures == set()
    two_sdk_roster = {**roster, "slack_id_by_name": {**roster["slack_id_by_name"], "Se": "USDK2"}}
    both, _ = make_resolver(
        [header, [serial, "Al", "Bo", "Ci", "Rb", "Ca", "Cb", "", "Sd", "Se"]],
        monday,
        two_sdk_roster,
    )
    assert _flaky_topic(both, "June 30") == (
        "Rota from June 30: <@UC1>, <@UC2> (Canton), <@USDK>, <@USDK2> (SDK)"
    )
    single, single_failures = make_resolver(
        [header[:-1], [serial, "Al", "Bo", "Ci", "Rb", "Ca", "Cb", "", "Sd"]], monday, roster
    )
    assert single("flaky-sdk") == [] and single_failures == {"flaky-sdk"}

    # missing header/name -> placeholder, and the failed rotations feed the log in order
    broken, broken_failures = make_resolver([["Week", "L4"], [serial, "Bo"]], monday, roster)
    assert broken("l3") == [] and broken("l4") == ["UL4"] and broken("ci") == []
    assert broken_failures == {"l3", "ci"}
    assert build_gap_message(broken_failures, "June 30", "https://sheet") == (
        "could not resolve the rota for the week of June 30: L3, CI. "
        'The affected Slack topics show a "(nobody on rota)" placeholder. '
        "Fill in the rota sheet: https://sheet"
    )

    # empty values (the total sheet-read-failure path): every rotation fails and every topic
    # is all-placeholder, including the flaky Canton slot
    empty, empty_failures = make_resolver([], monday, roster)
    empty_topics = {label: build(empty, "June 30") for label, _, build in CHANNELS}
    assert empty_failures == set(TOPIC_ROTATIONS)
    assert empty_topics["#team-canton-flaky-tests"] == (
        "Rota from June 30: (nobody on rota) (Canton), (nobody on rota) (SDK)"
    )

    # main()'s exit-code contract, driven through --values-file so no network is touched
    _self_test_main_exit_codes(monday, header, serial, roster)

    print("update_rota_topics self-checks passed")


def _self_test_main_exit_codes(
    monday: dt.date, header: list[str], serial: int, roster: dict
) -> None:
    """A full week exits 0, a week with a gap exits 1 (documented placeholder contract). Driven
    through --values-file and a temp roster so no network and no repo files are touched."""
    import tempfile

    full = [header, [serial, "Al", "Bo", "Ci", "Rb", "Ca", "Cb", "", "Sd", ""]]
    gap = [header, [serial, "", "Bo", "Ci", "Rb", "Ca", "Cb", "", "Sd", ""]]
    date = monday.isoformat()
    original_roster_path = select_rota.ROSTER_PATH
    with tempfile.TemporaryDirectory() as tmp:
        roster_file = Path(tmp) / "roster.json"
        roster_file.write_text(json.dumps(roster), encoding="utf-8")
        full_file = Path(tmp) / "full.json"
        full_file.write_text(json.dumps(full), encoding="utf-8")
        gap_file = Path(tmp) / "gap.json"
        gap_file.write_text(json.dumps(gap), encoding="utf-8")
        select_rota.ROSTER_PATH = roster_file
        try:
            assert main(["--date", date, "--values-file", str(full_file)]) == 0
            assert main(["--date", date, "--values-file", str(gap_file)]) == 1
            _self_test_apply_posts_no_message(date, str(gap_file))
        finally:
            select_rota.ROSTER_PATH = original_roster_path


def _self_test_apply_posts_no_message(date: str, gap_file: str) -> None:
    """--apply on a week with a gap sets every topic (gap as a placeholder), posts no chat message
    and exits 1, so the workflow failure alert is the only notification. slack_post is faked."""
    channel_envs = [env for _, env, _ in CHANNELS]
    env_keys = (SLACK_TOKEN_ENV, *channel_envs)
    saved_env = {k: os.environ.get(k) for k in env_keys}
    real_slack_post = select_rota.slack_post
    calls: list[tuple[str, dict[str, str]]] = []

    def fake_slack_post(url: str, token: str, fields: dict[str, str]) -> dict[str, Any]:
        calls.append((url, fields))
        return {"ok": True}

    try:
        select_rota.slack_post = fake_slack_post
        os.environ[SLACK_TOKEN_ENV] = "xoxb-self-test"
        for env in channel_envs:
            os.environ[env] = f"C-{env}"
        assert main(["--apply", "--date", date, "--values-file", gap_file]) == 1
        assert [url for url, _ in calls] == [SLACK_SET_TOPIC_URL] * len(CHANNELS), calls
        assert "(nobody on rota)" in calls[0][1]["topic"], calls[0]
    finally:
        select_rota.slack_post = real_slack_post
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _run_self_test() -> None:
    """Run self_test() and fail if it leaked os.environ, mirroring select_rota."""
    before = dict(os.environ)
    try:
        self_test()
    finally:
        if dict(os.environ) != before:
            raise RuntimeError("self_test() polluted os.environ")


def test_self_test() -> None:
    """Pytest entry point so scripts/ CI exercises the self-checks (otherwise --self-test only)."""
    _run_self_test()


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        _run_self_test()
        raise SystemExit(0)
    raise SystemExit(main())
