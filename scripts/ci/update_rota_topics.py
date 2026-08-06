#!/usr/bin/env python3
"""Update the rota Slack channel topics with this week's duty assignments.

Reads the current week's row from the rota Google Sheet (reusing select_rota's helpers)
and rewrites three channel topics with whoever is on duty:

    #team-canton              L3 and L4 support duty
    #team-canton-ci           CI rota and Release & Blackduck rota
    #team-canton-flaky-tests  flaky rota (1 or 2 Canton people plus 1 SDK person)

The date shown is the current week's Monday, refreshed every week. By default the script
is a dry run (it logs the topics and writes nothing). Pass --apply to call
conversations.setTopic. Any slot that cannot be resolved falls back to a "(nobody on
rota)" placeholder so a topic is always well-formed, and the usual suspects (the roster's
fallback_pool, override with ROTA_TOPICS_ADMIN_SLACK_ID) are each DMed to fix the sheet.
A run with any placeholder exits non-zero.

Environment: GCLOUD_SHEETS_SA_KEY (Viewer on the sheet), ROTA_SHEET_ID (optional
override), SLACK_BOT_QA_NOTIFICATIONS (bot token, needs channels:manage or groups:write
plus chat:write and channel membership), SLACK_CHANNEL_ID_TEAM_CANTON[_CI|_FLAKY_TESTS]
(target channel ids, apply mode only), ROTA_TOPICS_ADMIN_SLACK_ID (optional comma-separated
DM targets that replace the fallback_pool).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

import select_rota

Resolver = Callable[[str], list[str]]

SLACK_SET_TOPIC_URL = "https://slack.com/api/conversations.setTopic"
SLACK_OPEN_DM_URL = "https://slack.com/api/conversations.open"
SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
SLACK_TOKEN_ENV = "SLACK_BOT_QA_NOTIFICATIONS"
FIX_ME_SLACK_ID_ENV = "ROTA_TOPICS_ADMIN_SLACK_ID"

# rotation name -> expected sheet column count. None means "1 or 2 both fine" (flaky-canton
# is sometimes a pair). A different count degrades to a placeholder rather than pinging the
# wrong set. This order is also the order the fix-me DM lists failed rotations in.
EXPECTED_COLUMNS: dict[str, int | None] = {
    "l3": 1,
    "l4": 1,
    "ci": 1,
    "release-blackduck": 1,
    "flaky-canton": None,
    "flaky-sdk": 1,
}


def format_week_date(monday: dt.date) -> str:
    """Render the week's Monday like 'July 27' (full month, no leading zero)."""
    return f"{monday.strftime('%B')} {monday.day}"


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
# them before the single fix-me DM is sent.
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
    ``failures`` set, which drives the fix-me DM once all channels are built."""
    id_by_name = roster["slack_id_by_name"]
    failures: set[str] = set()

    def resolve(rotation: str) -> list[str]:
        try:
            row, cols = select_rota.find_header_columns(
                values, select_rota.ROTATION_HEADERS[rotation], EXPECTED_COLUMNS[rotation]
            )
            slack_ids = select_rota.resolve_slack_ids(
                select_rota.names_for_week(values, row, cols, monday), id_by_name
            )
        except Exception as exc:
            select_rota.log(f"warning: could not resolve rotation {rotation!r}: {exc!r}")
            slack_ids = []
        if not slack_ids:
            failures.add(rotation)
        return slack_ids

    return resolve, failures


def fetch_week_values(monday: dt.date, sheet_id: str) -> list[list[Any]]:
    """Fetch the half-year tab holding the given week, reusing select_rota's helpers."""
    sa_key = os.environ.get("GCLOUD_SHEETS_SA_KEY")
    if not sa_key:
        raise RuntimeError("GCLOUD_SHEETS_SA_KEY is not set")
    token = select_rota.get_access_token(json.loads(sa_key))
    return select_rota.fetch_tab_values(token, sheet_id, select_rota.half_year_tab(monday))


def _slack_post(url: str, token: str, fields: dict) -> dict:
    """POST form-encoded fields to a Slack Web API method, returning the parsed ok response."""
    req = urllib.request.Request(
        url,
        data=urllib.parse.urlencode(fields).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.load(resp)
    except urllib.error.HTTPError as err:
        body = err.read().decode(errors="replace")
        raise RuntimeError(f"{url} failed: HTTP {err.code} {body}") from err
    if not payload.get("ok"):
        raise RuntimeError(f"{url} failed: {payload.get('error')}")
    return payload


def build_fix_me_message(
    failed_rotations: set[str], date_str: str, sheet_url: str, sheet_read_failed: bool = False
) -> str:
    """Compose the DM asking a maintainer to fix the rota sheet.

    A total sheet-read failure and per-rotation gaps need different asks: on a read failure
    the topics are left untouched (not placeholdered), so the message points at sheet access
    rather than a missing name.
    """
    if sheet_read_failed:
        return (
            f":warning: I could not read the rota sheet for the week of {date_str}, so the "
            "channel topics were left unchanged. Please check the sheet and the service "
            f"account's access to it: {sheet_url}"
        )
    ordered = sorted(failed_rotations, key=list(EXPECTED_COLUMNS).index)
    labels = ", ".join(select_rota.ROTATION_HEADERS[r][0] for r in ordered)
    return (
        f":warning: I could not resolve the rota for the week of {date_str}: {labels}. "
        'The affected Slack topics show a "(nobody on rota)" placeholder. '
        f"Please fill in the rota sheet: {sheet_url}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Update the rota Slack channel topics with this week's duty."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually set the topics and send the DM. Without it the script only logs.",
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
    date_str = format_week_date(monday)
    sheet_id = os.environ.get("ROTA_SHEET_ID", select_rota.DEFAULT_SHEET_ID)

    exit_code = 0
    sheet_read_failed = False
    if args.values_file:
        values = json.loads(Path(args.values_file).read_text(encoding="utf-8"))
    else:
        try:
            values = fetch_week_values(monday, sheet_id)
        except Exception as exc:
            # A per-slot gap degrades to a placeholder and still gets written, but a whole-sheet
            # read failure is a transient outage, not a rota gap. We compose the (all-placeholder)
            # topics for the dry-run log, flag the run, and DM the maintainer, but do NOT overwrite
            # the live topics, so a Sheets blip cannot wipe a good week's topics on all channels.
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
                _slack_post(SLACK_SET_TOPIC_URL, token, {"channel": channel_id, "topic": topic})
                select_rota.log("    done")
            except Exception as exc:
                select_rota.log(f"    ERROR: {exc!r}")
                exit_code = 1

    # If any rotation resolved to nobody, DM the usual suspects so the sheet gets fixed.
    if failures:
        exit_code = 1
        admin_override = os.environ.get(FIX_ME_SLACK_ID_ENV)
        if admin_override:
            admin_ids = [i.strip() for i in admin_override.split(",") if i.strip()]
            admin_label = f"{FIX_ME_SLACK_ID_ENV} override"
        else:
            admin_ids = select_rota.resolve_slack_ids(
                roster["fallback_pool"], roster["slack_id_by_name"]
            )
            admin_label = "the usual suspects (" + ", ".join(roster["fallback_pool"]) + ")"
        message = build_fix_me_message(
            failures,
            date_str,
            f"https://docs.google.com/spreadsheets/d/{sheet_id}",
            sheet_read_failed,
        )
        select_rota.log(
            f"{'DMing' if args.apply else 'would DM'} {admin_label} "
            f"({', '.join(admin_ids) or 'unset'}) to fix the rota:"
        )
        select_rota.log(f"    {message}")
        if args.apply and token and admin_ids:
            # Open a 1:1 DM per maintainer and post to that conversation id: chat.postMessage
            # needs a conversation id, not a user id. conversations.open is covered by the
            # channels:manage/groups:write scope the topic writes already need, so no extra one.
            for admin_id in admin_ids:
                try:
                    dm = _slack_post(SLACK_OPEN_DM_URL, token, {"users": admin_id})
                    _slack_post(
                        SLACK_POST_MESSAGE_URL,
                        token,
                        {"channel": dm["channel"]["id"], "text": message},
                    )
                    select_rota.log(f"    done: {admin_id}")
                except Exception as exc:
                    select_rota.log(f"    ERROR ({admin_id}): {exc!r}")
        elif args.apply:
            select_rota.log(f"    skipped: {SLACK_TOKEN_ENV} or the admin ids are not set")

    return exit_code


def self_test() -> None:
    """In-memory checks of the pure helpers. No network, clock, or roster file."""
    assert format_week_date(dt.date(2025, 7, 27)) == "July 27"
    assert format_week_date(dt.date(2025, 1, 5)) == "January 5"
    assert _mentions([]) == "(nobody on rota)"
    assert _mentions(["U1", "U2"]) == "<@U1>, <@U2>"

    # every rotation the channels build must be a known column and a known header, so a future
    # channel cannot drift out of EXPECTED_COLUMNS or ROTATION_HEADERS unnoticed
    for rotation in EXPECTED_COLUMNS:
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
        "fallback_pool": ["Al", "Bo", "Ci"],
    }

    # a resolution gap DMs the whole fallback_pool (the usual suspects), not a single person
    assert select_rota.resolve_slack_ids(roster["fallback_pool"], roster["slack_id_by_name"]) == [
        "UL3",
        "UL4",
        "UCI",
    ]

    # a fully populated week: every channel resolves, no failures
    resolve, failures = make_resolver(
        [header, [serial, "Al", "Bo", "Ci", "Rb", "Ca", "Cb", "Sd"]], monday, roster
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

    # a single Canton person is valid (1 or 2), not a failure
    one, one_failures = make_resolver(
        [header, [serial, "Al", "Bo", "Ci", "Rb", "Ca", "", "Sd"]], monday, roster
    )
    assert one("flaky-canton") == ["UC1"]
    assert _flaky_topic(one, "June 30") == "Rota from June 30: <@UC1> (Canton), <@USDK> (SDK)"
    assert one_failures == set()

    # missing header/name -> placeholder, and the failed rotations feed the DM in order
    broken, broken_failures = make_resolver([["Week", "L4"], [serial, "Bo"]], monday, roster)
    assert broken("l3") == [] and broken("l4") == ["UL4"] and broken("ci") == []
    assert broken_failures == {"l3", "ci"}
    assert build_fix_me_message(broken_failures, "June 30", "https://sheet") == (
        ":warning: I could not resolve the rota for the week of June 30: L3, CI. "
        'The affected Slack topics show a "(nobody on rota)" placeholder. '
        "Please fill in the rota sheet: https://sheet"
    )
    # a total sheet-read failure asks about sheet access, not a missing name, since the topics
    # are left unchanged rather than placeholdered
    assert build_fix_me_message(broken_failures, "June 30", "https://sheet", True) == (
        ":warning: I could not read the rota sheet for the week of June 30, so the channel "
        "topics were left unchanged. Please check the sheet and the service account's access "
        "to it: https://sheet"
    )

    # empty values (the total sheet-read-failure path): every rotation fails and every topic
    # is all-placeholder, including the flaky Canton slot
    empty, empty_failures = make_resolver([], monday, roster)
    empty_topics = {label: build(empty, "June 30") for label, _, build in CHANNELS}
    assert empty_failures == set(EXPECTED_COLUMNS)
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

    full = [header, [serial, "Al", "Bo", "Ci", "Rb", "Ca", "Cb", "Sd"]]
    gap = [header, [serial, "", "Bo", "Ci", "Rb", "Ca", "Cb", "Sd"]]
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
        finally:
            select_rota.ROSTER_PATH = original_roster_path


def _run_self_test() -> None:
    """Run self_test() and fail if it leaked os.environ, mirroring select_rota."""
    before = dict(os.environ)
    try:
        self_test()
    finally:
        if dict(os.environ) != before:
            raise RuntimeError("self_test() polluted os.environ")


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        _run_self_test()
        raise SystemExit(0)
    raise SystemExit(main())
