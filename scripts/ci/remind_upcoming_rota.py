#!/usr/bin/env python3
"""DM the people who start a rota shift about a month from now (reusing select_rota's helpers).

Runs weekly. For each rotation it diffs the week LEAD_DAYS (28) ahead against the week before it
and DMs whoever is newly on duty, so a mid-shift person is not re-pinged. A sheet-read failure or
an unresolved name exits non-zero (a total sheet-read failure DMs the roster's fallback_pool so
access gets fixed).

Env: GCLOUD_SHEETS_SA_KEY (Viewer on the sheet), ROTA_SHEET_ID (optional override),
SLACK_BOT_QA_NOTIFICATIONS (bot token with im:write and chat:write).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

import select_rota

SLACK_TOKEN_ENV = select_rota.SLACK_TOKEN_ENV

# About a month, evaluated on a weekly cron so each shift-start Monday is the target exactly once.
LEAD_DAYS = 28

# rotation -> expected sheet column count (None = skip the count check, e.g. flaky-canton pairs).
# The DM label is ROTATION_HEADERS[rotation][0], and self_test asserts this covers every rotation.
REMINDER_COLUMNS: dict[str, int | None] = {
    "l3": 1,
    "l4": 1,
    "ci": 1,
    "release-blackduck": 1,
    "flaky-canton": None,
    "flaky-sdk": 1,
    "flaky-coordination": 1,
    "sdk": 1,
}


def names_for_rotation(
    values: list[list[Any]], rotation: str, monday: dt.date, expected: int | None
) -> list[str]:
    """Names on the given rotation for ``monday``. Raises LookupError on a missing header/row."""
    row, cols = select_rota.find_header_columns(
        values, select_rota.ROTATION_HEADERS[rotation], expected
    )
    return select_rota.names_for_week(values, row, cols, monday)


def newly_starting(target_names: list[str], prior_names: list[str]) -> list[str]:
    """Names on duty in the target week but not the week before it (accent/case-insensitive).

    Duplicates in the target week (e.g. a multi-column rotation listing someone twice) are
    collapsed to the first occurrence, so nobody is DMed more than once."""
    prior = {select_rota.normalize(n) for n in prior_names}
    seen: set[str] = set()
    out: list[str] = []
    for name in target_names:
        key = select_rota.normalize(name)
        if key in prior or key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def build_reminder_message(label: str, date_str: str, sheet_url: str) -> str:
    """Compose the private heads-up DM for someone starting a rota shift in about a month."""
    return (
        f":calendar: Heads up: you're scheduled on the {label} rota for the week of {date_str}, "
        "about a month from now. Please check it works for you and arrange a swap early if it "
        f"does not. Rota sheet: {sheet_url}"
    )


def _report_sheet_failure(
    exc: Exception,
    date_str: str,
    sheet_url: str,
    roster: dict[str, Any],
    token: str | None,
) -> int:
    """Log a total sheet-read failure and DM the usual suspects to fix sheet access. Returns 1."""
    select_rota.log(f"ERROR: could not read the rota sheet ({exc!r})")
    admin_ids = select_rota.resolve_slack_ids(roster["fallback_pool"], roster["slack_id_by_name"])
    message = (
        f":warning: I could not read the rota sheet while preparing upcoming-rota reminders for "
        f"the week of {date_str}. Please check the sheet and the service account's access to it: "
        f"{sheet_url}"
    )
    select_rota.log(
        f"DMing the usual suspects ({', '.join(admin_ids) or 'unset'}) "
        f"about the sheet-read failure:"
    )
    select_rota.log(f"    {message}")
    if token and admin_ids:
        for admin_id in admin_ids:
            try:
                select_rota.send_dm(token, admin_id, message)
                select_rota.log(f"    done: {admin_id}")
            except Exception as dm_exc:
                select_rota.log(f"    ERROR ({admin_id}): {dm_exc!r}")
    else:
        select_rota.log(f"    skipped: {SLACK_TOKEN_ENV} or the fallback pool is not set")
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="DM the people who start a rota shift in about a month."
    )
    parser.add_argument("--date", help="ISO date to evaluate (default today UTC), for testing.")
    parser.add_argument(
        "--target-values-file",
        help="Local JSON of the target week's sheet rows instead of fetching, for testing.",
    )
    parser.add_argument(
        "--prior-values-file",
        help="Local JSON of the prior week's rows (defaults to the target file), for testing.",
    )
    args = parser.parse_args(argv)

    # Read the roster unwrapped: a corrupt roster here should crash loudly, not silently degrade.
    roster = json.loads(select_rota.ROSTER_PATH.read_text(encoding="utf-8"))
    id_by_name = roster["slack_id_by_name"]
    today = (
        dt.date.fromisoformat(args.date) if args.date else dt.datetime.now(dt.timezone.utc).date()
    )
    target_monday = select_rota.current_monday(today) + dt.timedelta(days=LEAD_DAYS)
    prior_monday = target_monday - dt.timedelta(days=7)
    date_str = select_rota.format_week_date(target_monday)
    sheet_id = os.environ.get("ROTA_SHEET_ID", select_rota.DEFAULT_SHEET_ID)
    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    token = os.environ.get(SLACK_TOKEN_ENV)

    # A missing token is a hard config error: fail fast before reading the sheet.
    if not token:
        select_rota.log(f"ERROR: {SLACK_TOKEN_ENV} is not set, cannot send reminders")
        return 1

    # Set when the prior week is unreadable: we still send (over-remind rather than skip a
    # boundary week) but report failure so the degraded run is visible, not green.
    prior_read_failed = False
    if args.target_values_file:
        target_values = json.loads(Path(args.target_values_file).read_text(encoding="utf-8"))
        prior_values = (
            json.loads(Path(args.prior_values_file).read_text(encoding="utf-8"))
            if args.prior_values_file
            else target_values
        )
    else:
        try:
            target_values = select_rota.fetch_week_values(target_monday, sheet_id)
        except Exception as exc:
            return _report_sheet_failure(exc, date_str, sheet_url, roster, token)
        # Target and prior weeks share a half-year tab except at the H1/H2 boundary: reuse if so.
        if select_rota.half_year_tab(prior_monday) == select_rota.half_year_tab(target_monday):
            prior_values = target_values
        else:
            try:
                prior_values = select_rota.fetch_week_values(prior_monday, sheet_id)
            except Exception as exc:
                select_rota.log(
                    f"warning: could not read the prior week ({exc!r}), treating everyone in the "
                    "target week as newly starting and marking the run failed"
                )
                prior_values = []
                prior_read_failed = True

    exit_code = 1 if prior_read_failed else 0
    for rotation, expected in REMINDER_COLUMNS.items():
        label = select_rota.ROTATION_HEADERS[rotation][0]
        try:
            target_names = names_for_rotation(target_values, rotation, target_monday, expected)
        except Exception as exc:
            # A gap in a future week is not actionable yet (nobody to remind), so just note it.
            select_rota.log(f"note: no {label} assignment for the week of {date_str} yet ({exc!r})")
            continue
        try:
            prior_names = names_for_rotation(prior_values, rotation, prior_monday, expected)
        except Exception:
            prior_names = []  # unknown prior week: treat the target-week people as newly starting

        for name in newly_starting(target_names, prior_names):
            ids = select_rota.resolve_slack_ids([name], id_by_name)
            if not ids:
                exit_code = 1  # resolve_slack_ids already logged the missing-name warning
                continue
            select_rota.log(
                f"DMing {name} ({ids[0]}) for the {label} rota starting the week of {date_str}"
            )
            try:
                select_rota.send_dm(
                    token, ids[0], build_reminder_message(label, date_str, sheet_url)
                )
                select_rota.log("    done")
            except Exception as exc:
                select_rota.log(f"    ERROR: {exc!r}")
                exit_code = 1

    return exit_code


def self_test() -> None:
    """In-memory checks of the pure helpers. No network, clock, or roster file."""
    # REMINDER_COLUMNS must not drift out of ROTATION_HEADERS, and must cover every rotation.
    for rotation in REMINDER_COLUMNS:
        assert rotation in select_rota.ROTATION_HEADERS, rotation
    assert set(REMINDER_COLUMNS) == set(select_rota.ROTATION_HEADERS)

    # newly_starting: only the target-week arrivals, accent/case-insensitive, order preserved
    assert newly_starting(["Sören", "Ada"], ["soren"]) == ["Ada"]
    assert newly_starting(["Ada", "Bo"], []) == ["Ada", "Bo"]
    assert newly_starting(["Ada"], ["Ada", "Bo"]) == []
    # a name listed twice in the target week (e.g. a multi-column rotation) is DMed once
    assert newly_starting(["Ada", "ada", "Bo"], []) == ["Ada", "Bo"]

    # build_reminder_message: mentions the rota, the week, and the sheet link
    msg = build_reminder_message("CI", "July 28", "https://sheet")
    assert "CI rota" in msg and "July 28" in msg and "https://sheet" in msg

    monday = dt.date(2025, 6, 30)
    target_serial = (monday + dt.timedelta(days=LEAD_DAYS) - select_rota.SHEETS_EPOCH).days
    prior_serial = (monday + dt.timedelta(days=LEAD_DAYS - 7) - select_rota.SHEETS_EPOCH).days
    header = ["Week", "CI"]
    values = [header, [prior_serial, "Al"], [target_serial, "Bo"]]
    target_monday = monday + dt.timedelta(days=LEAD_DAYS)
    prior_monday = target_monday - dt.timedelta(days=7)
    # names_for_rotation reads the right week, the diff flags the target-week arrival only
    assert names_for_rotation(values, "ci", target_monday, 1) == ["Bo"]
    assert names_for_rotation(values, "ci", prior_monday, 1) == ["Al"]
    assert newly_starting(
        names_for_rotation(values, "ci", target_monday, 1),
        names_for_rotation(values, "ci", prior_monday, 1),
    ) == ["Bo"]

    # main()'s DM and exit-code contract, driven through --values-file so no network is touched
    _self_test_main(monday, header, prior_serial, target_serial)

    print("remind_upcoming_rota self-checks passed")


def _self_test_main(
    monday: dt.date, header: list[str], prior_serial: int, target_serial: int
) -> None:
    """DM and exit-code contract via a temp roster and stubbed send_dm (no network)."""
    import tempfile

    roster = {"slack_id_by_name": {"Al": "UA", "Bo": "UB"}, "fallback_pool": ["Al"]}
    date = monday.isoformat()

    real_send_dm = select_rota.send_dm
    saved_token = os.environ.get(SLACK_TOKEN_ENV)
    original_roster_path = select_rota.ROSTER_PATH
    sent: list[tuple[str, str]] = []

    def fake_send_dm(token: str, user_id: str, text: str) -> None:
        sent.append((user_id, text))

    try:
        select_rota.send_dm = fake_send_dm
        os.environ[SLACK_TOKEN_ENV] = "xoxb-self-test"
        with tempfile.TemporaryDirectory() as tmp:
            roster_file = Path(tmp) / "roster.json"
            roster_file.write_text(json.dumps(roster), encoding="utf-8")
            select_rota.ROSTER_PATH = roster_file

            # Bo starts in the target week (Al was on the prior week): exactly one DM, to Bo.
            newly = [header, [prior_serial, "Al"], [target_serial, "Bo"]]
            newly_file = Path(tmp) / "newly.json"
            newly_file.write_text(json.dumps(newly), encoding="utf-8")
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(newly_file)]) == 0
            assert sent == [("UB", sent[0][1])] and "CI rota" in sent[0][1], sent

            # Al is on both weeks (a continuing shift): no reminder.
            cont = [header, [prior_serial, "Al"], [target_serial, "Al"]]
            cont_file = Path(tmp) / "cont.json"
            cont_file.write_text(json.dumps(cont), encoding="utf-8")
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(cont_file)]) == 0
            assert sent == [], sent

            # An unknown target-week name has no Slack id: nothing sent, exit 1.
            unknown = [header, [prior_serial, "Al"], [target_serial, "Nobody Here"]]
            unknown_file = Path(tmp) / "unknown.json"
            unknown_file.write_text(json.dumps(unknown), encoding="utf-8")
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(unknown_file)]) == 1
            assert sent == [], sent

            # A missing token is a hard config error: fail fast, nothing sent, exit 1.
            os.environ.pop(SLACK_TOKEN_ENV, None)
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(newly_file)]) == 1
            assert sent == [], sent
            os.environ[SLACK_TOKEN_ENV] = "xoxb-self-test"
    finally:
        select_rota.send_dm = real_send_dm
        select_rota.ROSTER_PATH = original_roster_path
        if saved_token is None:
            os.environ.pop(SLACK_TOKEN_ENV, None)
        else:
            os.environ[SLACK_TOKEN_ENV] = saved_token


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
