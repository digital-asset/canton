#!/usr/bin/env python3
"""DM the people who start a rota shift about a month from now (reusing select_rota's helpers).

Runs weekly. For each rotation it diffs the week LEAD_DAYS (28) ahead against the week before it
and DMs whoever is newly on duty, so a mid-shift person is not re-pinged. A sheet-read failure, a
header or column-count mismatch, or an unresolved name exits non-zero, and the workflow's failure
alert is the notification. A week with no row yet is skipped quietly.

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

# Rotations to remind. The DM label is ROTATION_HEADERS[rotation][0], and self_test asserts this
# covers every rotation.
REMINDER_ROTATIONS = (
    "l3",
    "l4",
    "ci",
    "release-blackduck",
    "flaky-canton",
    "flaky-sdk",
    "flaky-coordination",
    "sdk",
)


def names_for_rotation(values: list[list[Any]], rotation: str, monday: dt.date) -> list[str]:
    """Names on the given rotation for ``monday``. Raises LookupError on a header/column-count
    mismatch, and its MissingWeekError subclass when the week has no row."""
    row, cols = select_rota.find_header_columns(
        values, select_rota.ROTATION_HEADERS[rotation], select_rota.expected_columns(rotation)
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
            select_rota.log(
                f"ERROR: could not read the rota sheet for the week of {date_str} ({exc!r}). "
                f"Check the sheet and the service account's access to it: {sheet_url}"
            )
            return 1
        # Target and prior weeks share a half-year tab except at the H1/H2 boundary: reuse if so.
        if select_rota.half_year_tab(prior_monday) == select_rota.half_year_tab(target_monday):
            prior_values = target_values
        else:
            try:
                prior_values = select_rota.fetch_week_values(prior_monday, sheet_id)
            except Exception as exc:
                # failure-surfaced: sets prior_read_failed, checked below to force exit_code = 1
                select_rota.log(
                    f"warning: could not read the prior week ({exc!r}), treating everyone in the "
                    "target week as newly starting and marking the run failed"
                )
                prior_values = []
                prior_read_failed = True

    exit_code = 1 if prior_read_failed else 0
    for rotation in REMINDER_ROTATIONS:
        label = select_rota.ROTATION_HEADERS[rotation][0]
        try:
            target_names = names_for_rotation(target_values, rotation, target_monday)
        except select_rota.MissingWeekError as exc:
            # silent-ok: a future week without a row yet is not actionable (nobody to remind)
            select_rota.log(f"note: no {label} assignment for the week of {date_str} yet ({exc!r})")
            continue
        except Exception as exc:
            # failure-surfaced: sets exit_code = 1, a header or column-count mismatch is a sheet bug
            select_rota.log(f"warning: could not resolve target-week {label} names ({exc!r})")
            exit_code = 1
            continue
        try:
            prior_names = names_for_rotation(prior_values, rotation, prior_monday)
        except Exception as exc:
            # failure-surfaced: sets exit_code = 1 below, may re-DM someone already mid-shift
            select_rota.log(
                f"warning: could not resolve prior-week {label} names ({exc!r}), "
                "treating everyone in the target week as newly starting"
            )
            prior_names = []
            exit_code = 1

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
                # failure-surfaced: sets exit_code = 1 below
                select_rota.log(f"    ERROR: {exc!r}")
                exit_code = 1

    return exit_code


def self_test() -> None:
    """In-memory checks of the pure helpers. No network, clock, or roster file."""
    # REMINDER_ROTATIONS must not drift out of ROTATION_HEADERS, and must cover every rotation.
    for rotation in REMINDER_ROTATIONS:
        assert rotation in select_rota.ROTATION_HEADERS, rotation
    assert set(REMINDER_ROTATIONS) == set(select_rota.ROTATION_HEADERS)

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
    assert names_for_rotation(values, "ci", target_monday) == ["Bo"]
    assert names_for_rotation(values, "ci", prior_monday) == ["Al"]
    assert newly_starting(
        names_for_rotation(values, "ci", target_monday),
        names_for_rotation(values, "ci", prior_monday),
    ) == ["Bo"]

    # Flaky SDK has two slots, an empty one is skipped
    sdk = [["Week", "Flaky SDK", "Flaky SDK"], [target_serial, "", "Sd"]]
    assert names_for_rotation(sdk, "flaky-sdk", target_monday) == ["Sd"]

    # main()'s DM and exit-code contract, driven through --values-file so no network is touched
    _self_test_main(monday, prior_serial, target_serial)

    print("remind_upcoming_rota self-checks passed")


def _self_test_main(monday: dt.date, prior_serial: int, target_serial: int) -> None:
    """DM and exit-code contract via a temp roster and stubbed send_dm (no network)."""
    import tempfile

    # every rotation's header with its expected column count, only the CI slot is filled
    header = ["Week"] + [
        select_rota.ROTATION_HEADERS[r][0]
        for r in REMINDER_ROTATIONS
        for _ in range(select_rota.expected_columns(r))
    ]

    def row(serial: int, ci_name: str) -> list[Any]:
        cells: list[Any] = [serial] + [""] * (len(header) - 1)
        cells[header.index("CI")] = ci_name
        return cells

    roster = {"slack_id_by_name": {"Al": "UA", "Bo": "UB"}}
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
            newly = [header, row(prior_serial, "Al"), row(target_serial, "Bo")]
            newly_file = Path(tmp) / "newly.json"
            newly_file.write_text(json.dumps(newly), encoding="utf-8")
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(newly_file)]) == 0
            assert sent == [("UB", sent[0][1])] and "CI rota" in sent[0][1], sent

            # A per-rotation prior-week resolve failure (its row is simply missing from the
            # prior sheet) still DMs the newly-starting person, but exits 1 to flag the
            # degraded run instead of looking green.
            prior_gap = [header, row(target_serial, "Bo")]  # no row for prior_monday
            prior_gap_file = Path(tmp) / "prior_gap.json"
            prior_gap_file.write_text(json.dumps(prior_gap), encoding="utf-8")
            sent.clear()
            assert (
                main(
                    [
                        "--date",
                        date,
                        "--target-values-file",
                        str(newly_file),
                        "--prior-values-file",
                        str(prior_gap_file),
                    ]
                )
                == 1
            )
            assert sent == [("UB", sent[0][1])] and "CI rota" in sent[0][1], sent

            # Al is on both weeks (a continuing shift): no reminder.
            cont = [header, row(prior_serial, "Al"), row(target_serial, "Al")]
            cont_file = Path(tmp) / "cont.json"
            cont_file.write_text(json.dumps(cont), encoding="utf-8")
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(cont_file)]) == 0
            assert sent == [], sent

            # An unknown target-week name has no Slack id: nothing sent, exit 1.
            unknown = [header, row(prior_serial, "Al"), row(target_serial, "Nobody Here")]
            unknown_file = Path(tmp) / "unknown.json"
            unknown_file.write_text(json.dumps(unknown), encoding="utf-8")
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(unknown_file)]) == 1
            assert sent == [], sent

            # The target week has no row yet: nothing to remind, not a failure.
            no_target = [header, row(prior_serial, "Al")]
            no_target_file = Path(tmp) / "no_target.json"
            no_target_file.write_text(json.dumps(no_target), encoding="utf-8")
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(no_target_file)]) == 0
            assert sent == [], sent

            # A column-count mismatch (Flaky SDK listed once) is a sheet bug, not "nobody
            # assigned yet": the other rotations still DM, but the run exits 1.
            sdk_col = header.index("Flaky SDK")
            one_sdk = [
                header[:sdk_col] + header[sdk_col + 1 :],
                *(r[:sdk_col] + r[sdk_col + 1 :] for r in newly[1:]),
            ]
            one_sdk_file = Path(tmp) / "one_sdk.json"
            one_sdk_file.write_text(json.dumps(one_sdk), encoding="utf-8")
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(one_sdk_file)]) == 1
            assert sent == [("UB", sent[0][1])] and "CI rota" in sent[0][1], sent

            # A missing header (Flaky SDK deleted) is a sheet bug too: exit 1, others still DM.
            no_sdk = [[c for c, h in zip(r, header) if h != "Flaky SDK"] for r in newly]
            no_sdk_file = Path(tmp) / "no_sdk.json"
            no_sdk_file.write_text(json.dumps(no_sdk), encoding="utf-8")
            sent.clear()
            assert main(["--date", date, "--target-values-file", str(no_sdk_file)]) == 1
            assert sent == [("UB", sent[0][1])] and "CI rota" in sent[0][1], sent

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
