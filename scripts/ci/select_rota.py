#!/usr/bin/env python3
"""Select the Slack user ID(s) on the current rota shift for a given rotation.

Reads the current week's row from the rota Google Sheet and prints the Slack user ID(s)
of whoever is on the requested rotation, space-separated, on one line. Most rotations
resolve to one person; ``flaky-canton`` spans three columns and prints three IDs.

If the sheet cannot be read for any reason (missing/invalid credentials, network error,
tab/row not found, unresolved name), it falls back to a deterministic pick from the
roster's ``fallback_pool`` and prints one ID, so CI failure notifications never break.

Intended to run under the nix shell (which provides ``cryptography``), e.g.:
    .ci/nix-exec python3 scripts/ci/select_rota.py --rotation ci

Credentials: a Google service-account JSON key in the env var ``GCLOUD_SHEETS_SA_KEY``
(the SA must be a Viewer on the sheet). The spreadsheet id defaults to the roster sheet
and can be overridden with ``ROTA_SHEET_ID``.

roster_people.json (sibling file) format:
    slack_id_by_name: {"Full Name": "U…", …}  every person who can appear in a column.
    fallback_pool:    ["Full Name", …]          who is pinged when the sheet is unreadable.
Name matching against the sheet is accent- and case-insensitive (trimmed).

Tab/week assumption: the tab is ``YYYYH1``/``YYYYH2`` derived from the current week's
Monday, and that week's row is expected to live in the tab matching its Monday. A week
straddling the June/December half-year boundary must therefore be filed under the half of
its Monday; otherwise the row is not found and the fallback pool is used.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import sys
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
DEFAULT_SHEET_ID = "1PEmLKqoB2DpokVhao5PNxznMI5ufZZbXgUju7Npn0BU"
SHEETS_EPOCH = dt.date(1899, 12, 30)  # Google Sheets serial-date epoch
ROSTER_PATH = Path(__file__).resolve().parent / "roster_people.json"

# rotation cli name -> the sheet column header(s) it maps to.
ROTATION_HEADERS: dict[str, list[str]] = {
    "ci": ["CI"],
    "flaky-canton": ["Flaky Canton"],
    "release-blackduck": ["Release & Blackduck"],
    "l3": ["L3"],
    "l4": ["L4"],
    "sdk": ["SDK"],
    "flaky-sdk": ["Flaky SDK"],
    "flaky-coordination": ["Flaky coordination"],
}

# rotation cli name -> number of sheet columns (people) it should resolve to. Most
# rotations are one person; flaky-canton is a three-person group. A sheet that yields a
# different count (typo'd/duplicated/missing header) is treated as a lookup failure so
# it degrades to the fallback pool rather than silently pinging the wrong set.
ROTATION_COLUMNS: dict[str, int] = {"flaky-canton": 3}
DEFAULT_ROTATION_COLUMNS = 1

# CircleCI job name -> rotation to ping when that job fails on main (the red-main ping).
# Jobs not listed here ping the CI rota. Keep in sync with the jobs that invoke
# slack_red_main_with_volunteer.
JOB_ROTATION: dict[str, str] = {
    "blackduck": "release-blackduck",
}
DEFAULT_JOB_ROTATION = "ci"

# Slack Web API plumbing shared by the rota tools that write to Slack (topic updates,
# upcoming-shift reminders). The bot token lives in SLACK_TOKEN_ENV.
SLACK_OPEN_DM_URL = "https://slack.com/api/conversations.open"
SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
SLACK_TOKEN_ENV = "SLACK_BOT_QA_NOTIFICATIONS"


def rotation_for_job(job: str) -> str:
    """Map a CircleCI job name to the rotation that should be pinged when it fails."""
    return JOB_ROTATION.get(job, DEFAULT_JOB_ROTATION)


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


def normalize(name: str) -> str:
    """Accent-fold, lowercase and trim a name for tolerant matching."""
    decomposed = unicodedata.normalize("NFKD", name)
    no_accents = "".join(c for c in decomposed if not unicodedata.combining(c))
    return no_accents.casefold().strip()


def current_monday(today: dt.date) -> dt.date:
    return today - dt.timedelta(days=today.weekday())


def half_year_tab(monday: dt.date) -> str:
    return f"{monday.year}H{1 if monday.month <= 6 else 2}"


def format_week_date(monday: dt.date) -> str:
    """Human-friendly week label, e.g. 'August 31' (no leading zero on the day)."""
    return f"{monday.strftime('%B')} {monday.day}"


def _b64url(data: bytes) -> bytes:
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def _compact_json(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()


def get_access_token(sa: dict[str, Any]) -> str:
    """Exchange a service-account key for an OAuth access token via a hand-built JWT.

    Done manually (sign a JWT, POST it to the token endpoint) to avoid the heavy
    google-auth dependency, which is not in the nix shell (only cryptography is).
    """
    # cryptography is provided by the nix shell, imported lazily so the rest of the
    # module (parsing/fallback) loads even where it is absent.
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    token_uri = sa.get("token_uri", "https://oauth2.googleapis.com/token")
    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    header = {"alg": "RS256", "typ": "JWT"}
    claims = {
        "iss": sa["client_email"],
        "scope": SCOPE,
        "aud": token_uri,
        "iat": now,
        "exp": now + 3600,
    }
    signing_input = _b64url(_compact_json(header)) + b"." + _b64url(_compact_json(claims))
    key = serialization.load_pem_private_key(sa["private_key"].encode(), password=None)
    signature = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    assertion = (signing_input + b"." + _b64url(signature)).decode()

    body = urllib.parse.urlencode(
        {"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion}
    ).encode()
    req = urllib.request.Request(token_uri, data=body, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)["access_token"]


def fetch_tab_values(token: str, sheet_id: str, tab: str) -> list[list[Any]]:
    rng = urllib.parse.quote(f"'{tab}'!A1:AZ300", safe="")
    sid = urllib.parse.quote(sheet_id, safe="")
    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{sid}/values/{rng}"
        "?valueRenderOption=UNFORMATTED_VALUE&dateTimeRenderOption=SERIAL_NUMBER"
    )
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp).get("values", [])
    except urllib.error.HTTPError as err:  # surface the API reason for CI debugging
        body = err.read().decode("utf-8", "replace")[:500]
        raise RuntimeError(f"Sheets API HTTP {err.code}: {body}") from err


def fetch_week_values(monday: dt.date, sheet_id: str) -> list[list[Any]]:
    """Fetch the sheet rows for the half-year tab holding ``monday`` (from GCLOUD_SHEETS_SA_KEY)."""
    sa_key = os.environ.get("GCLOUD_SHEETS_SA_KEY")
    if not sa_key:
        raise RuntimeError("GCLOUD_SHEETS_SA_KEY is not set")
    token = get_access_token(json.loads(sa_key))
    return fetch_tab_values(token, sheet_id, half_year_tab(monday))


def find_header_columns(
    values: list[list[Any]], headers: list[str], expected: int | None = None
) -> tuple[int, list[int]]:
    """Locate the header row and the column indices matching the requested header(s).

    When ``expected`` is given, the number of matching columns must equal it, otherwise
    a LookupError is raised (a misconfigured sheet then degrades to the fallback pool).
    """
    wanted = {h.strip() for h in headers}
    for idx, row in enumerate(values):
        cols = [i for i, cell in enumerate(row) if isinstance(cell, str) and cell.strip() in wanted]
        if cols:
            if expected is not None and len(cols) != expected:
                raise LookupError(
                    f"header(s) {headers}: expected {expected} column(s), found {len(cols)} at row {idx}"
                )
            return idx, cols
    raise LookupError(f"header(s) {headers} not found in sheet")


def names_for_week(
    values: list[list[Any]], header_row: int, cols: list[int], monday: dt.date
) -> list[str]:
    for row in values[header_row + 1 :]:
        if not row:
            continue
        cell = row[0]
        if not isinstance(cell, (int, float)):
            continue
        if isinstance(cell, bool):  # bool is an int subclass, not a date
            continue
        # col A is a serial date (integer days since SHEETS_EPOCH) thanks to
        # dateTimeRenderOption=SERIAL_NUMBER on the fetch.
        if SHEETS_EPOCH + dt.timedelta(days=int(cell)) == monday:
            return [str(row[i]).strip() for i in cols if i < len(row) and str(row[i]).strip()]
    raise LookupError(f"no row for week starting {monday.isoformat()}")


def resolve_slack_ids(names: list[str], slack_id_by_name: dict[str, str]) -> list[str]:
    index = {normalize(k): v for k, v in slack_id_by_name.items()}
    slack_ids = []
    for name in names:
        sid = index.get(normalize(name))
        if sid:
            slack_ids.append(sid)
        else:
            log(f"warning: no Slack ID for '{name}' in roster_people.json")
    return slack_ids


def fallback_pick(roster: dict[str, Any]) -> str:
    known = {normalize(k) for k in roster["slack_id_by_name"]}
    pool = [n for n in roster["fallback_pool"] if normalize(n) in known]
    slack_ids = resolve_slack_ids(pool, roster["slack_id_by_name"])
    if not slack_ids:
        raise RuntimeError("fallback_pool resolved to no Slack IDs")
    workflow_id = os.environ.get("CIRCLE_WORKFLOW_ID") or os.environ.get("GITHUB_RUN_ID") or ""
    if not workflow_id:
        log("warning: no CIRCLE_WORKFLOW_ID/GITHUB_RUN_ID set; fallback pick will not vary per run")
    seed = int(hashlib.sha256(workflow_id.encode()).hexdigest(), 16)
    return slack_ids[seed % len(slack_ids)]


def slack_post(url: str, token: str, fields: dict[str, str]) -> dict[str, Any]:
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


def send_dm(token: str, user_id: str, text: str) -> None:
    """Send a private Slack DM to a user: open the 1:1 conversation, then post to it.

    chat.postMessage needs a conversation id, not a user id, so conversations.open is
    called first. The bot token needs both im:write (for conversations.open) and
    chat:write (for chat.postMessage)."""
    opened = slack_post(SLACK_OPEN_DM_URL, token, {"users": user_id})
    channel = opened["channel"]["id"]
    slack_post(SLACK_POST_MESSAGE_URL, token, {"channel": channel, "text": text})


def notify_tooling_failure(
    message: str,
    roster: dict[str, Any],
    token: str | None,
    rotation: str | None = None,
    monday: dt.date | None = None,
    values: list[list[Any]] | None = None,
) -> None:
    """Best-effort: DM whoever is on the CI rota about some CI/tooling failure.

    A broken tool is a CI/tooling problem, which is the CI rota's job to look into, not
    whichever shift it happened to break under. Reuses ``values``/``monday`` (sheet
    rows already fetched elsewhere, if any) to resolve the CI rota without a second
    network round-trip, otherwise fetches them fresh. Falls back to the roster's
    ``fallback_pool`` when the CI rota can't be resolved either, or when ``rotation``
    (the rotation the *original* failure happened under, if any) already IS "ci", to
    avoid paging a broken thing about itself. Never raises: a failure here must not
    affect the caller's own handling of the original failure.
    """
    if not token:
        log("warning: no Slack token in the environment, cannot report the tooling failure")
        return
    try:
        targets: list[str] = []
        if rotation != "ci":
            try:
                week = monday or current_monday(dt.datetime.now(dt.timezone.utc).date())
                sheet_values = values
                if sheet_values is None:
                    sheet_id = os.environ.get("ROTA_SHEET_ID", DEFAULT_SHEET_ID)
                    sheet_values = fetch_week_values(week, sheet_id)
                header_row, cols = find_header_columns(
                    sheet_values,
                    ROTATION_HEADERS["ci"],
                    ROTATION_COLUMNS.get("ci", DEFAULT_ROTATION_COLUMNS),
                )
                names = names_for_week(sheet_values, header_row, cols, week)
                targets = resolve_slack_ids(names, roster["slack_id_by_name"])
            except Exception as ci_exc:
                log(
                    f"warning: could not resolve the CI rota for a tooling-failure notice ({ci_exc!r})"
                )
        if not targets:
            targets = resolve_slack_ids(roster["fallback_pool"], roster["slack_id_by_name"])
        for uid in targets:
            try:
                send_dm(token, uid, message)
            except Exception as dm_exc:
                log(f"warning: failed to DM {uid} about the tooling failure ({dm_exc!r})")
    except Exception as notify_exc:
        log(f"warning: could not report the tooling failure ({notify_exc!r})")


def main() -> int:
    parser = argparse.ArgumentParser(description="Print Slack ID(s) for the current rota shift.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--rotation", choices=sorted(ROTATION_HEADERS))
    target.add_argument("--job", help="CircleCI job name, mapped to a rotation via JOB_ROTATION")
    parser.add_argument("--date", help="ISO date to evaluate (default: today); for testing")
    parser.add_argument(
        "--values-file", help="local JSON of sheet rows instead of fetching; for testing"
    )
    args = parser.parse_args()
    rotation = args.rotation or rotation_for_job(args.job)

    try:
        roster = json.loads(ROSTER_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        # The roster is the sole source of every Slack id (CI rota, fallback pool,
        # everyone), so if it is unreadable there is truly nobody left to notify: exit
        # non-zero so this at least surfaces as a failed step, instead of a silent,
        # unnoticed "nobody will be pinged".
        log(
            f"ERROR: roster {ROSTER_PATH} unreadable ({exc!r}); NO responder emitted, nobody will be pinged"
        )
        return 1
    today = (
        dt.date.fromisoformat(args.date) if args.date else dt.datetime.now(dt.timezone.utc).date()
    )
    monday = current_monday(today)
    headers = ROTATION_HEADERS[rotation]

    values: list[list[Any]] | None = None
    try:
        if args.values_file:
            values = json.loads(Path(args.values_file).read_text())
        else:
            sa_key = os.environ.get("GCLOUD_SHEETS_SA_KEY")
            if not sa_key:
                raise RuntimeError("GCLOUD_SHEETS_SA_KEY is not set")
            sa = json.loads(sa_key)
            token = get_access_token(sa)
            sheet_id = os.environ.get("ROTA_SHEET_ID", DEFAULT_SHEET_ID)
            values = fetch_tab_values(token, sheet_id, half_year_tab(monday))

        expected_cols = ROTATION_COLUMNS.get(rotation, DEFAULT_ROTATION_COLUMNS)
        header_row, cols = find_header_columns(values, headers, expected_cols)
        names = names_for_week(values, header_row, cols, monday)
        slack_ids = resolve_slack_ids(names, roster["slack_id_by_name"])
        if slack_ids:
            print(" ".join(slack_ids))
            return 0
        log("warning: sheet had no resolvable name for this rotation/week")
    except (
        Exception
    ) as exc:  # any failure must degrade to the fallback (never break CI notifications)
        log(f"warning: rota lookup failed ({exc!r}), using fallback pool")
        notify_tooling_failure(
            f":warning: select_rota.py failed to resolve the `{rotation}` rotation for "
            f"the week of {monday.isoformat()}: {exc!r}. It fell back to the fallback "
            "pool for the actual ping, can someone check the rota sheet / select_rota.py?",
            roster,
            os.environ.get(SLACK_TOKEN_ENV),
            rotation,
            monday,
            values,
        )

    # Last resort: a deterministic pick from the roster pool. This must never raise,
    # an unhandled error here would crash with a traceback instead of a clean message.
    try:
        print(fallback_pick(roster))
    except Exception as exc:
        # Nobody was pinged at all: exit 1 so this does not look like a clean run.
        log(f"warning: fallback pick failed ({exc!r}), no responder emitted")
        return 1
    return 0


def self_test() -> None:
    """In-memory checks of the pure helpers. No network, clock, or roster file."""
    # normalize: accent-fold + casefold + trim
    assert normalize("João Sá Sousa") == normalize("  joao sa sousa  ")
    assert normalize("Sören") == "soren"

    # current_monday: any weekday maps to that week's Monday
    assert current_monday(dt.date(2025, 7, 6)) == dt.date(2025, 6, 30)  # Sunday -> prior Monday
    assert current_monday(dt.date(2025, 6, 30)) == dt.date(2025, 6, 30)  # Monday -> itself

    # half_year_tab incl. the June/December boundary (week filed under its Monday's half)
    assert half_year_tab(dt.date(2025, 6, 30)) == "2025H1"
    assert half_year_tab(dt.date(2025, 7, 7)) == "2025H2"
    assert half_year_tab(dt.date(2025, 12, 29)) == "2025H2"

    # rotation_for_job: mapped jobs route to their rota, everything else to CI
    assert rotation_for_job("blackduck") == "release-blackduck"
    assert rotation_for_job("unit_test") == "ci"
    assert rotation_for_job("") == "ci"
    for mapped in JOB_ROTATION.values():  # every mapping targets a real rotation
        assert mapped in ROTATION_HEADERS, mapped

    # _b64url: URL-safe alphabet, no padding
    assert _b64url(b"\xff\xff\xff") == b"____"
    assert b"=" not in _b64url(b"any data!")

    serial = (dt.date(2025, 6, 30) - SHEETS_EPOCH).days
    sheet = [
        ["Week", "CI", "Flaky Canton", "Flaky Canton"],
        [serial, "Ada", "Bjarne", "Carla"],
    ]
    # find_header_columns: the two literal "Flaky Canton" cells both match, absent -> LookupError
    assert find_header_columns(sheet, ROTATION_HEADERS["flaky-canton"]) == (0, [2, 3])
    assert find_header_columns(sheet, ROTATION_HEADERS["ci"]) == (0, [1])
    try:
        find_header_columns(sheet, ["Nope"])
        raise AssertionError("expected LookupError for a missing header")
    except LookupError:
        pass
    # expected-count validation: a matching count passes, a mismatch raises LookupError
    assert find_header_columns(sheet, ROTATION_HEADERS["flaky-canton"], 2) == (0, [2, 3])
    assert find_header_columns(sheet, ROTATION_HEADERS["ci"], 1) == (0, [1])
    try:
        find_header_columns(sheet, ROTATION_HEADERS["flaky-canton"], 1)  # 2 columns, expected 1
        raise AssertionError("expected LookupError for an unexpected column count")
    except LookupError:
        pass

    # names_for_week: matches the serial row, skips empty/bool/non-numeric col-A rows,
    # drops blank cells, raises when the week is absent
    mixed = [
        ["Week", "Flaky Canton", "Flaky Canton"],
        [],  # empty row
        [True, "x", "y"],  # bool col A (bool is an int subclass, must be skipped)
        ["note", "x", "y"],  # non-numeric col A
        [serial, "Bjarne", ""],  # the match, second person cell blank
    ]
    assert names_for_week(mixed, 0, [1, 2], dt.date(2025, 6, 30)) == ["Bjarne"]
    assert names_for_week(sheet, 0, [2, 3], dt.date(2025, 6, 30)) == ["Bjarne", "Carla"]
    try:
        names_for_week(sheet, 0, [2, 3], dt.date(2030, 1, 7))
        raise AssertionError("expected LookupError for a missing week")
    except LookupError:
        pass

    # resolve_slack_ids: accent/case-insensitive, order preserved, unknown names dropped
    smap = {"João Sá Sousa": "U1", "Sören Bleikertz": "U2"}
    assert resolve_slack_ids(["joao sa sousa", "Soren Bleikertz"], smap) == ["U1", "U2"]
    assert resolve_slack_ids(["Sören Bleikertz", "João Sá Sousa"], smap) == ["U2", "U1"]
    assert resolve_slack_ids(["Nobody Here"], smap) == []

    # integration: the real two-column flaky-canton layout resolves to two Slack IDs
    roster = {
        "slack_id_by_name": {"Ada": "UCI", "Bjarne": "UB", "Carla": "UC"},
        "fallback_pool": ["Ada"],
    }
    hr, cols = find_header_columns(sheet, ROTATION_HEADERS["flaky-canton"])
    fc = resolve_slack_ids(
        names_for_week(sheet, hr, cols, dt.date(2025, 6, 30)), roster["slack_id_by_name"]
    )
    assert fc == ["UB", "UC"], fc

    # fallback_pick: deterministic per workflow id, restores env, raises on an empty pool
    global fallback_pick, ROSTER_PATH
    saved = {k: os.environ.get(k) for k in ("CIRCLE_WORKFLOW_ID", "GITHUB_RUN_ID")}
    try:
        os.environ.pop("GITHUB_RUN_ID", None)
        os.environ["CIRCLE_WORKFLOW_ID"] = "wf-self-test"
        pick1 = fallback_pick(roster)
        pick2 = fallback_pick(roster)
        assert pick1 == pick2 and pick1 in {"UCI", "UB", "UC"}
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    try:
        fallback_pick({"slack_id_by_name": {"Ada": "UCI"}, "fallback_pool": []})
        raise AssertionError("expected RuntimeError for an empty fallback pool")
    except RuntimeError:
        pass

    # main(): when both the sheet lookup and the deterministic fallback pick fail,
    # exit 1 instead of silently returning 0, nobody was pinged at all.
    real_fallback_pick = fallback_pick
    real_roster_path = ROSTER_PATH
    real_argv = sys.argv
    import tempfile

    def raising_fallback_pick(_roster: dict[str, Any]) -> str:
        raise RuntimeError("boom")

    try:
        fallback_pick = raising_fallback_pick
        with tempfile.TemporaryDirectory() as tmp:
            roster_file = Path(tmp) / "roster.json"
            roster_file.write_text(
                json.dumps({"slack_id_by_name": {}, "fallback_pool": []}), encoding="utf-8"
            )
            ROSTER_PATH = roster_file
            values_file = Path(tmp) / "values.json"
            values_file.write_text(json.dumps([["Week"]]), encoding="utf-8")  # no "CI" header
            sys.argv = [
                "select_rota.py",
                "--rotation",
                "ci",
                "--date",
                "2025-06-30",
                "--values-file",
                str(values_file),
            ]
            assert main() == 1
    finally:
        fallback_pick = real_fallback_pick
        ROSTER_PATH = real_roster_path
        sys.argv = real_argv

    # format_week_date: month name plus un-padded day
    assert format_week_date(dt.date(2025, 8, 31)) == "August 31"
    assert format_week_date(dt.date(2025, 1, 6)) == "January 6"

    # send_dm: opens the 1:1 first, then posts to the returned conversation id (never the
    # user id). Fake slack_post so the two-call contract is checked without any network.
    global slack_post, send_dm
    real_slack_post = slack_post
    calls: list[tuple[str, dict[str, str]]] = []

    def fake_slack_post(url: str, token: str, fields: dict[str, str]) -> dict[str, Any]:
        calls.append((url, fields))
        return {"ok": True, "channel": {"id": "D123"}}

    try:
        slack_post = fake_slack_post
        send_dm("xoxb-test", "U999", "hi there")
    finally:
        slack_post = real_slack_post
    assert calls == [
        (SLACK_OPEN_DM_URL, {"users": "U999"}),
        (SLACK_POST_MESSAGE_URL, {"channel": "D123", "text": "hi there"}),
    ], calls

    # notify_tooling_failure: prefers paging the CI rota (a broken tool is a CI/tooling
    # problem), falling back to fallback_pool when the CI rota can't be resolved either,
    # when the original failure's rotation already IS "ci", or when there is no sheet
    # data to reuse and no sheet credentials to fetch fresh. fallback_pool is
    # deliberately a different person than the CI column here so the two paths are
    # distinguishable in the assertions.
    real_send_dm = send_dm
    dmed: list[str] = []

    def fake_send_dm(token: str, user_id: str, text: str) -> None:
        dmed.append(user_id)

    notify_roster = {
        "slack_id_by_name": {"Ada": "UCI", "Bjarne": "UB", "Carla": "UC"},
        "fallback_pool": ["Carla"],
    }
    ci_sheet = [
        ["Week", "CI", "Flaky Canton", "Flaky Canton", "Flaky Canton"],
        [serial, "Ada", "Bjarne", "Bjarne", "Bjarne"],
    ]
    no_ci_sheet = [["Week", "Flaky Canton"], [serial, "Bjarne"]]
    week = dt.date(2025, 6, 30)
    saved_sa_key = os.environ.pop("GCLOUD_SHEETS_SA_KEY", None)
    try:
        send_dm = fake_send_dm

        # no token: no-op, nobody DMed
        notify_tooling_failure("boom", notify_roster, None, "flaky-canton", week, ci_sheet)
        assert dmed == []

        # resolvable CI column in the reused sheet values: pages the CI rota
        notify_tooling_failure("boom", notify_roster, "xoxb-test", "flaky-canton", week, ci_sheet)
        assert dmed == ["UCI"], dmed
        dmed.clear()

        # the original failure's rotation already IS "ci": must not page "ci" about itself
        notify_tooling_failure("boom", notify_roster, "xoxb-test", "ci", week, ci_sheet)
        assert dmed == ["UC"], dmed
        dmed.clear()

        # no sheet values to reuse and no GCLOUD_SHEETS_SA_KEY to fetch fresh: falls back
        notify_tooling_failure("boom", notify_roster, "xoxb-test", "flaky-canton", week, None)
        assert dmed == ["UC"], dmed
        dmed.clear()

        # sheet values present but no "CI" column in them: falls back
        notify_tooling_failure(
            "boom", notify_roster, "xoxb-test", "flaky-canton", week, no_ci_sheet
        )
        assert dmed == ["UC"], dmed
        dmed.clear()

        # no rotation given (a failure unrelated to any specific rotation, e.g. a Slack
        # post itself failing): still resolves and pages the CI rota like the default case
        notify_tooling_failure("boom", notify_roster, "xoxb-test", None, week, ci_sheet)
        assert dmed == ["UCI"], dmed
        dmed.clear()
    finally:
        send_dm = real_send_dm
        if saved_sa_key is not None:
            os.environ["GCLOUD_SHEETS_SA_KEY"] = saved_sa_key

    print("select_rota self-checks passed")


def _run_self_test() -> None:
    """Run self_test() and fail if it leaked os.environ. Inlined rather than reusing
    flaky_common so select_rota stays standalone, it is also used outside the flaky
    pipeline (the red-main ping) and on branches that predate flaky_common."""
    before = dict(os.environ)
    try:
        self_test()
    finally:
        if dict(os.environ) != before:
            raise RuntimeError("self_test() polluted os.environ")


def test_self_test() -> None:
    """Pytest entry point so lint_and_test_py_scripts.yml's plain `pytest` run exercises
    these self-checks too, not just `--self-test` (which nothing else invokes in CI)."""
    _run_self_test()


if __name__ == "__main__":
    # select_rota's stdout is the data channel (Slack IDs), so unlike the other
    # pipeline steps the self-tests run only under --self-test, never on a real run.
    if "--self-test" in sys.argv:
        _run_self_test()
        raise SystemExit(0)
    raise SystemExit(main())
