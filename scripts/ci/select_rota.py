#!/usr/bin/env python3
"""Select the Slack user ID(s) on the current rota shift for a given rotation.

Reads the current week's row from the rota Google Sheet and prints the Slack user ID(s)
of whoever is on the requested rotation, space-separated, on one line. Most rotations
resolve to one person; ``flaky-canton`` spans two columns and prints two IDs.

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
# rotations are one person; flaky-canton is a two-person pair. A sheet that yields a
# different count (typo'd/duplicated/missing header) is treated as a lookup failure so
# it degrades to the fallback pool rather than silently pinging the wrong set.
ROTATION_COLUMNS: dict[str, int] = {"flaky-canton": 2}
DEFAULT_ROTATION_COLUMNS = 1

# CircleCI job name -> rotation to ping when that job fails on main (the red-main ping).
# Jobs not listed here ping the CI rota. Keep in sync with the jobs that invoke
# slack_red_main_with_volunteer.
JOB_ROTATION: dict[str, str] = {
    "blackduck": "release-blackduck",
}
DEFAULT_JOB_ROTATION = "ci"


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
        # The roster is the fallback's only source, so if it is unreadable there is no
        # safe degrade: emit nothing and let the caller send an unmentioned alert. Exit 0
        # so the notification step itself still runs (never break CI notifications).
        log(
            f"ERROR: roster {ROSTER_PATH} unreadable ({exc!r}); NO responder emitted, nobody will be pinged"
        )
        return 0
    today = (
        dt.date.fromisoformat(args.date) if args.date else dt.datetime.now(dt.timezone.utc).date()
    )
    monday = current_monday(today)
    headers = ROTATION_HEADERS[rotation]

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

    # Last resort: a deterministic pick from the roster pool. This must never raise,
    # an unhandled error here would fail the CI notification step it feeds.
    try:
        print(fallback_pick(roster))
    except Exception as exc:
        log(f"warning: fallback pick failed ({exc!r}), no responder emitted")
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


if __name__ == "__main__":
    # select_rota's stdout is the data channel (Slack IDs), so unlike the other
    # pipeline steps the self-tests run only under --self-test, never on a real run.
    if "--self-test" in sys.argv:
        _run_self_test()
        raise SystemExit(0)
    raise SystemExit(main())
