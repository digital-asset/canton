#!/usr/bin/env python3
"""Post the open flaky-test issues that failed since the last post to the flaky tests Slack channel.

Only issues with a failure row since the previous workday are listed, so the post changes every day.
They are ranked by the number of distinct days with a failure in the last WINDOW_DAYS days, so a
one-day burst of failures does not outrank a test that keeps flaking. Ties break on distinct
builds, then on failure rows. Nothing is posted when no issue failed since the last post.
Unowned issues are marked, and so are assigned ones still failing STALE_AFTER_DAYS days after the
latest assignment. Each row shows its number of timeouts, which are the failures that hang a shard.
Flakes that timed out since the last post are listed even when they rank below the top list.
A missed run is not made up by the next one. Rerun it the same day with --post to recover it.

Needs GH_TOKEN with issues read. Posting needs SLACK_BOT_QA_NOTIFICATIONS and
SLACK_CHANNEL_ID_TEAM_CANTON_FLAKY_TESTS. Prints the message by default, pass --post to send it.
"""

import json
import os
import re
import subprocess
import sys
import urllib.request
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone

REPO = "DACH-NY/canton"
WINDOW_DAYS = 14
TOP_N = 5
FLAKY_LABELS = "flaky-test,flaky-test-sdk,flaky-test-flyway,flaky-test-pekko,flaky-timeouts"
TITLE_PREFIX = re.compile(r"^\[[^\]]+\]\s*Flaky\s*")
MAX_NAME_LENGTH = 70
SEARCH_RESULT_CAP = 1000
# The scheduled run starts at 06:00 UTC on workdays, see post_top_flakes.yml.
POST_HOUR_UTC = 6
# An assigned flake still failing after more than this many days gets flagged in the post.
STALE_AFTER_DAYS = 7
UNOWNED = "unowned"

# A failure row starts a line, either as a markdown table row or as a bare CircleCI line.
_ROW = re.compile(r"^\|?\s*(\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2}(.*)$", re.MULTILINE)
_ROW_FULL = re.compile(r"^\|?\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(.*)$", re.MULTILINE)
_ROW_STAMP = re.compile(r"^\|?\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", re.MULTILINE)
_BUILD = re.compile(r"\[(\d+)\]\(|url:(\S+)")


@dataclass(frozen=True)
class Flake:
    number: int
    name: str
    owner: str
    days: int
    builds: int
    rows: int
    last_failure: datetime
    # Days since the issue was last assigned, None when it has no owner or the lookup failed.
    assigned_days: int | None = None
    # Failures in the window that were watchdog kills, each one blocks its shard for about 30 min.
    timeouts: int = 0
    # The same count restricted to the period since the last post, which decides who is listed
    # below the top list, so an old timeout cannot make a flake look like a current shard hang.
    recent_timeouts: int = 0


def timeouts_since(body: str, since: datetime) -> int:
    """Timeout rows at or after `since`, with the time of day taken into account."""
    count = 0
    for stamp, rest in _ROW_FULL.findall(body or ""):
        cause = rest.rstrip().rstrip("|").rsplit("|", 1)[-1].strip()
        row_time = datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc)
        if cause == "timeout" and row_time >= since:
            count += 1
    return count


def timeout_rows(body: str, today: date, window_days: int = WINDOW_DAYS) -> int:
    """Rows inside the window whose Cause column is `timeout`. CircleCI lines have no cause."""
    start = today - timedelta(days=window_days - 1)
    count = 0
    for row_date, rest in _ROW.findall(body or ""):
        cause = rest.rstrip().rstrip("|").rsplit("|", 1)[-1].strip()
        if cause == "timeout" and date.fromisoformat(row_date) >= start:
            count += 1
    return count


def latest_assignment(events: list[dict], logins: set[str]) -> datetime | None:
    """When one of the current assignees was most recently assigned, from the issue events."""
    times = [
        datetime.fromisoformat(e["t"].replace("Z", "+00:00")) for e in events if e["a"] in logins
    ]
    return max(times, default=None)


def with_assignment_age(flake: Flake, events: list[dict], now: datetime) -> Flake:
    if flake.owner == UNOWNED:
        return flake
    assigned_at = latest_assignment(events, set(flake.owner.split(", ")))
    if assigned_at is None:
        return flake
    return replace(flake, assigned_days=(now - assigned_at).days)


def lookback_start(now: datetime) -> datetime:
    """The previous workday at POST_HOUR_UTC, so Monday also covers the weekend.

    The boundary is fixed, not relative to `now`. A run that GitHub starts late would otherwise
    open a window later than the last one closed and skip the failures in between.
    """
    day = now.date() - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return datetime(day.year, day.month, day.day, POST_HOUR_UTC, tzinfo=timezone.utc)


def last_failure_time(body: str) -> datetime | None:
    stamps = _ROW_STAMP.findall(body or "")
    if not stamps:
        return None
    return max(datetime.fromisoformat(s).replace(tzinfo=timezone.utc) for s in stamps)


def failure_stats(body: str, today: date, window_days: int = WINDOW_DAYS) -> tuple[int, int, int]:
    """Return (distinct days, distinct builds, rows) for the failure rows inside the window."""
    # Today counts as day one, so the window covers exactly window_days calendar days.
    start = today - timedelta(days=window_days - 1)
    days, builds, rows = set(), set(), 0
    for row_date, rest in _ROW.findall(body or ""):
        if date.fromisoformat(row_date) < start:
            continue
        rows += 1
        days.add(row_date)
        match = _BUILD.search(rest)
        # A row without a build id counts as its own build.
        builds.add(match.group(1) or match.group(2) if match else f"row-{rows}")
    return len(days), len(builds), rows


def rank_flakes(issues: list[dict], today: date, since: datetime) -> list[Flake]:
    """The issues with a failure at or after `since`, most persistent first."""
    flakes = []
    for issue in issues:
        body = issue.get("body", "")
        days, builds, rows = failure_stats(body, today)
        last_failure = last_failure_time(body)
        if rows == 0 or last_failure is None or last_failure < since:
            continue
        assignees = issue.get("assignees") or []
        flakes.append(
            Flake(
                number=issue["number"],
                name=TITLE_PREFIX.sub("", issue["title"]).strip(),
                owner=", ".join(a["login"] for a in assignees) or UNOWNED,
                days=days,
                builds=builds,
                rows=rows,
                last_failure=last_failure,
                timeouts=timeout_rows(body, today),
                recent_timeouts=timeouts_since(body, since),
            )
        )
    flakes.sort(key=lambda f: (-f.days, -f.builds, -f.rows, f.number))
    return flakes


def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_row(prefix: str, f: Flake) -> str:
    name = _escape(f.name[:MAX_NAME_LENGTH])
    link = f"<https://github.com/{REPO}/issues/{f.number}|#{f.number}>"
    last = f.last_failure.strftime("%m-%d %H:%M UTC")
    days = f"{f.days} day" if f.days == 1 else f"{f.days} days"
    marker = ":rotating_light: " if f.owner == UNOWNED else ""
    age = f" :hourglass_flowing_sand: *assigned {f.assigned_days} days ago*" if _is_stale(f) else ""
    noun = "timeout" if f.timeouts == 1 else "timeouts"
    hung = f" :stopwatch: *{f.timeouts} {noun} in {WINDOW_DAYS} days*" if f.timeouts else ""
    return f"{prefix} {marker}{name} {link} | {days}, last failed {last} | {f.owner}{age}{hung}"


def format_message(
    flakes: list[Flake], more: int = 0, timeout_extras: list[Flake] | None = None
) -> str:
    """The Slack text. `timeout_extras` are timeout flakes that did not make the top list.

    They are listed anyway, since each timeout hangs a shard for 30 minutes and a rank below the
    cut must not hide it.
    """
    extras = timeout_extras or []
    lines = [
        "*Flaky tests that failed since the last post* "
        f"(ranked by days with a failure in the last {WINDOW_DAYS} days)"
    ]
    unowned = sum(1 for f in flakes if f.owner == UNOWNED)
    stale = sum(1 for f in flakes if _is_stale(f))
    flags = []
    if unowned:
        flags.append(f":rotating_light: *{unowned} of {len(flakes)} unowned*")
    if stale:
        flags.append(
            f":hourglass_flowing_sand: *{stale} assigned for more than {STALE_AFTER_DAYS} days*"
        )
    timed_out = sum(1 for f in flakes if f.timeouts) + len(extras)
    if timed_out:
        flags.append(
            f":stopwatch: *{timed_out} with timeouts in {WINDOW_DAYS} days "
            "(30 min each, blocks the shard)*"
        )
    if flags:
        lines.append(", ".join(flags))
    for rank, f in enumerate(flakes, start=1):
        lines.append(_format_row(f"{rank}.", f))
    if extras:
        lines.append(":stopwatch: *Also timed out since the last post, below the top list:*")
        lines.extend(_format_row("-", f) for f in extras)
    if more > 0:
        lines.append(f"+{more} more failed since the last post")
    return "\n".join(lines)


def _is_stale(flake: Flake) -> bool:
    return flake.assigned_days is not None and flake.assigned_days > STALE_AFTER_DAYS


def fetch_open_flaky_issues() -> list[dict]:
    result = subprocess.run(
        [
            "gh", "issue", "list", "-R", REPO, "--state", "open", "-L", "5000",
            "--search", f"label:{FLAKY_LABELS}",
            "--json", "number,title,body,assignees",
        ],
        capture_output=True,
        text=True,
    )  # fmt: skip
    if result.returncode != 0:
        sys.exit(f"gh issue list failed: {result.stderr.strip()}")
    issues = json.loads(result.stdout)
    # The GitHub search API returns at most 1000 results, so a full page may be a truncated one.
    if len(issues) >= SEARCH_RESULT_CAP:
        sys.exit(f"Search returned {len(issues)} issues, the ranking may be missing some")
    return issues


def fetch_assignment_events(number: int) -> list[dict]:
    """The `assigned` events of an issue as {"a": login, "t": time}. Empty if the call fails."""
    result = subprocess.run(
        [
            "gh", "api", f"repos/{REPO}/issues/{number}/events", "--paginate", "--jq",
            '.[] | select(.event == "assigned") | {a: .assignee.login, t: .created_at}',
        ],
        capture_output=True,
        text=True,
    )  # fmt: skip
    if result.returncode != 0:
        # A missing age only drops the stale flag for this row, the post itself still goes out.
        print(
            f"::warning::Could not read events of #{number}: {result.stderr.strip()}",
            file=sys.stderr,
        )
        return []
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


def post_to_slack(message: str) -> None:
    token = os.environ.get("SLACK_BOT_QA_NOTIFICATIONS", "")
    channel = os.environ.get("SLACK_CHANNEL_ID_TEAM_CANTON_FLAKY_TESTS", "")
    if not token or not channel:
        sys.exit(
            "SLACK_BOT_QA_NOTIFICATIONS and SLACK_CHANNEL_ID_TEAM_CANTON_FLAKY_TESTS must be set"
        )
    request = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps({"channel": channel, "text": message, "unfurl_links": False}).encode(
            "utf-8"
        ),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        result = json.loads(response.read().decode("utf-8"))
    if not result.get("ok"):
        sys.exit(f"Slack API error: {result.get('error', 'unknown')}")


def main() -> None:
    now = datetime.now(timezone.utc)
    since = lookback_start(now)
    flakes = rank_flakes(fetch_open_flaky_issues(), now.date(), since)
    if not flakes:
        print(f"No open flaky issue failed since {since:%Y-%m-%d %H:%M} UTC, nothing to post")
        return
    shown = [with_assignment_age(f, fetch_assignment_events(f.number), now) for f in flakes[:TOP_N]]
    extras = [
        with_assignment_age(f, fetch_assignment_events(f.number), now)
        for f in flakes[TOP_N:]
        if f.recent_timeouts
    ]
    rest = len(flakes) - len(shown) - len(extras)
    message = format_message(shown, more=rest, timeout_extras=extras)
    print(message)
    if "--post" in sys.argv[1:]:
        post_to_slack(message)
        print("Posted to Slack")


if __name__ == "__main__":
    main()
