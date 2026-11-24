"""Tests for post_top_flakes.py: the daily Slack list must be fresh, persistent and unduplicated."""

import json
import sys
from dataclasses import replace
from datetime import date, datetime, timezone

import post_top_flakes
from post_top_flakes import (
    STALE_AFTER_DAYS,
    Flake,
    failure_stats,
    format_message,
    last_failure_time,
    latest_assignment,
    lookback_start,
    post_to_slack,
    rank_flakes,
    timeout_rows,
    timeouts_since,
    with_assignment_age,
)

LONG_AGO = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _table_body(*rows: str) -> str:
    return "| Date | Job | Shard | Build |\n|---|---|---|---|\n" + "\n".join(rows)


def test_burst_does_not_outrank_persistent_flake():
    today = date(2026, 11, 23)
    burst = _table_body(*[f"| 2026-11-17 12:00:{i:02d} | t | 1 | [{i}](u) |" for i in range(50)])
    steady = _table_body(
        "| 2026-11-12 12:00:00 | t | 1 | [900](u) |",
        "| 2026-11-15 12:00:00 | t | 1 | [901](u) |",
        "| 2026-11-20 12:00:00 | t | 1 | [902](u) |",
    )
    issues = [
        {"number": 1, "title": "[main] Flaky Burst", "body": burst},
        {"number": 2, "title": "[main] Flaky Steady", "body": steady},
    ]
    assert [f.name for f in rank_flakes(issues, today, LONG_AGO)] == ["Steady", "Burst"]


def test_shards_of_one_build_count_as_one_build():
    body = _table_body(
        "| 2026-11-20 12:00:00 | t | 1 | [77](u) |",
        "| 2026-11-20 12:00:01 | t | 2 | [77](u) |",
    )
    assert failure_stats(body, date(2026, 11, 23)) == (1, 1, 2)


def test_window_covers_exactly_the_stated_number_of_days():
    # "Last 14 days" with today as day one runs from Nov 10, so Nov 9 must not count.
    body = _table_body(
        "| 2026-11-09 12:00:00 | t | 1 | [1](u) |",
        "| 2026-11-10 12:00:00 | t | 1 | [2](u) |",
    )
    assert failure_stats(body, date(2026, 11, 23)) == (1, 1, 1)


def test_rows_outside_window_and_circleci_lines():
    body = (
        "2026-11-20 09:20:38 job:test node_index:9 url:https://x/1\n"
        "2025-01-01 00:00:00 job:test url:https://x/2\n"
    )
    assert failure_stats(body, date(2026, 11, 23)) == (1, 1, 1)


def test_issue_without_recent_failures_is_dropped():
    issues = [{"number": 3, "title": "[main] Flaky Old", "body": "2025-01-01 00:00:00 job:t url:x"}]
    assert rank_flakes(issues, date(2026, 11, 23), LONG_AGO) == []


def test_only_issues_that_failed_since_the_last_post_are_listed():
    # A persistent flake that did not fail today stays out, so the post changes every day.
    now = datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc)
    issues = [
        {
            "number": 1,
            "title": "[main] Flaky FailedToday",
            "body": _table_body("| 2026-11-24 02:00:00 | t | 1 | [1](u) |"),
        },
        {
            "number": 2,
            "title": "[main] Flaky QuietToday",
            "body": _table_body(
                "| 2026-11-20 02:00:00 | t | 1 | [2](u) |",
                "| 2026-11-21 02:00:00 | t | 1 | [3](u) |",
                "| 2026-11-22 02:00:00 | t | 1 | [4](u) |",
            ),
        },
    ]
    flakes = rank_flakes(issues, now.date(), lookback_start(now))
    assert [f.name for f in flakes] == ["FailedToday"]


def test_monday_looks_back_over_the_weekend():
    # No post runs on Saturday or Sunday, so a Friday failure must still show up on Monday.
    monday = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)
    tuesday = datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc)
    friday_failure = "| 2026-11-20 10:00:00 | t | 1 | [1](u) |"
    issues = [{"number": 1, "title": "[main] Flaky Friday", "body": _table_body(friday_failure)}]
    assert len(rank_flakes(issues, monday.date(), lookback_start(monday))) == 1
    assert rank_flakes(issues, tuesday.date(), lookback_start(tuesday)) == []


def test_late_start_does_not_skip_failures_between_two_runs():
    # Monday's run started at 06:05 and Tuesday's at 06:30. A failure at Monday 06:10 came after
    # Monday's run read the issues, so Tuesday must still list it. A window relative to the run
    # start would open at Monday 06:30 and lose it.
    tuesday_late = datetime(2026, 11, 24, 6, 30, tzinfo=timezone.utc)
    failure = "| 2026-11-23 06:10:00 | t | 1 | [1](u) |"
    issues = [{"number": 1, "title": "[main] Flaky Gap", "body": _table_body(failure)}]
    assert len(rank_flakes(issues, tuesday_late.date(), lookback_start(tuesday_late))) == 1


def test_lookback_starts_at_the_previous_workday_post_time():
    assert lookback_start(datetime(2026, 11, 24, 6, 30, tzinfo=timezone.utc)) == datetime(
        2026, 11, 23, 6, 0, tzinfo=timezone.utc
    )
    # Monday reaches back to Friday, over the weekend.
    assert lookback_start(datetime(2026, 11, 23, 6, 5, tzinfo=timezone.utc)) == datetime(
        2026, 11, 20, 6, 0, tzinfo=timezone.utc
    )


def test_single_failure_day_is_not_pluralized():
    flake = Flake(
        number=1,
        name="X",
        owner="unowned",
        days=1,
        builds=1,
        rows=1,
        last_failure=datetime(2026, 11, 24, 4, 12, tzinfo=timezone.utc),
    )
    assert "| 1 day, last failed" in format_message([flake])


NOW = datetime(2026, 11, 24, 6, 0, tzinfo=timezone.utc)


def _flake(owner: str, assigned_days: int | None = None) -> Flake:
    return Flake(
        number=7,
        name="X",
        owner=owner,
        days=2,
        builds=2,
        rows=2,
        last_failure=NOW,
        assigned_days=assigned_days,
    )


def test_assignment_age_counts_from_the_latest_assignment_of_a_current_assignee():
    # Reassigning resets the clock, and people who are no longer assigned do not count.
    events = [
        {"a": "former", "t": "2026-11-20T10:00:00Z"},
        {"a": "adam", "t": "2026-10-29T14:00:00Z"},
        {"a": "adam", "t": "2026-11-19T17:00:00Z"},
    ]
    assert latest_assignment(events, {"adam"}) == datetime(2026, 11, 19, 17, tzinfo=timezone.utc)
    assert latest_assignment(events, {"nobody"}) is None
    assert with_assignment_age(_flake("adam"), events, NOW).assigned_days == 4


def test_unowned_flake_gets_no_assignment_age():
    events = [{"a": "adam", "t": "2026-11-19T17:00:00Z"}]
    assert with_assignment_age(_flake("unowned"), events, NOW).assigned_days is None


def test_unowned_flakes_are_flagged_in_header_and_row():
    # An unowned flake that is still failing is the one nobody is working on, so it must stand out.
    message = format_message([_flake("unowned"), _flake("adam", 1)])
    header, unowned_row, owned_row = message.splitlines()[1:]
    assert "1 of 2 unowned" in header
    assert unowned_row.startswith("1. :rotating_light:")
    assert ":rotating_light:" not in owned_row


def test_flake_assigned_for_more_than_the_limit_is_flagged_but_not_at_the_limit():
    stale = format_message([_flake("adam", STALE_AFTER_DAYS + 1)])
    at_limit = format_message([_flake("adam", STALE_AFTER_DAYS)])
    unknown = format_message([_flake("adam", None)])
    assert f"1 assigned for more than {STALE_AFTER_DAYS} days" in stale
    assert f"assigned {STALE_AFTER_DAYS + 1} days ago" in stale
    assert "assigned" not in at_limit
    assert "assigned" not in unknown


def test_footer_says_how_many_failed_flakes_are_not_shown():
    # With only the top few listed, readers must not take the list for everything that failed.
    assert format_message([_flake("adam", 1)], more=3).splitlines()[-1] == (
        "+3 more failed since the last post"
    )
    assert "more failed" not in format_message([_flake("adam", 1)])


def test_rank_flakes_returns_every_failed_issue_so_the_caller_can_count_the_rest():
    body = _table_body("| 2026-11-24 02:00:00 | t | 1 | [1](u) |")
    issues = [{"number": n, "title": f"[main] Flaky T{n}", "body": body} for n in range(1, 9)]
    assert len(rank_flakes(issues, NOW.date(), lookback_start(NOW))) == 8


def test_slack_payload_does_not_unfurl_links(monkeypatch):
    # Five issue links would each expand into a preview card and bloat the channel.
    sent = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"ok": true}'

    def fake_urlopen(request, timeout):
        sent.update(json.loads(request.data))
        return FakeResponse()

    monkeypatch.setenv("SLACK_BOT_QA_NOTIFICATIONS", "xoxb-test")
    monkeypatch.setenv("SLACK_CHANNEL_ID_TEAM_CANTON_FLAKY_TESTS", "C123")
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    post_to_slack("hello")
    assert sent == {"channel": "C123", "text": "hello", "unfurl_links": False}


def _cause_row(day: str, build: int, cause: str) -> str:
    return f"| {day} 12:00:00 | test_dev | 3 | [{build}](u) | [abc](c) | [log](e) | {cause} |"


def test_timeouts_are_counted_per_row_from_the_cause_column_inside_the_window():
    # A watchdog kill hangs a shard for 30 minutes, so it must not blend into ordinary failures.
    body = _table_body(
        _cause_row("2026-11-20", 1, "timeout"),
        _cause_row("2026-11-21", 2, "timeout"),
        _cause_row("2026-11-21", 3, "regular"),
        _cause_row("2026-10-01", 4, "timeout"),
    )
    assert timeout_rows(body, date(2026, 11, 23)) == 2


def test_circleci_lines_and_empty_body_have_no_timeouts():
    assert timeout_rows("2026-11-20 09:20:38 job:test url:https://x/1\n", date(2026, 11, 23)) == 0
    assert timeout_rows("", date(2026, 11, 23)) == 0


def test_ranked_flake_carries_its_timeout_count():
    body = _table_body(
        _cause_row("2026-11-24", 1, "timeout"), _cause_row("2026-11-24", 2, "regular")
    )
    issues = [{"number": 5, "title": "[main] Flaky Hangs", "body": body}]
    assert rank_flakes(issues, NOW.date(), lookback_start(NOW))[0].timeouts == 1


def test_message_shows_timeout_count_and_counts_flakes_with_timeouts_in_header():
    hung = replace(_flake("adam", 1), timeouts=3)
    one = replace(_flake("adam", 1), timeouts=1)
    quick = _flake("adam", 1)
    lines = format_message([hung, one, quick]).splitlines()
    assert "2 with timeouts" in lines[1]
    assert lines[2].endswith(":stopwatch: *3 timeouts in 14 days*")
    assert lines[3].endswith(":stopwatch: *1 timeout in 14 days*")
    assert "stopwatch" not in lines[4]


def test_timeout_flakes_below_the_top_list_are_still_listed():
    # A timeout hangs a shard for 30 minutes, so ranking below the cut must not hide it.
    top = [_flake("adam", 1)]
    hidden = replace(_flake("unowned"), number=99, timeouts=2)
    message = format_message(top, more=4, timeout_extras=[hidden])
    lines = message.splitlines()
    assert "1 with timeouts" in lines[1]
    assert lines[-2].startswith("- :rotating_light:") and lines[-2].endswith(
        "*2 timeouts in 14 days*"
    )
    assert lines[-3] == ":stopwatch: *Also timed out since the last post, below the top list:*"
    assert lines[-1] == "+4 more failed since the last post"


def test_no_extra_section_without_timeout_extras():
    assert "Also timed out" not in format_message([_flake("adam", 1)])


def test_an_old_timeout_does_not_count_as_a_recent_one():
    # The timeout was 11 days ago and today's failure was ordinary. The flake still shows its
    # 14 day count, but it must not look like a shard hang that happened since the last post.
    body = _table_body(
        _cause_row("2026-11-13", 1, "timeout"),
        _cause_row("2026-11-24", 2, "regular"),
    )
    issues = [{"number": 8, "title": "[main] Flaky Old", "body": body}]
    flake = rank_flakes(issues, NOW.date(), lookback_start(NOW))[0]
    assert (flake.timeouts, flake.recent_timeouts) == (1, 0)


def test_recent_timeouts_use_the_time_of_day_not_only_the_date():
    # The cut is Monday 06:00. A timeout at 05:00 that day belongs to the previous post.
    since = datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc)
    before = _table_body("| 2026-11-23 05:00:00 | t | 1 | [1](u) | [a](c) | [l](e) | timeout |")
    after = _table_body("| 2026-11-23 07:00:00 | t | 1 | [2](u) | [a](c) | [l](e) | timeout |")
    assert timeouts_since(before, since) == 0
    assert timeouts_since(after, since) == 1


def test_message_has_no_flag_line_when_everything_is_fresh_and_owned():
    message = format_message([_flake("adam", 2)])
    assert len(message.splitlines()) == 2


def test_last_failure_is_the_newest_row_even_outside_the_window():
    body = "2025-01-01 00:00:00 job:t url:x\n2025-06-01 12:30:00 job:t url:y\n"
    assert last_failure_time(body) == datetime(2025, 6, 1, 12, 30, tzinfo=timezone.utc)
    assert last_failure_time("no rows here") is None


def test_message_links_issue_and_marks_unowned():
    flake = Flake(
        number=42,
        name="A<B>",
        owner="unowned",
        days=3,
        builds=4,
        rows=5,
        last_failure=datetime(2026, 11, 24, 4, 12, tzinfo=timezone.utc),
    )
    message = format_message([flake])
    assert "<https://github.com/DACH-NY/canton/issues/42|#42>" in message
    assert "A&lt;B&gt;" in message
    assert "last failed 11-24 04:12 UTC" in message
    assert message.endswith("| unowned")


def _row_at(stamp: str, build: int, cause: str) -> str:
    return f"| {stamp} | test_dev | 3 | [{build}](u) | [abc](c) | [log](e) | {cause} |"


def test_main_lists_only_recent_timeouts_below_the_top_and_counts_the_rest(monkeypatch, capsys):
    # main() picks who goes under "Also timed out". Swapping recent_timeouts for timeouts there
    # would list a flake whose only timeout is old as a current shard hang, and no test of
    # format_message alone would notice.
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    persistent = _table_body(
        *[_row_at(f"2026-11-{day} 02:00:00", day, "regular") for day in (24, 22, 21, 20)]
    )
    top = [
        {"number": n, "title": f"[main] Flaky Top{n}", "body": persistent} for n in range(101, 106)
    ]
    recent_timeout = _table_body(_row_at("2026-11-24 03:00:00", 1, "timeout"))
    old_timeout_only = _table_body(
        _row_at("2026-11-13 12:00:00", 2, "timeout"), _row_at("2026-11-24 03:00:00", 3, "regular")
    )
    plain = _table_body(_row_at("2026-11-24 03:00:00", 4, "regular"))
    below = [
        {"number": 201, "title": "[main] Flaky RecentTimeout", "body": recent_timeout},
        {"number": 202, "title": "[main] Flaky OldTimeout", "body": old_timeout_only},
        {"number": 203, "title": "[main] Flaky Plain", "body": plain},
    ]

    def no_slack(message):
        raise AssertionError("a dry run must not post to Slack")

    monkeypatch.setattr(post_top_flakes, "datetime", FixedDateTime)
    monkeypatch.setattr(post_top_flakes, "fetch_open_flaky_issues", lambda: top + below)
    monkeypatch.setattr(post_top_flakes, "fetch_assignment_events", lambda number: [])
    monkeypatch.setattr(post_top_flakes, "post_to_slack", no_slack)
    monkeypatch.setattr(sys, "argv", ["post_top_flakes.py"])

    post_top_flakes.main()

    lines = capsys.readouterr().out.splitlines()
    header = lines.index(":stopwatch: *Also timed out since the last post, below the top list:*")
    section = lines[header + 1 : -1]
    assert len(section) == 1 and "issues/201|" in section[0]
    assert not any("issues/202|" in line or "issues/203|" in line for line in lines)
    # 8 failed, 5 shown and 1 in the section, so the footer counts only the two in neither.
    assert lines[-1] == "+2 more failed since the last post"
