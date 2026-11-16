#!/usr/bin/env python3
"""Attaches flaky-test issues to monthly issues under the delivery tracking issue. Runs daily.

Delivery reports on flaky tests through https://github.com/DACH-NY/delivery/issues/503. GitHub
allows at most 100 sub-issues per parent, so each flaky issue goes under a "Canton Flaky Tests
YYYY-MM" issue for the month it was created in, and the monthly issues go under #503. A month
with more than 100 flaky issues continues in "Canton Flaky Tests YYYY-MM (2)" and so on.

This script is the only writer to the delivery repo and the workflow runs one instance at a time,
so a month is never created twice. Each run links every open flaky issue, and every one created
since the start of last month, that has no parent yet. A missing month is created, or attached to
#503 if an earlier run created it but failed before attaching it. An issue that already has
another parent is left alone. Any failure exits non-zero, and the workflow then pages the CI rota.

Env: GH_TOKEN (Issues read and write on DACH-NY/delivery, Issues read on DACH-NY/canton).

Usage:
  link_flaky_issues_to_delivery.py [--dry-run]
"""

import argparse
import contextlib
import datetime as dt
import io
import re
import subprocess
import sys
from collections import defaultdict
from unittest.mock import patch

from flaky_common import check_result, run_gh_with_retries

CANTON_REPO = "DACH-NY/canton"
DELIVERY_REPO = "DACH-NY/delivery"
TRACKING_ISSUE = 503
FLAKY_MILESTONE = 31  # "Flaky Tests"
MONTHLY_TITLE_PATTERN = re.compile(r"^Canton Flaky Tests \d{4}-\d{2}( \(\d+\))?$")
MAX_SUB_ISSUES = 100  # GitHub's limit per parent issue
MONTHLY_BODY = (
    "Created and filled by CI. Flaky-test issues from DACH-NY/canton created in this month."
)


def gh(*args: str, attempts: int = 3) -> subprocess.CompletedProcess:
    return run_gh_with_retries(["api", *args], attempts)


def gh_ok(*args: str) -> str:
    result = gh(*args)
    check_result(result)
    return result.stdout


def tsv(text: str) -> list[list[str]]:
    return [line.split("\t") for line in text.splitlines() if line]


def start_of_last_month(today: dt.date) -> dt.date:
    return (today.replace(day=1) - dt.timedelta(days=1)).replace(day=1)


def flaky_issues(today: dt.date) -> dict[int, tuple[int, str]]:
    """number -> (id, YYYY-MM) of the open flaky issues and of those created since the start of
    last month, so a closed one is still linked when a few runs failed in a row."""
    cutoff = start_of_last_month(today).isoformat()
    jq = ".[] | select(.pull_request | not) | [.number, .id, .created_at] | @tsv"
    base = f"repos/{CANTON_REPO}/issues?milestone={FLAKY_MILESTONE}&per_page=100"
    rows = tsv(gh_ok("--paginate", f"{base}&state=open", "--jq", jq))
    rows += [
        row
        for row in tsv(
            gh_ok("--paginate", f"{base}&state=all&since={cutoff}T00:00:00Z", "--jq", jq)
        )
        if row[2] >= cutoff
    ]
    return {int(number): (int(issue_id), created[:7]) for number, issue_id, created in rows}


def monthly_title(month: str, part: int) -> str:
    return f"Canton Flaky Tests {month}" + (f" ({part})" if part > 1 else "")


def monthly_issues() -> dict[str, list[int]]:
    """title -> numbers, lowest first, of the monthly issues under #503. A title repeats if a copy
    was attached by hand. One page is enough, GitHub allows 100 sub-issues per parent at most."""
    path = f"repos/{DELIVERY_REPO}/issues/{TRACKING_ISSUE}/sub_issues?per_page=100"
    months: dict[str, list[int]] = defaultdict(list)
    for number, title in sorted(
        tsv(gh_ok(path, "--jq", ".[] | [.number, .title] | @tsv")), key=lambda r: int(r[0])
    ):
        if MONTHLY_TITLE_PATTERN.match(title):
            months[title].append(int(number))
    return months


def sub_issue_ids(number: int) -> list[int]:
    path = f"repos/{DELIVERY_REPO}/issues/{number}/sub_issues?per_page=100"
    return [int(issue_id) for issue_id in gh_ok(path, "--jq", ".[].id").split()]


def has_parent(number: int) -> bool:
    """True if canton issue `number` has a parent. GitHub answers 404 when it has none, any other
    error raises."""
    result = gh(f"repos/{CANTON_REPO}/issues/{number}/parent", "--jq", ".number")
    if result.returncode != 0 and "(HTTP 404)" in result.stderr:
        return False
    check_result(result)
    return True


def attach(parent: int, issue_id: int) -> subprocess.CompletedProcess:
    path = f"repos/{DELIVERY_REPO}/issues/{parent}/sub_issues"
    return gh("-X", "POST", path, "-F", f"sub_issue_id={issue_id}")


def create_monthly_issue(title: str) -> int:
    """Creates the monthly issue `title`, attaches it to #503 and returns its number. Reuses one an
    earlier run created but could not attach, so a failed run never leads to a second issue for
    the same month. Lists
    every delivery issue, not only recent ones, since the leftover can be old. Search is not used,
    it can lag behind recent writes."""
    every = f"repos/{DELIVERY_REPO}/issues?state=all&per_page=100"
    found = tsv(gh_ok("--paginate", every, "--jq", ".[] | [.number, .id, .title] | @tsv"))
    exact = sorted(
        (int(number), int(issue_id))
        for number, issue_id, found_title in found
        if found_title == title
    )
    if exact:
        number, issue_id = exact[0]
    else:
        # Not retried: a 5xx can come back after GitHub created the issue, and a retry would
        # create a second one. A failed run is found again by the lookup above on the next run.
        created = gh(
            "-X",
            "POST",
            f"repos/{DELIVERY_REPO}/issues",
            "-f",
            f"title={title}",
            "-f",
            f"body={MONTHLY_BODY}",
            "--jq",
            "[.number, .id] | @tsv",
            attempts=1,
        )
        check_result(created)
        number, issue_id = map(int, created.stdout.split())
    check_result(attach(TRACKING_ISSUE, issue_id))
    return number


def link(today: dt.date, dry_run: bool) -> list[str]:
    """Links what is missing and returns the issues that could not be linked."""
    listed = monthly_issues()
    ids = {number: sub_issue_ids(number) for numbers in listed.values() for number in numbers}
    # A repeated title uses its lowest number, as create_monthly_issue() does, and the children of
    # every copy count as linked.
    months = {title: numbers[0] for title, numbers in listed.items()}
    children = {title: ids[numbers[0]] for title, numbers in listed.items()}
    done = {issue_id for linked in ids.values() for issue_id in linked}
    todo: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for number, (issue_id, month) in sorted(flaky_issues(today).items()):
        if issue_id in done:
            continue
        if has_parent(number):
            print(f"#{number} already has another parent, left alone")
            continue
        todo[month].append((number, issue_id))
    failed = []
    planned: dict[str, int] = defaultdict(int)
    for month, issues in sorted(todo.items()):
        part = 1
        for number, issue_id in issues:
            while len(children.get(monthly_title(month, part), [])) >= MAX_SUB_ISSUES:
                part += 1
            title = monthly_title(month, part)
            if title not in children:
                if not dry_run:
                    try:
                        months[title] = create_monthly_issue(title)
                    except Exception as exc:
                        # failure-surfaced: returned in `failed`, so main() exits non-zero and pages
                        failed.append(
                            f"{title}: could not create or attach it ({exc}), the rest of {month} is not linked"
                        )
                        break
                children[title] = []
            if dry_run:
                planned[title] += 1
                children[title].append(issue_id)
                continue
            result = attach(months[title], issue_id)
            if result.returncode != 0:
                failed.append(f"#{number}: {result.stderr.strip()}")
            else:
                children[title].append(issue_id)
                print(f"linked #{number} to {DELIVERY_REPO}#{months[title]} ({title})")
    for title, count in planned.items():
        create = "" if title in months else f"create {title} and "
        print(f"would {create}link {count} issue(s) into {title}")
    return failed


def _annotation(message: str) -> str:
    """An ::error:: workflow command. GitHub ends a command at a newline, so they are encoded."""
    return "::error::" + message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="only print what would be created and linked"
    )
    args = parser.parse_args(argv)
    try:
        failed = link(dt.datetime.now(dt.timezone.utc).date(), args.dry_run)
        if failed:
            raise RuntimeError(
                f"could not link {len(failed)} flaky issue(s):\n" + "\n".join(failed)
            )
    except Exception as exc:
        print(_annotation(str(exc)))
        return 1
    return 0


# --- tests ----------------------------------------------------------------------------------


class FakeGitHub:
    """Answers the gh calls of this script from in-memory canton and delivery issues."""

    def __init__(self, canton, delivery, parents=None, fail_attach=(), fail_create=()):
        self.canton = canton  # number -> (id, created_at, state)
        self.delivery = delivery  # number -> {"id", "title", "children": [ids]}
        self.parents = parents or {}  # canton number -> parent number, outside delivery
        self.fail_attach = set(fail_attach)  # canton ids whose attach fails
        self.fail_create = set(fail_create)  # monthly titles whose creation fails
        self.writes: list[tuple] = []
        self.parent_lookups = 0
        self.create_attempts: list[int] = []

    def __call__(self, args, attempts=3, stdin=None):
        args = args[1:]

        def ok(out=""):
            return subprocess.CompletedProcess(args, 0, out, "")

        def fail(err):
            return subprocess.CompletedProcess(args, 1, "", err)

        if args[:2] == ["-X", "POST"] and args[2].endswith("/sub_issues"):
            parent = int(args[2].split("/")[-2])
            issue_id = int(args[4].split("=")[1])
            if issue_id in self.fail_attach:
                return fail("gh: Validation Failed (HTTP 422)")
            self.writes.append(("attach", parent, issue_id))
            self.delivery[parent]["children"].append(issue_id)
            return ok()
        if args[:3] == ["-X", "POST", f"repos/{DELIVERY_REPO}/issues"]:
            number = max(self.delivery) + 1
            title = args[4].split("=", 1)[1]
            if title in self.fail_create:
                return fail("gh: Server Error (HTTP 502)")
            self.delivery[number] = {"id": number * 10, "title": title, "children": []}
            self.writes.append(("create", number, title))
            self.create_attempts.append(attempts)
            return ok(f"{number}\t{number * 10}\n")
        paginated = args[0] == "--paginate"
        if (args[1] if paginated else args[0]).startswith(f"repos/{DELIVERY_REPO}/issues?"):
            newest = sorted(self.delivery, reverse=True)
            # GitHub returns 100 per page, only --paginate fetches the rest.
            numbers = newest if paginated else newest[:100]
            rows = [
                f"{n}\t{self.delivery[n]['id']}\t{self.delivery[n]['title']}\n" for n in numbers
            ]
            return ok("".join(rows))
        if paginated:
            query = args[1]
            rows = []
            for number, (issue_id, created, state) in self.canton.items():
                if "state=open" in query and state != "open":
                    continue
                rows.append(f"{number}\t{issue_id}\t{created}\n")
            return ok("".join(rows))
        path = args[0]
        if path.endswith("/parent"):
            self.parent_lookups += 1
            number = int(path.split("/")[-2])
            issue_id = self.canton[number][0]
            for parent, issue in self.delivery.items():
                if issue_id in issue["children"]:
                    return ok(f"{parent}\n")
            if number in self.parents:
                return ok(f"{self.parents[number]}\n")
            return fail("gh: No parent issue found (HTTP 404)")
        if "/sub_issues" in path:
            parent = int(path.split("/")[-2])
            children = self.delivery[parent]["children"]
            if parent == TRACKING_ISSUE:
                rows = [
                    f"{n}\t{self.delivery[n]['title']}\n"
                    for n in self.delivery
                    if self.delivery[n]["id"] in children
                ]
                return ok("".join(rows))
            return ok("".join(f"{i}\n" for i in children))
        raise AssertionError(args)


TODAY = dt.date(2026, 10, 26)


def _delivery(*monthly):
    """Delivery with #503 and the given (number, title, attached to #503) monthly issues."""
    issues = {TRACKING_ISSUE: {"id": 5030, "title": "Canton Flaky Tests", "children": []}}
    for number, title, attached in monthly:
        issues[number] = {"id": number * 10, "title": title, "children": []}
        if attached:
            issues[TRACKING_ISSUE]["children"].append(number * 10)
    return issues


def _run(fake, dry_run=False):
    with patch(f"{__name__}.run_gh_with_retries", side_effect=fake):
        return link(TODAY, dry_run)


def test_links_new_issues_and_a_rerun_writes_nothing():
    fake = FakeGitHub(
        canton={1: (100, "2026-10-02T10:00:00Z", "open")},
        delivery=_delivery((526, "Canton Flaky Tests 2026-10", True)),
    )
    assert _run(fake) == []
    assert fake.writes == [("attach", 526, 100)]
    # Daily runs must be idempotent, an already linked issue is not posted again. It is not even
    # looked up one by one, which would cost a request per open flaky issue every day.
    lookups = fake.parent_lookups
    assert _run(fake) == []
    assert fake.writes == [("attach", 526, 100)]
    assert fake.parent_lookups == lookups, fake.parent_lookups


def test_missing_month_is_created_once_and_attached_to_the_tracking_issue():
    fake = FakeGitHub(
        canton={1: (100, "2026-09-02T10:00:00Z", "open"), 2: (200, "2026-09-03T10:00:00Z", "open")},
        delivery=_delivery(),
    )
    assert _run(fake) == []
    # Creating is not idempotent, a retry after a 5xx that GitHub had already acted on would
    # make a second monthly issue. So it gets a single attempt.
    assert fake.create_attempts == [1], fake.create_attempts
    assert fake.writes == [
        ("create", 504, "Canton Flaky Tests 2026-09"),
        ("attach", TRACKING_ISSUE, 5040),
        ("attach", 504, 100),
        ("attach", 504, 200),
    ]


def test_a_month_left_unattached_by_a_failed_run_is_reused_not_created_again():
    fake = FakeGitHub(
        canton={1: (100, "2026-09-02T10:00:00Z", "open")},
        delivery=_delivery((600, "Canton Flaky Tests 2026-09", False)),
    )
    assert _run(fake) == []
    assert fake.writes == [("attach", TRACKING_ISSUE, 6000), ("attach", 600, 100)]


def test_an_issue_under_another_parent_is_left_alone():
    fake = FakeGitHub(
        canton={1: (100, "2026-10-02T10:00:00Z", "open")},
        delivery=_delivery((526, "Canton Flaky Tests 2026-10", True)),
        parents={1: 999},
    )
    assert _run(fake) == []
    assert fake.writes == []


def test_old_closed_issues_are_skipped_but_recent_closed_ones_are_linked():
    fake = FakeGitHub(
        canton={
            1: (100, "2026-08-31T23:00:00Z", "closed"),  # before the start of last month
            2: (200, "2026-09-01T00:00:00Z", "closed"),
            3: (300, "2025-06-01T00:00:00Z", "open"),  # old but still open
        },
        delivery=_delivery(),
    )
    assert _run(fake) == []
    linked = {
        issue_id
        for kind, parent, issue_id in fake.writes
        if kind == "attach" and parent != TRACKING_ISSUE
    }
    assert linked == {200, 300}, fake.writes


def test_one_failing_attach_does_not_stop_the_others_and_is_reported():
    fake = FakeGitHub(
        canton={1: (100, "2026-10-02T10:00:00Z", "open"), 2: (200, "2026-10-03T10:00:00Z", "open")},
        delivery=_delivery((526, "Canton Flaky Tests 2026-10", True)),
        fail_attach={100},
    )
    failed = _run(fake)
    assert len(failed) == 1 and failed[0].startswith("#1:"), failed
    assert fake.writes == [("attach", 526, 200)]


def test_dry_run_writes_nothing_but_names_the_month_it_would_create():
    fake = FakeGitHub(canton={1: (100, "2026-09-02T10:00:00Z", "open")}, delivery=_delivery())
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        assert _run(fake, dry_run=True) == []
    assert fake.writes == []
    # A dry run is how a new delivery issue gets reviewed before a real run creates it.
    assert "would create Canton Flaky Tests 2026-09 and link 1 issue(s)" in out.getvalue()


def test_a_full_month_continues_in_a_second_part():
    # GitHub refuses a 101st sub-issue, so without parts a busy month fails every day.
    delivery = _delivery((526, "Canton Flaky Tests 2026-10", True))
    delivery[526]["children"] = list(range(1, MAX_SUB_ISSUES))  # one slot left
    fake = FakeGitHub(
        canton={1: (100, "2026-10-02T10:00:00Z", "open"), 2: (200, "2026-10-03T10:00:00Z", "open")},
        delivery=delivery,
    )
    assert _run(fake) == []
    assert fake.writes == [
        ("attach", 526, 100),
        ("create", 527, "Canton Flaky Tests 2026-10 (2)"),
        ("attach", TRACKING_ISSUE, 5270),
        ("attach", 527, 200),
    ]


def test_an_unattached_month_older_than_one_page_is_still_found():
    # A month left unattached by a failed run can stay so while delivery gets new issues. It must
    # still be found, or the month is created a second time.
    delivery = _delivery((600, "Canton Flaky Tests 2026-09", False))
    for number in range(601, 751):
        delivery[number] = {"id": number * 10, "title": f"Unrelated {number}", "children": []}
    fake = FakeGitHub(canton={1: (100, "2026-09-02T10:00:00Z", "open")}, delivery=delivery)
    assert _run(fake) == []
    assert fake.writes == [("attach", TRACKING_ISSUE, 6000), ("attach", 600, 100)]


def test_a_month_listed_twice_uses_the_lowest_and_counts_both_as_linked():
    # A copy attached by hand must not split the month, and its children are linked already.
    title = "Canton Flaky Tests 2026-10"
    delivery = _delivery((526, title, True), (530, title, True))
    delivery[530]["children"] = [100]
    fake = FakeGitHub(
        canton={1: (100, "2026-10-02T10:00:00Z", "open"), 2: (200, "2026-10-03T10:00:00Z", "open")},
        delivery=delivery,
    )
    assert _run(fake) == []
    assert fake.writes == [("attach", 526, 200)]
    assert fake.parent_lookups == 1, fake.parent_lookups  # only #2, #1 counts as linked


def test_a_month_that_cannot_be_created_does_not_stop_the_other_months():
    fake = FakeGitHub(
        canton={1: (100, "2026-09-02T10:00:00Z", "open"), 2: (200, "2026-10-03T10:00:00Z", "open")},
        delivery=_delivery((526, "Canton Flaky Tests 2026-10", True)),
        fail_create={"Canton Flaky Tests 2026-09"},
    )
    failed = _run(fake)
    assert len(failed) == 1 and failed[0].startswith("Canton Flaky Tests 2026-09:"), failed
    assert fake.writes == [("attach", 526, 200)]


def test_the_error_annotation_keeps_every_failing_issue():
    # A workflow command ends at the first newline, so an unencoded list would show only its
    # first line in the run's annotations.
    fake = FakeGitHub(
        canton={1: (100, "2026-10-02T10:00:00Z", "open"), 2: (200, "2026-10-03T10:00:00Z", "open")},
        delivery=_delivery((526, "Canton Flaky Tests 2026-10", True)),
        fail_attach={100, 200},
    )
    out = io.StringIO()
    with (
        patch(f"{__name__}.run_gh_with_retries", side_effect=fake),
        contextlib.redirect_stdout(out),
    ):
        assert main([]) == 1
    (annotation,) = [line for line in out.getvalue().splitlines() if line.startswith("::error::")]
    assert "#1:" in annotation and "#2:" in annotation, annotation


def test_start_of_last_month():
    assert start_of_last_month(dt.date(2026, 10, 26)) == dt.date(2026, 9, 1)
    assert start_of_last_month(dt.date(2026, 1, 1)) == dt.date(2025, 12, 1)


if __name__ == "__main__":
    sys.exit(main())
