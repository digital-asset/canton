"""Shared helpers for the flaky-test CI pipeline.

Holds the code the pipeline scripts share: CI-environment detection, the `gh`
wrapper, test-name and issue-table formatting, the project and field config, the
JSON artifact contracts, and a small self-test harness. Each script imports the
names it needs (`from flaky_common import ...`).
"""

import os
import re
import io
import subprocess
import datetime
import time
import json
import tempfile
import contextlib
from typing import Final, Optional
from unittest.mock import patch

metric_short_version = "canton.failed_test_grouped"

# The number of consecutive git commits a test must fail on before triggering a Slack alert.
# Set to 3 to avoid alerting on single transient failures or manual CircleCI job retries.
# Set to 10 for unstable tests. The trade off is about false alarms vs not detecting broken test.
CONSECUTIVE_FAILURES_THRESHOLD: Final[int] = 3
CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE: Final[int] = 10

# A test that fails on this many consecutive *nightly* runs is treated as broken
# (not merely flaky) and the CI rota is pinged. Nightly runs are days apart and
# land on non-adjacent commits, so the per-commit streak check never fires for
# them; this is the nightly-specific equivalent.
NIGHTLY_CONSECUTIVE_FAILURES: Final[int] = 3
# Label applied to the tracking issue when a nightly streak is detected.
NIGHTLY_BROKEN_LABEL: Final[str] = "broken-nightly"
# Nightly cron from .circleci/config/workflows/canton_nightly.yml: "0 22 * * 1-5".
NIGHTLY_CRON_HOUR_UTC: Final[int] = 22
# Nightly jobs that keep a historical name instead of the `nightly_` prefix. Matched
# on the job name (CIRCLE_JOB / GITHUB_JOB), so keep in sync across a GHA migration.
NIGHTLY_JOBS_WITHOUT_PREFIX: Final[frozenset[str]] = frozenset(
    {
        "toxiproxy_test_slow_36",
        "external_parties_test_36",
        "test_36_with_java17",
        "test_37",
        "protocol_continuity_test_all",
        "crash_recovery_test_35",
        "crash_recovery_test_37",
        "postgres_conformance_test_14",
        "postgres_conformance_test_15",
        "postgres_conformance_test_16",
        "postgres_conformance_test_17",
        "postgres_conformance_test_18",
        "benchmark",
        "topology_chaos_test_36",
        "stability_crash_recovery_test_36",
        "ledger_api_compatibility_test_36",
        "model_based_test_36",
    }
)

milestone = "Flaky Tests"  # flaky tests milestone M97 (milestone number 31)
flaky_test_project = "PVT_kwDOAJX-Fc4AbncN"  # https://github.com/orgs/DACH-NY/projects/38/

release_line_field = "PVTSSF_lADOAJX-Fc4AbncNzgcWE1k"  # ID of the custom field "Release Line"

# The value of this dictionary points to the ID of the field value of the `Release Line` field in the
# flaky test kanban board.
# If you add a new field, execute listReleaseLineFields.graphql to find the new ID
branches_to_report = {
    "main": "733298b6",  # ID of the value "main" for the Release Line field in the project
    "main-2.x": "073b4df3",  # ID of the value "main-2.x" for the Release Line field in the project
}

# At most this many failing tests are reported per CI run: a fundamental problem
# can fail everything, which should not become hundreds of issues. The first ones
# in sorted order are reported (manage_flaky_issues) and get evidence
# (collect_flake_evidence).
FAILING_TESTS_MAX: Final[int] = 20

GH_RETRY_ATTEMPTS: Final[int] = 3
GH_RETRY_DELAY_SECONDS: Final[int] = 5
TRANSIENT_HTTP_CODES: Final[frozenset] = frozenset({"502", "503", "504"})


# gha-migration: changed getting branch name, build url, node index, job name
# and commit hash to account for CircleCI and GHA
def is_github_actions_ci() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "").lower() == "true"


def is_circle_ci() -> bool:
    return os.environ.get("CIRCLECI", "").lower() == "true"


def get_ci_branch() -> str:
    if is_github_actions_ci():
        return os.environ.get("GITHUB_HEAD_REF") or os.environ.get("GITHUB_REF_NAME", "")
    if is_circle_ci():
        return os.environ.get("CIRCLE_BRANCH", "")
    return os.environ.get("CIRCLE_BRANCH", "")


def get_ci_build_url() -> str:
    if is_github_actions_ci():
        server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        repository = os.environ.get("GITHUB_REPOSITORY", "")
        run_id = os.environ.get("GITHUB_RUN_ID", "")
        run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "")
        if repository and run_id:
            url = f"{server}/{repository}/actions/runs/{run_id}"
            # Include the attempt so the link points at the run where the test
            # actually failed. A bare runs/<id> URL resolves to the latest
            # attempt, which after an auto-rerun is usually the green one and
            # hides the failure.
            return f"{url}/attempts/{run_attempt}" if run_attempt else url
        return ""
    if is_circle_ci():
        return os.environ.get("CIRCLE_BUILD_URL", "")
    return os.environ.get("CIRCLE_BUILD_URL", "")


def get_ci_node_index() -> str:
    if is_github_actions_ci():
        return os.environ.get("MATRIX_SHARD", "0")
    if is_circle_ci():
        return os.environ.get("CIRCLE_NODE_INDEX", "0")
    return os.environ.get("CIRCLE_NODE_INDEX", "0")


def get_ci_job_name() -> str:
    if is_circle_ci():
        return os.environ.get("CIRCLE_JOB", "unknown")
    if is_github_actions_ci():
        return os.environ.get("CANTON_CI_JOB_NAME") or os.environ.get("GITHUB_JOB", "unknown")
    return os.environ.get("CIRCLE_JOB", "unknown")


def get_ci_commit_hash() -> str:
    if is_github_actions_ci():
        return os.environ.get("GITHUB_SHA", "unknown")
    if is_circle_ci():
        return os.environ.get("CIRCLE_SHA1", "unknown")
    return os.environ.get("CIRCLE_SHA1", "unknown")


def get_ci_parallel_run_url(build_url: str, node_index: str) -> str:
    if is_github_actions_ci():
        # On GHA every matrix shard is its own job, so the job's own page points
        # straight at the shard that failed. Resolve it, falling back to the
        # run-attempt `build_url` when the job id can't be determined.
        return resolve_gha_job_url(build_url)
    if is_circle_ci():
        return f"{build_url.replace('circleci.com/gh/', 'app.circleci.com/jobs/github/')}/parallel-runs/{node_index}"
    return f"{build_url.replace('circleci.com/gh/', 'app.circleci.com/jobs/github/')}/parallel-runs/{node_index}"


branch = get_ci_branch()

# replace GITHUB_TOKEN with GITHUB_FLAKY_TEST_TOKEN, which also is permitted to use the project scope.
# this is required to use the gh command to unarchive/restore issues
gh_flaky_test_env = os.environ.copy()
if "GITHUB_FLAKY_TEST_TOKEN" in gh_flaky_test_env:
    gh_flaky_test_env["GITHUB_TOKEN"] = os.environ["GITHUB_FLAKY_TEST_TOKEN"]


def is_dry_run() -> bool:
    """FLAKY_DRY_RUN=1: no GitHub or Datadog calls, on any branch. The evidence comments
    are written to <artifact dir>/comments/ and the job summary instead."""
    return os.environ.get("FLAKY_DRY_RUN", "") == "1"


def should_report_issues():
    return branch in branches_to_report


def is_nightly_job(job: str) -> bool:
    """`nightly_*` jobs plus NIGHTLY_JOBS_WITHOUT_PREFIX, from canton_nightly.yml.

    The `unstable_*` jobs run nightly too but are allowed to fail, so they are
    excluded: broken-nightly is for genuinely broken tests, not unstable ones.
    """
    return bool(job) and (job.startswith("nightly_") or job in NIGHTLY_JOBS_WITHOUT_PREFIX)


def is_unstable_job(job: str) -> bool:
    """`unstable_test` and `unstable_test_slow` run the intentionally-flaky
    UnstableTest lanes (see .circleci/config/jobs/@test.yml).

    They use the lenient CONSECUTIVE_FAILURES_THRESHOLD_UNSTABLE, so match on the
    `unstable_test` prefix rather than the exact name, otherwise `unstable_test_slow`
    silently inherits the strict per-commit threshold and alerts far too eagerly.
    """
    return bool(job) and job.startswith("unstable_test")


# --- gh CLI wrapper -------------------------------------------------------


def is_transient_gh_error(result) -> bool:
    combined = result.stderr + result.stdout
    return any(code in combined for code in TRANSIENT_HTTP_CODES)


def run_gh_with_retries(
    args: list, attempts: int = GH_RETRY_ATTEMPTS, stdin: Optional[str] = None
) -> subprocess.CompletedProcess:
    # Linux caps a single arg at 128 KB, so large bodies have to be passed via stdin (using `--body-file -`).
    result = subprocess.run(
        ["gh"] + args, input=stdin, capture_output=True, text=True, env=gh_flaky_test_env
    )
    if result.returncode != 0 and is_transient_gh_error(result) and attempts > 1:
        print(
            f"Transient gh error (attempt {GH_RETRY_ATTEMPTS - attempts + 1}/{GH_RETRY_ATTEMPTS}), retrying in {GH_RETRY_DELAY_SECONDS}s..."
        )
        time.sleep(GH_RETRY_DELAY_SECONDS)
        return run_gh_with_retries(args, attempts - 1, stdin)
    return result


# Resolved once per process. The repository/run/attempt/runner tuple is constant
# for a job's lifetime, but create_issue_table_row runs once per failing test
# (up to failing_tests_max), so without this the paginated jobs API would be hit
# once per row. `None` means "not resolved yet". _reset_gha_job_url_cache lets the
# self-tests exercise each scenario from a clean slate.
_gha_job_url_cache: Optional[str] = None
# Number of the job's step that is running the flaky report, see get_ci_log_url.
_gha_job_step_cache: Optional[int] = None


def _reset_gha_job_url_cache() -> None:
    global _gha_job_url_cache, _gha_job_step_cache
    _gha_job_url_cache = None
    _gha_job_step_cache = None


def resolve_gha_job_url(fallback_url: str) -> str:
    """Resolve the current GHA job's own page URL so the flaky-issue Build link
    opens the exact job (and therefore shard) that failed, not just the run
    summary. Each matrix shard is a separate job, so its `html_url` points
    straight at that shard.

    The numeric job id lives in no environment variable, so we ask the Actions
    API for the jobs of this run attempt and pick the one currently running on
    this runner. The flaky report is a step of the failing job itself, so that
    job is the only `in_progress` one on this runner even if the runner was
    reused earlier in the attempt, which makes the match unambiguous without
    relying on the order the API lists jobs in.

    Any failure to resolve (missing token, API error, no match) falls back to
    `fallback_url` (the run-attempt URL), so the link is never worse than
    before. We log when that happens so it is visible in the CI step output.
    """
    global _gha_job_url_cache, _gha_job_step_cache
    if _gha_job_url_cache is None:
        _gha_job_url_cache, _gha_job_step_cache = _resolve_gha_job_uncached(fallback_url)
    return _gha_job_url_cache


def get_ci_log_url(build_url: str, node_index: str) -> str:
    """Link to the CI log of the step that ran the failing tests.

    On GHA the flaky report runs inside the test step itself (cleanup_test_job
    is part of the run_tests composite), so the job's in-progress step is the
    one whose log holds the failure: link it as <job url>#step:<n>. Without a
    step number this is the job link of the issue table.
    """
    job_url = get_ci_parallel_run_url(build_url, node_index)
    if is_github_actions_ci() and _gha_job_step_cache and "/job/" in job_url:
        return f"{job_url}#step:{_gha_job_step_cache}"
    return job_url


def _resolve_gha_job_uncached(fallback_url: str) -> tuple[str, Optional[int]]:
    """(job url, number of its in-progress step); the step is None if unknown."""
    if is_dry_run():
        return fallback_url, None
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "")
    runner_name = os.environ.get("RUNNER_NAME", "")
    if not (repository and run_id and run_attempt and runner_name):
        print(
            "Cannot resolve GHA job URL (missing repository/run id/run attempt/runner "
            f"name), falling back to run URL {fallback_url}"
        )
        return fallback_url, None
    result = run_gh_with_retries(
        [
            "api",
            f"/repos/{repository}/actions/runs/{run_id}/attempts/{run_attempt}/jobs",
            "--paginate",
            "--jq",
            f'.jobs[] | select(.runner_name == "{runner_name}" and .status == "in_progress")'
            ' | "\\(.html_url) \\([.steps[]? | select(.status == "in_progress") | .number]'
            ' | first // "")"',
        ]
    )
    if result.returncode != 0:
        print(
            f"Could not resolve GHA job URL (gh api failed: {result.stderr.strip()}), "
            f"falling back to run URL {fallback_url}"
        )
        return fallback_url, None
    urls = [line for line in result.stdout.splitlines() if line.strip()]
    if not urls:
        print(
            f"Could not resolve GHA job URL (no in-progress job matched runner {runner_name}), "
            f"falling back to run URL {fallback_url}"
        )
        return fallback_url, None
    url, _, step = urls[0].partition(" ")
    return url, int(step) if step.strip().isdigit() else None


def check_result(result):
    if result.returncode != 0:
        print("### ERROR while executing gh command!")
        print(f"Command was {result.args}")
        print(f"stderr was {result.stderr}")
        print(f"stdout was {result.stdout}")
        raise Exception("gh command failed")


# --- formatting -----------------------------------------------------------


# Datadog only accepts ASCII characters in their tag names
def remove_non_ascii_characters(test_name: str):
    chars = []
    for char in test_name:
        if ord(char) >= 128:
            print(f"Non-ASCII character {char} found in test named {test_name}. Converting to '_'")
            char = '_'
        chars.append(char)
    return ''.join(chars)


def remove_everything_after_first_slash(test_name: str) -> str:
    """
    Useful for flaky tests with variable context, e.g.:
    WARN c.d.c.r.DbStorageSingle:BftOrderingGetConnectedSynchronizersIntegrationTest/sequencerx=sequencer1
    WARN c.d.c.r.DbStorageSingle:BftOrderingGetConnectedSynchronizersIntegrationTest/sequencerx=sequencer2
    will be turned into:
    WARN c.d.c.r.DbStorageSingle:BftOrderingGetConnectedSynchronizersIntegrationTest
    """
    if "/" in test_name:
        return test_name.split("/")[0]
    return test_name


def format_issue_title(test_name: str) -> str:
    return f"[{branch}] Flaky {format_test_name(test_name)}"


def format_test_name(test_name: str):
    test_name = remove_everything_after_first_slash(remove_non_ascii_characters(test_name))
    # Collapse shard partitions so all `FooShard0Test`, `FooShard1Test`, ... map to the same
    # name, since they are the same logical test split across parallel CI runners.
    test_name = re.sub(r'Shard\d+', '', test_name)
    if len(test_name) > 200:
        print(f"Truncating {test_name} to 200 characters.")
    return test_name[:200]


# Cause bucket recorded alongside each failing test and rendered in the issue
# table's Cause column. "regular" is a JUnit failure/error, "timeout" a watchdog
# kill (see generate_watchdog_xml.py), and warn_other/error_other come from the
# found_problems.txt log-check path. Also the fallback for pre-Cause artifacts.
DEFAULT_CAUSE: Final[str] = "regular"


def create_issue_table_header():
    return (
        "| Date | Job | Shard | Build | Commit | Evidence | Cause |\n|---|---|---|---|---|---|---|"
    )


def get_ci_commit_url(commit_hash: str) -> str:
    if is_github_actions_ci():
        repo_owner = os.environ.get('GITHUB_REPOSITORY', 'DACH-NY/canton').split('/', 1)[0]
    else:
        repo_owner = os.environ.get('CIRCLE_PROJECT_USERNAME', 'DACH-NY')
    return f"https://github.com/{repo_owner}/canton/commit/{commit_hash}"


def with_evidence_link(row: str, evidence_url: Optional[str], label: str = "evidence") -> str:
    """Fills the Evidence cell of a row built by create_issue_table_row with a link to
    the failure's evidence comment, e.g. `[2 failing tests](...)`. The Evidence cell
    is the second to last one."""
    if not evidence_url:
        return row
    head, _evidence, cause, tail = row.rsplit("|", 3)
    return f"{head}| [{label}]({evidence_url}) |{cause}|{tail}"


def create_issue_table_row(
    cause: str = DEFAULT_CAUSE, evidence_url: Optional[str] = None, evidence_label: str = "evidence"
):
    date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit_hash = get_ci_commit_hash()
    commit_url = get_ci_commit_url(commit_hash)
    build_url = get_ci_build_url()
    if is_github_actions_ci():
        # Derive the label from the run id directly. The URL now ends in
        # /attempts/<n>, so taking its last path segment would show the attempt
        # number instead of the run id.
        build_number = os.environ.get('GITHUB_RUN_ID', '')
    else:
        build_number = build_url.rstrip('/').rsplit('/', 1)[-1]
    job = get_ci_job_name()
    node_index = get_ci_node_index()
    # On GHA this points at the failing test step of the job (#step:<n>), see get_ci_log_url.
    log_url = get_ci_log_url(build_url, node_index)
    row = f"| {date_str} | {job} | {node_index} | [{build_number}]({log_url}) | [{commit_hash[:8]}]({commit_url}) |  | {cause} |"
    return with_evidence_link(row, evidence_url, evidence_label)


# --- JSON artifact contracts ---------------------------------------------
#
# The pipeline steps pass data via JSON files under a shared directory so each
# concern is its own process. Paths default to a CI temp dir and can be
# overridden per step with --failing-tests-json / --streaks-json.

ARTIFACT_VERSION: Final[int] = 2


def artifact_dir() -> str:
    return (
        os.environ.get("FLAKY_ARTIFACT_DIR")
        or os.environ.get("RUNNER_TEMP")
        or tempfile.gettempdir()
    )


def failing_tests_path() -> str:
    return os.path.join(artifact_dir(), "failing_tests.json")


def streaks_path() -> str:
    return os.path.join(artifact_dir(), "streaks.json")


def evidence_path() -> str:
    return os.path.join(artifact_dir(), "evidence.json")


def write_failing_tests(path: str, tests, causes=None) -> None:
    """Writes the failing tests plus their causes to the JSON artifact.

    `tests` may be a plain iterable of names or a {name: cause} mapping. In the
    latter case the causes are taken from it unless an explicit `causes` map is
    passed. Names without a cause default to DEFAULT_CAUSE so every failing test
    always carries one. `failing_tests` stays a sorted name list for the datadog
    and report_failing_tests readers that only care about names.
    """
    if causes is None and isinstance(tests, dict):
        causes = tests
    causes = causes or {}
    names = sorted(set(tests))
    cause_map = {name: causes.get(name, DEFAULT_CAUSE) for name in names}
    with open(path, "w") as f:
        json.dump(
            {"version": ARTIFACT_VERSION, "failing_tests": names, "causes": cause_map},
            f,
            indent=2,
        )
    print(f"Wrote {len(names)} failing test(s) to {path}")


def read_failing_tests(path: str) -> list:
    if not path or not os.path.exists(path):
        print(f"No failing-tests artifact at {path}; treating as empty.")
        return []
    with open(path) as f:
        return list(json.load(f).get("failing_tests", []))


def read_failing_test_causes(path: str) -> dict:
    """Returns a {test_name: cause} map from the failing-tests artifact.

    Missing entries and pre-Cause artifacts (version 1) fall back to
    DEFAULT_CAUSE, so callers always get a cause for every failing test.
    """
    if not path or not os.path.exists(path):
        return {}
    with open(path) as f:
        data = json.load(f)
    stored = data.get("causes", {})
    return {name: stored.get(name, DEFAULT_CAUSE) for name in data.get("failing_tests", [])}


def write_streaks(path: str, streaks) -> None:
    """`streaks` is an iterable of (issue_id, title, commit_hash) tuples."""
    payload = [{"issue_id": i, "title": t, "commit_hash": h} for (i, t, h) in streaks]
    with open(path, "w") as f:
        json.dump({"version": ARTIFACT_VERSION, "streaks": payload}, f, indent=2)
    print(f"Wrote {len(payload)} streak(s) to {path}")


def read_streaks(path: str) -> list:
    """Returns a list of (issue_id, title, commit_hash) tuples."""
    if not path or not os.path.exists(path):
        print(f"No streaks artifact at {path}; treating as empty.")
        return []
    with open(path) as f:
        data = json.load(f)
    return [(s["issue_id"], s["title"], s["commit_hash"]) for s in data.get("streaks", [])]


# --- env validation + self-test harness ----------------------------------


def ci_required_context_vars() -> list:
    if is_github_actions_ci():
        return ['GITHUB_SHA', 'GITHUB_JOB', 'GITHUB_RUN_ID', 'GITHUB_REPOSITORY', 'MATRIX_SHARD']
    elif is_circle_ci():
        return ['CIRCLE_SHA1', 'CIRCLE_BUILD_URL', 'CIRCLE_JOB', 'CIRCLE_NODE_INDEX']
    else:
        # Keep backward compatibility in unknown CI contexts with CircleCI-style vars.
        return ['CIRCLE_SHA1', 'CIRCLE_BUILD_URL', 'CIRCLE_JOB', 'CIRCLE_NODE_INDEX']


def assert_required_env_vars(required: list):
    """Ensures the given environment variables are present before execution."""
    missing = [var for var in required if not os.environ.get(var)]
    if missing:
        raise RuntimeError(
            f"Cannot execute CI script. Missing required environment variables: {', '.join(missing)}"
        )


def run_guarded_self_test(self_test_fn) -> None:
    """Run a script's self_test() and fail loudly if it leaked os.environ changes."""
    env_before = dict(os.environ)
    try:
        self_test_fn()
    finally:
        env_after = dict(os.environ)
        if env_before != env_after:
            leaked = {
                k: (env_before.get(k), env_after.get(k))
                for k in env_before.keys() | env_after.keys()
                if env_before.get(k) != env_after.get(k)
            }
            raise RuntimeError(f"self_test() polluted os.environ (before -> after): {leaked}")


def self_test():
    """Self-checks for the shared helpers; each pipeline script runs this too."""
    test_gh_flags()
    test_format_test_name_collapses_shards()
    test_create_issue_table_row()
    test_get_ci_log_url_links_the_running_step()
    test_is_nightly_job()
    test_is_unstable_job()
    test_ci_environment_detection()
    test_get_ci_branch()
    test_get_ci_job_name_and_defaults()
    test_get_ci_commit_url()
    test_format_test_name_sanitizes()
    test_format_issue_title_and_should_report_issues()
    test_with_evidence_link()
    test_is_transient_gh_error()
    test_run_gh_with_retries()
    test_check_result()
    test_artifact_dir()
    test_required_env_vars()
    print("flaky_common self-checks passed")


def test_format_test_name_collapses_shards():
    # Shard partitions of the same logical test must collapse to the same name
    assert (
        format_test_name("LedgerApiShard0ConformanceTestPostgres")
        == "LedgerApiConformanceTestPostgres"
    )
    assert (
        format_test_name("LedgerApiShard5ConformanceTestPostgres")
        == "LedgerApiConformanceTestPostgres"
    )
    # Two-digit shard indices
    assert format_test_name("LedgerApiShard10ConformanceTest") == "LedgerApiConformanceTest"
    # Non-shard names are unchanged
    assert format_test_name("RegularFlakyTest") == "RegularFlakyTest"
    # "Shard" without a digit suffix must not be stripped
    assert format_test_name("ShardingLayerTest") == "ShardingLayerTest"


def test_gh_flags():
    checks = [
        (["issue", "create", "--help"], ["--title", "--body-file", "--milestone", "--repo"]),
        (["issue", "edit", "--help"], ["--title", "--body-file", "--repo"]),
        (["issue", "reopen", "--help"], ["--repo"]),
    ]
    for subcommand, flags in checks:
        result = subprocess.run(["gh"] + subcommand, capture_output=True, text=True)
        help_text = result.stdout + result.stderr
        for flag in flags:
            assert flag in help_text, (
                f"`gh {' '.join(subcommand[:-1])}` help does not mention flag `{flag}` — was it renamed?"
            )


def test_get_ci_log_url_links_the_running_step():
    env = {
        'CIRCLECI': 'false',
        'GITHUB_ACTIONS': 'true',
        'GITHUB_SHA': 'a' * 40,
        'GITHUB_JOB': 'test',
        'MATRIX_SHARD': '0',
        'GITHUB_RUN_ID': '1',
        'GITHUB_RUN_ATTEMPT': '1',
        'GITHUB_REPOSITORY': 'DACH-NY/canton',
        'GITHUB_SERVER_URL': 'https://github.com',
        'RUNNER_NAME': 'r1',
    }
    job_url = 'https://github.com/DACH-NY/canton/actions/runs/1/job/2'
    for stdout, expected in [
        (f'{job_url} 6\n', f'{job_url}#step:6'),
        # No step number from the API: the plain job link.
        (f'{job_url} \n', job_url),
        (f'{job_url}\n', job_url),
    ]:
        _reset_gha_job_url_cache()
        ok = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr='')
        with (
            patch.dict(os.environ, env, clear=True),
            patch(f'{__name__}.run_gh_with_retries', return_value=ok),
        ):
            assert get_ci_log_url(get_ci_build_url(), '0') == expected, (stdout, expected)
            # The table's Build link points at the same place.
            assert f'[1]({expected}) |' in create_issue_table_row(), create_issue_table_row()
    _reset_gha_job_url_cache()


def test_create_issue_table_row():
    # Test CircleCI (DACH-NY)
    env_circleci_dach = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_JOB': 'test_with_java17',
        'CIRCLE_NODE_INDEX': '5',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/DACH-NY/canton/3196076',
        'CIRCLE_SHA1': 'a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2',
        'CIRCLE_PROJECT_USERNAME': 'DACH-NY',
    }
    with patch.dict(os.environ, env_circleci_dach, clear=False):
        header = create_issue_table_header()
        assert (
            header.splitlines()[0] == '| Date | Job | Shard | Build | Commit | Evidence | Cause |'
        ), f"Missing Evidence/Cause columns in header: {header}"
        assert header.splitlines()[1] == '|---|---|---|---|---|---|---|', (
            f"Separator row must match the seven-column header: {header}"
        )
        line = create_issue_table_row("timeout")
        assert line.endswith('| timeout |'), f"Missing cause cell in: {line}"
        linked = create_issue_table_row(
            "timeout", "https://github.com/x/issues/1#issuecomment-2", "2 failing tests"
        )
        assert linked.endswith(
            '| [2 failing tests](https://github.com/x/issues/1#issuecomment-2) | timeout |'
        ), f"Missing evidence cell in: {linked}"
        assert line.endswith('|  | timeout |'), f"Rows without evidence keep an empty cell: {line}"
        assert line.count('|') == linked.count('|') == 8, (line, linked)
        assert create_issue_table_row().endswith(f'| {DEFAULT_CAUSE} |'), (
            "create_issue_table_row() must default the cause cell"
        )
        assert '| test_with_java17 |' in line, f"Missing job in: {line}"
        assert '| 5 |' in line, f"Missing node_index in: {line}"
        assert (
            '[3196076](https://app.circleci.com/jobs/github/DACH-NY/canton/3196076/parallel-runs/5)'
            in line
        ), f"Missing parallel-run build link in: {line}"
        assert (
            '[a1b2c3d4](https://github.com/DACH-NY/canton/commit/a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2)'
            in line
        ), f"Missing commit in: {line}"

    # Test CircleCI (OSS digital-asset)
    env_circleci_oss = {
        'CIRCLECI': 'true',
        'GITHUB_ACTIONS': 'false',
        'CIRCLE_JOB': 'test_protocol_version_dev',
        'CIRCLE_NODE_INDEX': '6',
        'CIRCLE_BUILD_URL': 'https://circleci.com/gh/digital-asset/canton/1491',
        'CIRCLE_SHA1': 'b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3',
        'CIRCLE_PROJECT_USERNAME': 'digital-asset',
    }
    with patch.dict(os.environ, env_circleci_oss, clear=False):
        line = create_issue_table_row()
        assert (
            '[1491](https://app.circleci.com/jobs/github/digital-asset/canton/1491/parallel-runs/6)'
            in line
        ), f"Missing OSS canton build link in: {line}"
        assert (
            '[b2c3d4e5](https://github.com/digital-asset/canton/commit/b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3)'
            in line
        ), f"Missing OSS canton commit link in: {line}"

    # Test GitHub Actions
    env_gha = {
        'CIRCLECI': 'false',
        'GITHUB_ACTIONS': 'true',
        'GITHUB_JOB': 'integration-tests-shard-2',
        'MATRIX_SHARD': '2',
        'GITHUB_RUN_ID': '9876543210',
        'GITHUB_RUN_ATTEMPT': '2',
        'GITHUB_REPOSITORY': 'DACH-NY/canton',
        'GITHUB_SERVER_URL': 'https://github.com',
        'GITHUB_SHA': 'c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4',
    }
    # With a runner name and a resolvable job, the Build link points at the
    # specific job (and therefore shard), while the label stays the run id. A
    # second row in the same process reuses the resolved URL without asking the
    # jobs API again, so a job with many failing tests spends one API call.
    job_url = 'https://github.com/DACH-NY/canton/actions/runs/9876543210/job/555000111'
    ok = subprocess.CompletedProcess(args=[], returncode=0, stdout=job_url + '\n', stderr='')
    _reset_gha_job_url_cache()
    with (
        patch.dict(os.environ, {**env_gha, 'RUNNER_NAME': 'canton-runner-abc'}, clear=True),
        patch(f'{__name__}.run_gh_with_retries', return_value=ok) as resolve_mock,
    ):
        line = create_issue_table_row()
        create_issue_table_row()
        assert resolve_mock.call_count == 1, (
            f"Expected the job URL to be resolved once per process, not {resolve_mock.call_count}x"
        )
        assert '| integration-tests-shard-2 |' in line, f"Missing GHA job name in: {line}"
        assert '| 2 |' in line, f"Missing GHA matrix shard in: {line}"
        assert f'[9876543210]({job_url})' in line, f"Missing GHA per-job build link in: {line}"
        assert (
            '[c3d4e5f6](https://github.com/DACH-NY/canton/commit/c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4)'
            in line
        ), f"Missing GHA commit in: {line}"

    # When the job id can't be resolved (API error), fall back to the
    # run-attempt URL so the link survives an auto-rerun and is never worse.
    failed = subprocess.CompletedProcess(args=[], returncode=1, stdout='', stderr='HTTP 500')
    _reset_gha_job_url_cache()
    with (
        patch.dict(os.environ, {**env_gha, 'RUNNER_NAME': 'canton-runner-abc'}, clear=True),
        patch(f'{__name__}.run_gh_with_retries', return_value=failed),
    ):
        line = create_issue_table_row()
        assert (
            '[9876543210](https://github.com/DACH-NY/canton/actions/runs/9876543210/attempts/2)'
            in line
        ), f"Expected fallback to run-attempt URL on resolve failure, got: {line}"

    # Without a runner name we can't resolve a job, so fall back the same way
    # (and make no API call).
    _reset_gha_job_url_cache()
    with patch.dict(os.environ, env_gha, clear=True):
        line = create_issue_table_row()
        assert (
            '[9876543210](https://github.com/DACH-NY/canton/actions/runs/9876543210/attempts/2)'
            in line
        ), f"Expected fallback to run-attempt URL without a runner name, got: {line}"

    # GHA without a run attempt in the environment falls back to the bare run URL.
    env_gha_no_attempt = {k: v for k, v in env_gha.items() if k != 'GITHUB_RUN_ATTEMPT'}
    with patch.dict(os.environ, env_gha_no_attempt, clear=True):
        assert get_ci_build_url() == 'https://github.com/DACH-NY/canton/actions/runs/9876543210', (
            f"Expected bare run URL without attempt, got: {get_ci_build_url()}"
        )
    with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}, clear=True):
        assert get_ci_build_url() == '', "GHA without repository/run id has no build URL"


def test_is_nightly_job():
    assert is_nightly_job("nightly_integration_test")
    assert is_nightly_job("nightly_test_upgrades_matrix")
    assert is_nightly_job("toxiproxy_test_slow_36")  # nightly, keeps its historical name
    assert is_nightly_job("external_parties_test_36")  # nightly, keeps its historical name
    assert is_nightly_job("test_36_with_java17")  # nightly, keeps its historical name
    assert is_nightly_job("test_37")  # nightly, keeps its historical name
    assert is_nightly_job("protocol_continuity_test_all")  # nightly, keeps its historical name
    assert is_nightly_job("crash_recovery_test_35")  # nightly, keeps its historical name
    assert is_nightly_job("crash_recovery_test_37")  # nightly, keeps its historical name
    assert is_nightly_job("postgres_conformance_test_14")  # nightly, keeps its historical name
    assert is_nightly_job("postgres_conformance_test_15")  # nightly, keeps its historical name
    assert is_nightly_job("postgres_conformance_test_16")  # nightly, keeps its historical name
    assert is_nightly_job("postgres_conformance_test_17")  # nightly, keeps its historical name
    assert is_nightly_job("postgres_conformance_test_18")  # nightly, keeps its historical name
    assert is_nightly_job("benchmark")  # nightly, keeps its historical name
    assert is_nightly_job("topology_chaos_test_36")  # nightly, keeps its historical name
    assert is_nightly_job("stability_crash_recovery_test_36")  # nightly, keeps its historical name
    assert is_nightly_job("ledger_api_compatibility_test_36")  # nightly, keeps its historical name
    assert is_nightly_job("model_based_test_36")  # nightly, keeps its historical name
    assert not is_nightly_job("toxiproxy_test_fast")  # runs per-commit, not nightly
    assert not is_nightly_job("unstable_test")  # intentionally unstable, excluded
    assert not is_nightly_job("unstable_test_slow")  # intentionally unstable, excluded
    assert not is_nightly_job("")


def test_is_unstable_job():
    assert is_unstable_job("unstable_test")
    assert is_unstable_job("unstable_test_slow")  # the prefix must catch the slow lane too
    assert not is_unstable_job("test_with_java17")
    assert not is_unstable_job("nightly_integration_test")
    assert not is_unstable_job("")


def test_ci_environment_detection():
    with patch.dict(os.environ, {'GITHUB_ACTIONS': 'TRUE'}, clear=True):
        assert is_github_actions_ci(), "GITHUB_ACTIONS must be matched case-insensitively"
        assert not is_circle_ci()
    with patch.dict(os.environ, {'CIRCLECI': 'true'}, clear=True):
        assert is_circle_ci()
        assert not is_github_actions_ci()
    with patch.dict(os.environ, {}, clear=True):
        assert not is_github_actions_ci()
        assert not is_circle_ci()


def test_get_ci_branch():
    gha = {'GITHUB_ACTIONS': 'true', 'GITHUB_REF_NAME': 'main'}
    # On a pull request GITHUB_HEAD_REF holds the source branch and wins.
    with patch.dict(os.environ, {**gha, 'GITHUB_HEAD_REF': 'feature-x'}, clear=True):
        assert get_ci_branch() == 'feature-x', get_ci_branch()
    # On a push GITHUB_HEAD_REF is empty, so fall back to GITHUB_REF_NAME.
    with patch.dict(os.environ, {**gha, 'GITHUB_HEAD_REF': ''}, clear=True):
        assert get_ci_branch() == 'main', get_ci_branch()
    with patch.dict(os.environ, {'CIRCLECI': 'true', 'CIRCLE_BRANCH': 'main-2.x'}, clear=True):
        assert get_ci_branch() == 'main-2.x', get_ci_branch()
    with patch.dict(os.environ, {}, clear=True):
        assert get_ci_branch() == '', get_ci_branch()


def test_get_ci_job_name_and_defaults():
    gha = {'GITHUB_ACTIONS': 'true', 'GITHUB_JOB': 'run_tests'}
    # The composite's CANTON_CI_JOB_NAME carries the logical job name and wins over GITHUB_JOB.
    with patch.dict(
        os.environ, {**gha, 'CANTON_CI_JOB_NAME': 'nightly_integration_test'}, clear=True
    ):
        assert get_ci_job_name() == 'nightly_integration_test', get_ci_job_name()
    with patch.dict(os.environ, gha, clear=True):
        assert get_ci_job_name() == 'run_tests', get_ci_job_name()
        assert get_ci_node_index() == '0', "GHA without MATRIX_SHARD must default to shard 0"
        assert get_ci_commit_hash() == 'unknown', get_ci_commit_hash()
    with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}, clear=True):
        assert get_ci_job_name() == 'unknown', get_ci_job_name()
        assert get_ci_node_index() == '0', get_ci_node_index()
        assert get_ci_commit_hash() == 'unknown', get_ci_commit_hash()
    with patch.dict(os.environ, {'CIRCLECI': 'true'}, clear=True):
        assert get_ci_job_name() == 'unknown', get_ci_job_name()
        assert get_ci_node_index() == '0', get_ci_node_index()
        assert get_ci_commit_hash() == 'unknown', get_ci_commit_hash()


def test_get_ci_commit_url():
    sha = 'd' * 40
    gha_oss = {'GITHUB_ACTIONS': 'true', 'GITHUB_REPOSITORY': 'digital-asset/canton'}
    with patch.dict(os.environ, gha_oss, clear=True):
        assert get_ci_commit_url(sha) == f'https://github.com/digital-asset/canton/commit/{sha}'
    with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}, clear=True):
        assert get_ci_commit_url(sha) == f'https://github.com/DACH-NY/canton/commit/{sha}'
    with patch.dict(os.environ, {'CIRCLECI': 'true'}, clear=True):
        assert get_ci_commit_url(sha) == f'https://github.com/DACH-NY/canton/commit/{sha}'


def test_format_test_name_sanitizes():
    # Datadog tags must be ASCII
    assert remove_non_ascii_characters("Tést–Name") == "T_st_Name"
    assert remove_non_ascii_characters("PlainName") == "PlainName"
    # Variable context after the first slash is dropped
    assert remove_everything_after_first_slash("Foo/sequencerx=sequencer1/bar") == "Foo"
    assert remove_everything_after_first_slash("Foo") == "Foo"
    assert (
        format_test_name("WARN c.d.c.r.DbStorageSingle:BftShard3Test/sequencerx=sequencer2")
        == "WARN c.d.c.r.DbStorageSingle:BftTest"
    )
    # Long names are truncated to 200 characters
    assert format_test_name("A" * 250) == "A" * 200
    assert format_test_name("A" * 200) == "A" * 200


def test_format_issue_title_and_should_report_issues():
    with patch(f'{__name__}.branch', 'main'):
        assert format_issue_title("FooShard1Test/x=1") == "[main] Flaky FooTest"
        assert should_report_issues()
    with patch(f'{__name__}.branch', 'main-2.x'):
        assert should_report_issues()
    with patch(f'{__name__}.branch', 'feature-x'):
        assert not should_report_issues()
    with patch(f'{__name__}.branch', ''):
        assert not should_report_issues()


def test_with_evidence_link():
    row = "| d | job | 0 | [1](u) | [c](u) |  | regular |"
    assert with_evidence_link(row, None) == row
    assert with_evidence_link(row, "") == row
    assert (
        with_evidence_link(row, "https://e")
        == "| d | job | 0 | [1](u) | [c](u) | [evidence](https://e) | regular |"
    )


def test_is_transient_gh_error():
    def result(stdout='', stderr=''):
        return subprocess.CompletedProcess(args=[], returncode=1, stdout=stdout, stderr=stderr)

    for code in TRANSIENT_HTTP_CODES:
        assert is_transient_gh_error(result(stderr=f'HTTP {code}: Bad Gateway')), code
    assert is_transient_gh_error(result(stdout='HTTP 503')), "stdout must be checked too"
    assert not is_transient_gh_error(result(stderr='HTTP 404: Not Found'))
    assert not is_transient_gh_error(result())


def test_run_gh_with_retries():
    def completed(returncode, stderr=''):
        return subprocess.CompletedProcess(args=[], returncode=returncode, stdout='', stderr=stderr)

    ok = completed(0)
    transient = completed(1, 'HTTP 502')
    permanent = completed(1, 'HTTP 404')

    # A transient error is retried and the eventual success is returned.
    with (
        patch(f'{__name__}.subprocess.run', side_effect=[transient, ok]) as run_mock,
        patch(f'{__name__}.time.sleep') as sleep_mock,
    ):
        assert run_gh_with_retries(['issue', 'list'], stdin='body') is ok
        assert run_mock.call_count == 2, run_mock.call_count
        assert sleep_mock.call_count == 1, sleep_mock.call_count
        for call in run_mock.call_args_list:
            assert call.args[0] == ['gh', 'issue', 'list'], call
            assert call.kwargs['input'] == 'body', "stdin must be passed on every attempt"
            assert call.kwargs['env'] is gh_flaky_test_env, "gh must get the flaky-test token"

    # A non-transient error is returned straight away.
    with (
        patch(f'{__name__}.subprocess.run', return_value=permanent) as run_mock,
        patch(f'{__name__}.time.sleep') as sleep_mock,
    ):
        assert run_gh_with_retries(['issue', 'list']) is permanent
        assert run_mock.call_count == 1, run_mock.call_count
        assert sleep_mock.call_count == 0, sleep_mock.call_count

    # Persistent transient errors give up after GH_RETRY_ATTEMPTS.
    with (
        patch(f'{__name__}.subprocess.run', return_value=transient) as run_mock,
        patch(f'{__name__}.time.sleep') as sleep_mock,
    ):
        assert run_gh_with_retries(['issue', 'list']) is transient
        assert run_mock.call_count == GH_RETRY_ATTEMPTS, run_mock.call_count
        assert sleep_mock.call_count == GH_RETRY_ATTEMPTS - 1, sleep_mock.call_count


def test_check_result():
    check_result(subprocess.CompletedProcess(args=['gh'], returncode=0, stdout='', stderr=''))
    failed = subprocess.CompletedProcess(args=['gh'], returncode=1, stdout='', stderr='boom')
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            check_result(failed)
    except Exception as e:
        assert str(e) == "gh command failed", e
    else:
        raise AssertionError("check_result must raise on a non-zero return code")


def test_artifact_dir():
    with patch.dict(
        os.environ, {'FLAKY_ARTIFACT_DIR': '/flaky', 'RUNNER_TEMP': '/runner'}, clear=True
    ):
        assert artifact_dir() == '/flaky', artifact_dir()
        assert failing_tests_path() == os.path.join('/flaky', 'failing_tests.json')
        assert streaks_path() == os.path.join('/flaky', 'streaks.json')
        assert evidence_path() == os.path.join('/flaky', 'evidence.json')
    with patch.dict(os.environ, {'RUNNER_TEMP': '/runner'}, clear=True):
        assert artifact_dir() == '/runner', artifact_dir()
    with patch.dict(os.environ, {}, clear=True):
        assert artifact_dir() == tempfile.gettempdir(), artifact_dir()


def test_required_env_vars():
    with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}, clear=True):
        assert 'MATRIX_SHARD' in ci_required_context_vars(), ci_required_context_vars()
        assert 'GITHUB_SHA' in ci_required_context_vars(), ci_required_context_vars()
    with patch.dict(os.environ, {'CIRCLECI': 'true'}, clear=True):
        assert 'CIRCLE_SHA1' in ci_required_context_vars(), ci_required_context_vars()
    with patch.dict(os.environ, {}, clear=True):
        assert 'CIRCLE_SHA1' in ci_required_context_vars(), ci_required_context_vars()

    with patch.dict(os.environ, {'PRESENT': 'x', 'EMPTY': ''}, clear=True):
        assert_required_env_vars(['PRESENT'])
        try:
            assert_required_env_vars(['PRESENT', 'EMPTY', 'ABSENT'])
        except RuntimeError as e:
            assert 'EMPTY, ABSENT' in str(e), e
            assert 'PRESENT' not in str(e), e
        else:
            raise AssertionError("assert_required_env_vars must raise on empty/absent vars")


if __name__ == "__main__":
    # flaky_common is a library, running it directly executes the shared self-tests.
    run_guarded_self_test(self_test)
