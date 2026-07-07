"""Shared helpers for the flaky-test CI pipeline.

Holds the code the pipeline scripts share: CI-environment detection, the `gh`
wrapper, test-name and issue-table formatting, the project and field config, the
JSON artifact contracts, and a small self-test harness. Each script imports the
names it needs (`from flaky_common import ...`).
"""

import os
import re
import subprocess
import datetime
import time
import json
import tempfile
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

milestone = "Flaky Tests" # flaky tests milestone M97 (milestone number 31)
flaky_test_project = "PVT_kwDOAJX-Fc4AbncN" # https://github.com/orgs/DACH-NY/projects/38/

release_line_field = "PVTSSF_lADOAJX-Fc4AbncNzgcWE1k" # ID of the custom field "Release Line"

# The value of this dictionary points to the ID of the field value of the `Release Line` field in the
# flaky test kanban board.
# If you add a new field, execute listReleaseLineFields.graphql to find the new ID
branches_to_report = {
    "main" : "733298b6", # ID of the value "main" for the Release Line field in the project
    "main-2.x" : "073b4df3" # ID of the value "main-2.x" for the Release Line field in the project
}

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
        if repository and run_id:
            return f"{server}/{repository}/actions/runs/{run_id}"
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
        return os.environ.get("GITHUB_JOB", "unknown")
    return os.environ.get("CIRCLE_JOB", "unknown")

def get_ci_commit_hash() -> str:
    if is_github_actions_ci():
        return os.environ.get("GITHUB_SHA", "unknown")
    if is_circle_ci():
        return os.environ.get("CIRCLE_SHA1", "unknown")
    return os.environ.get("CIRCLE_SHA1", "unknown")

def get_ci_parallel_run_url(build_url: str, node_index: str) -> str:
    if is_github_actions_ci():
        return build_url
    if is_circle_ci():
        return f"{build_url.replace('circleci.com/gh/', 'app.circleci.com/jobs/github/')}/parallel-runs/{node_index}"
    return f"{build_url.replace('circleci.com/gh/', 'app.circleci.com/jobs/github/')}/parallel-runs/{node_index}"


branch = get_ci_branch()

# replace GITHUB_TOKEN with GITHUB_FLAKY_TEST_TOKEN, which also is permitted to use the project scope.
# this is required to use the gh command to unarchive/restore issues
gh_flaky_test_env = os.environ.copy()
if "GITHUB_FLAKY_TEST_TOKEN" in gh_flaky_test_env:
    gh_flaky_test_env["GITHUB_TOKEN"] = os.environ["GITHUB_FLAKY_TEST_TOKEN"]

def should_report_issues():
    return branch in branches_to_report


def is_nightly_job(job: str) -> bool:
    """Nightly test jobs are the `nightly_*` jobs in canton_nightly.yml.

    `unstable_test` also runs nightly but is intentionally allowed to fail, so it
    is excluded: broken-nightly is for genuinely broken tests, not unstable ones.
    """
    return bool(job) and job.startswith("nightly_")


# --- gh CLI wrapper -------------------------------------------------------

def is_transient_gh_error(result) -> bool:
    combined = result.stderr + result.stdout
    return any(code in combined for code in TRANSIENT_HTTP_CODES)

def run_gh_with_retries(args: list, attempts: int = GH_RETRY_ATTEMPTS) -> subprocess.CompletedProcess:
    result = subprocess.run(["gh"] + args, capture_output=True, text=True, env=gh_flaky_test_env)
    if result.returncode != 0 and is_transient_gh_error(result) and attempts > 1:
        print(f"Transient gh error (attempt {GH_RETRY_ATTEMPTS - attempts + 1}/{GH_RETRY_ATTEMPTS}), retrying in {GH_RETRY_DELAY_SECONDS}s...")
        time.sleep(GH_RETRY_DELAY_SECONDS)
        return run_gh_with_retries(args, attempts - 1)
    return result

def check_result(result):
    if result.returncode != 0:
        print(f"### ERROR while executing gh command!")
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


def create_issue_table_header():
    return "| Date | Job | Node | Build | Commit |\n|---|---|---|---|---|"

def create_issue_table_row():
    date_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit_hash = get_ci_commit_hash()
    if is_github_actions_ci():
        repo_owner = os.environ.get('GITHUB_REPOSITORY', 'DACH-NY/canton').split('/', 1)[0]
    else:
        repo_owner = os.environ.get('CIRCLE_PROJECT_USERNAME', 'DACH-NY')
    commit_url = f"https://github.com/{repo_owner}/canton/commit/{commit_hash}"
    build_url = get_ci_build_url()
    build_number = build_url.rstrip('/').rsplit('/', 1)[-1]
    job = get_ci_job_name()
    node_index = get_ci_node_index()
    parallel_run_url = get_ci_parallel_run_url(build_url, node_index)
    return f"| {date_str} | {job} | {node_index} | [{build_number}]({parallel_run_url}) | [{commit_hash[:8]}]({commit_url}) |"


# --- JSON artifact contracts ---------------------------------------------
#
# The pipeline steps pass data via JSON files under a shared directory so each
# concern is its own process. Paths default to a CI temp dir and can be
# overridden per step with --failing-tests-json / --streaks-json.

ARTIFACT_VERSION: Final[int] = 1

def artifact_dir() -> str:
    return os.environ.get("FLAKY_ARTIFACT_DIR") or os.environ.get("RUNNER_TEMP") or tempfile.gettempdir()

def failing_tests_path() -> str:
    return os.path.join(artifact_dir(), "failing_tests.json")

def streaks_path() -> str:
    return os.path.join(artifact_dir(), "streaks.json")

def write_failing_tests(path: str, tests) -> None:
    with open(path, "w") as f:
        json.dump({"version": ARTIFACT_VERSION, "failing_tests": sorted(set(tests))}, f, indent=2)
    print(f"Wrote {len(set(tests))} failing test(s) to {path}")

def read_failing_tests(path: str) -> list:
    if not path or not os.path.exists(path):
        print(f"No failing-tests artifact at {path}; treating as empty.")
        return []
    with open(path) as f:
        return list(json.load(f).get("failing_tests", []))

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
        raise RuntimeError(f"Cannot execute CI script. Missing required environment variables: {', '.join(missing)}")

def run_guarded_self_test(self_test_fn) -> None:
    """Run a script's self_test() and fail loudly if it leaked os.environ changes."""
    env_before = dict(os.environ)
    try:
        self_test_fn()
    finally:
        env_after = dict(os.environ)
        if env_before != env_after:
            leaked = {k: (env_before.get(k), env_after.get(k)) for k in env_before.keys() | env_after.keys() if env_before.get(k) != env_after.get(k)}
            raise RuntimeError(f"self_test() polluted os.environ (before -> after): {leaked}")


def self_test():
    """Self-checks for the shared helpers; each pipeline script runs this too."""
    test_gh_flags()
    test_format_test_name_collapses_shards()
    test_create_issue_table_row()
    test_is_nightly_job()
    print("flaky_common self-checks passed")


def test_format_test_name_collapses_shards():
    # Shard partitions of the same logical test must collapse to the same name
    assert format_test_name("LedgerApiShard0ConformanceTestPostgres") == "LedgerApiConformanceTestPostgres"
    assert format_test_name("LedgerApiShard5ConformanceTestPostgres") == "LedgerApiConformanceTestPostgres"
    # Two-digit shard indices
    assert format_test_name("LedgerApiShard10ConformanceTest") == "LedgerApiConformanceTest"
    # Non-shard names are unchanged
    assert format_test_name("RegularFlakyTest") == "RegularFlakyTest"
    # "Shard" without a digit suffix must not be stripped
    assert format_test_name("ShardingLayerTest") == "ShardingLayerTest"

def test_gh_flags():
    checks = [
        (["issue", "create", "--help"], ["--title", "--body", "--milestone", "--repo"]),
        (["issue", "edit",   "--help"], ["--title", "--body", "--repo"]),
        (["issue", "reopen", "--help"], ["--repo"]),
    ]
    for subcommand, flags in checks:
        result = subprocess.run(["gh"] + subcommand, capture_output=True, text=True)
        help_text = result.stdout + result.stderr
        for flag in flags:
            assert flag in help_text, f"`gh {' '.join(subcommand[:-1])}` help does not mention flag `{flag}` — was it renamed?"

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
        assert '| Date | Job | Node | Build | Commit |' in header, f"Missing header in: {header}"
        line = create_issue_table_row()
        assert '| test_with_java17 |' in line, f"Missing job in: {line}"
        assert '| 5 |' in line, f"Missing node_index in: {line}"
        assert '[3196076](https://app.circleci.com/jobs/github/DACH-NY/canton/3196076/parallel-runs/5)' in line, f"Missing parallel-run build link in: {line}"
        assert '[a1b2c3d4](https://github.com/DACH-NY/canton/commit/a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2)' in line, f"Missing commit in: {line}"

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
        assert '[1491](https://app.circleci.com/jobs/github/digital-asset/canton/1491/parallel-runs/6)' in line, f"Missing OSS canton build link in: {line}"
        assert '[b2c3d4e5](https://github.com/digital-asset/canton/commit/b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3)' in line, f"Missing OSS canton commit link in: {line}"

    # Test GitHub Actions
    env_gha = {
        'CIRCLECI': 'false',
        'GITHUB_ACTIONS': 'true',
        'GITHUB_JOB': 'integration-tests-shard-2',
        'MATRIX_SHARD': '2',
        'GITHUB_RUN_ID': '9876543210',
        'GITHUB_REPOSITORY': 'DACH-NY/canton',
        'GITHUB_SERVER_URL': 'https://github.com',
        'GITHUB_SHA': 'c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4',
    }
    with patch.dict(os.environ, env_gha, clear=False):
        line = create_issue_table_row()
        assert '| integration-tests-shard-2 |' in line, f"Missing GHA job name in: {line}"
        assert '| 2 |' in line, f"Missing GHA matrix shard in: {line}"
        assert '[9876543210](https://github.com/DACH-NY/canton/actions/runs/9876543210)' in line, f"Missing GHA build link in: {line}"
        assert '[c3d4e5f6](https://github.com/DACH-NY/canton/commit/c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4)' in line, f"Missing GHA commit in: {line}"

def test_is_nightly_job():
    assert is_nightly_job("nightly_integration_test")
    assert is_nightly_job("nightly_test_upgrades_matrix")
    assert not is_nightly_job("test_with_java17")
    assert not is_nightly_job("unstable_test")  # intentionally unstable, excluded
    assert not is_nightly_job("")


if __name__ == "__main__":
    # flaky_common is a library, running it directly executes the shared self-tests.
    run_guarded_self_test(self_test)
