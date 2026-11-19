#!/usr/bin/env python3
"""Guard that keeps NIGHTLY_JOBS_WITHOUT_PREFIX in sync with `canton_nightly`.

`is_nightly_job` in flaky_common.py decides which failures get the time-based
nightly streak check and the `broken-nightly` escalation. It recognises a job
by its `nightly_` prefix or by its name in the hand-maintained
NIGHTLY_JOBS_WITHOUT_PREFIX set, so that set can silently drift from the jobs
the nightly workflow actually runs:

  - a new or renamed prefix-less nightly job is left unrecognised, so a broken
    nightly test streaks for weeks without ever being flagged
  - a renamed or removed job leaves its old name behind in the set

This script asserts the invariant that every job in the nightly workflows (the
live CircleCI `.circleci/config/workflows/canton_nightly.yml` and the GitHub
Actions `.github/workflows/canton_nightly.yml` it is being migrated to) is either
recognised by `is_nightly_job` (or `is_unstable_job`, for the intentionally-flaky
lanes) or listed in an explicit exclusion allowlist, and
that every name in NIGHTLY_JOBS_WITHOUT_PREFIX matches a job in one of them. It
exits non-zero (listing the offenders) when the invariant is broken.

Jobs are checked under the name `is_nightly_job` sees at run time, which is not
always the name written in the workflow:

  - a CircleCI workflow entry runs under its `name:` when one is given (that is
    what CIRCLE_JOB holds) and under the job key otherwise
  - a GitHub Actions test job reports CANTON_CI_JOB_NAME, which the reusable
    workflow sets from its `job_name` input (canton_nightly.yml passes the
    CircleCI name there) and cleanup_test_job falls back to `github.job`, the id
    of the job inside the reusable workflow, when that is unset or empty. Jobs
    that never reach cleanup_test_job keep their key, since they never reach the
    flaky pipeline.

That GitHub Actions resolution holds only while the reusable workflow passes its
`job_name` input through unchanged, so the guard also fails when a test job sets
CANTON_CI_JOB_NAME or a step's `job_name` to anything but `${{ inputs.job_name }}`.

The workflow is parsed with PyYAML (available in the repo's nix shell) rather
than by hand, so any valid reshaping of the YAML (anchors and merge keys,
inline comments, reordered keys) is understood without special-casing.

Run locally with:  python3 scripts/ci/check_canton_nightly_job_classification.py
"""

import sys
from pathlib import Path
from typing import NamedTuple

import pytest
import yaml
from flaky_common import NIGHTLY_JOBS_WITHOUT_PREFIX, is_nightly_job, is_unstable_job

REPO_ROOT = Path(__file__).parents[2]
CIRCLECI_WORKFLOW = REPO_ROOT / ".circleci/config/workflows/canton_nightly.yml"
GHA_WORKFLOW = REPO_ROOT / ".github/workflows/canton_nightly.yml"
NIGHTLY_PREFIX = "nightly_"

# The action that runs the flaky pipeline and sets the job name it reports. A GitHub
# Actions job runs tests when it reaches this action, directly or through local
# composite actions such as run_tests.
GHA_CLEANUP_ACTION = "./.github/actions/test/cleanup_test_job"

# Repo-relative path to flaky_common.py, so drift messages point the reader
# straight at the file to edit rather than a bare basename.
FLAKY_COMMON_PATH = Path(__file__).with_name("flaky_common.py").relative_to(REPO_ROOT).as_posix()

# Nightly-workflow jobs that must not be treated as nightly tests: build, package,
# docs and publish steps, and jobs outside the flaky pipeline's nightly scope.
# Adding to this set is a conscious, reviewable choice, which is the whole point
# of the guard.
EXCLUDED = {
    # GitHub Actions only
    "notify",
    "pick_runner",
    # CircleCI, or both
    "assembly_ledger_api_test_tool",
    "benchmark_transaction_snapshot",
    "blackduck",
    "build_and_publish_docker",
    "build_docs",
    "build_docs_snippets",
    "build_scaladoc",
    "check_todos",
    "compile",
    "package_community",
    "package_jsonapi",
    "package_kms_driver",
    "publish_canton_open_source_code_drop",
    "publish_libraries",
    "publish_release",
    "stability_crash_recovery_test_alerting",
    "test_performance_runner",
    "test_ping_arm",
    "test_ping_windows",
    "test_scala3",
}


def _circleci_job_names(workflow):
    """Return the names the CircleCI workflow's jobs run under.

    A CircleCI workflow `jobs:` entry is either a bare job key or a single-key
    mapping `{job_key: params}`, where `params.name` overrides the run name.
    """
    names = set()
    for entry in (workflow or {}).get("jobs") or []:
        if isinstance(entry, str):
            names.add(entry)
            continue
        ((key, params),) = entry.items()
        names.add((params or {}).get("name", key))
    return names


def _workflow_call_inputs(workflow):
    # PyYAML (YAML 1.1) parses the bare `on` key as the boolean True.
    triggers = workflow.get(True) or workflow.get("on") or {}
    if not isinstance(triggers, dict):  # `on: workflow_call` or a list: no inputs
        return {}
    return (triggers.get("workflow_call") or {}).get("inputs") or {}


def _reaches_cleanup(steps, load):
    """Whether `steps` run GHA_CLEANUP_ACTION, following local composite actions.

    A step passing `report_to_datadog: false` does not count: cleanup_test_job only
    feeds the flaky pipeline when that input is 'true'.
    """
    for step in steps or []:
        uses = step.get("uses") or ""
        if str((step.get("with") or {}).get("report_to_datadog", "true")).lower() == "false":
            continue
        if uses == GHA_CLEANUP_ACTION:
            return True
        if uses.startswith("./"):
            action = load(f"{uses}/action.yml") or {}
            if _reaches_cleanup((action.get("runs") or {}).get("steps"), load):
                return True
    return False


def _gha_test_jobs(workflow, load):
    """Yield `(caller_key, job, called_path, called, test_jobs)` for each job that calls a
    local reusable workflow, where `test_jobs` maps the ids of the called workflow's jobs
    that reach GHA_CLEANUP_ACTION to their definitions."""
    for key, job in ((workflow or {}).get("jobs") or {}).items():
        uses = job.get("uses") or ""
        if not uses.startswith("./"):
            continue
        called = load(uses)
        test_jobs = {
            inner: inner_job
            for inner, inner_job in (called.get("jobs") or {}).items()
            if _reaches_cleanup(inner_job.get("steps"), load)
        }
        yield key, job, uses, called, test_jobs


def _passed_job_name(job, called):
    """Return the `job_name` input the caller `job` passes to `called`, or its default.

    cleanup_test_job reports `inputs.job_name || github.job`, so an unset or empty
    input falls back to the inner job's id.
    """
    default = (_workflow_call_inputs(called).get("job_name") or {}).get("default")
    return (job.get("with") or {}).get("job_name") or default


def _gha_job_names(workflow, load):
    """Return the names the GitHub Actions workflow's jobs report at run time.

    `load` maps a repo-relative local path (`./.github/...`, a reusable workflow
    or an action's `action.yml`) to its parsed YAML.
    """
    names = {
        key
        for key, job in ((workflow or {}).get("jobs") or {}).items()
        if not (job.get("uses") or "").startswith("./")
    }
    for key, job, _, called, test_jobs in _gha_test_jobs(workflow, load):
        if not test_jobs:
            names.add(key)
            continue
        job_name = _passed_job_name(job, called)
        if job_name:
            names.add(job_name)
        else:
            names.update(test_jobs)
    return names


PASSED_THROUGH_JOB_NAME = "${{inputs.job_name}}"


def _gha_job_name_overrides(workflow, load):
    """Return where a reusable workflow's test job reports a name other than its
    `job_name` input, which `_gha_job_names` assumes it passes through unchanged.

    Checks CANTON_CI_JOB_NAME in the workflow, job and step `env`, and each step's
    `with.job_name`. A test job given a `job_name` must also set CANTON_CI_JOB_NAME in
    its workflow or job `env`, which cleanup_test_job reads first: without it, dropping
    the step's `job_name` too would silently report `github.job` instead.
    """
    found = set()
    for _, job, uses, called, test_jobs in _gha_test_jobs(workflow, load):
        for inner, inner_job in test_jobs.items():
            env_set = any(
                "CANTON_CI_JOB_NAME" in (env or {})
                for env in (called.get("env"), inner_job.get("env"))
            )
            if _passed_job_name(job, called) and not env_set:
                found.add(f"{uses.removeprefix('./')} jobs.{inner}.env.CANTON_CI_JOB_NAME: not set")
            settings = [
                ("env.CANTON_CI_JOB_NAME", called.get("env") or {}),
                (f"jobs.{inner}.env.CANTON_CI_JOB_NAME", inner_job.get("env") or {}),
            ]
            for i, step in enumerate(inner_job.get("steps") or []):
                where = f"jobs.{inner}.steps[{i}]"
                settings.append((f"{where}.env.CANTON_CI_JOB_NAME", step.get("env") or {}))
                settings.append((f"{where}.with.job_name", step.get("with") or {}))
            for where, mapping in settings:
                field = where.rsplit(".", 1)[1]
                if field not in mapping:
                    continue
                value = str(mapping[field])
                if "".join(value.split()) != PASSED_THROUGH_JOB_NAME:
                    found.add(f"{uses.removeprefix('./')} {where}: {value!r}")
    return tuple(sorted(found))


def _classified(job):
    """Whether the flaky pipeline classifies `job` as a nightly job, or as an
    intentionally-flaky unstable lane (which runs nightly but is kept out of
    broken-nightly by design)."""
    return is_nightly_job(job) or is_unstable_job(job)


class Drift(NamedTuple):
    """How the classification sets disagree with the workflows; all empty when in sync."""

    unclassified: tuple[str, ...] = ()  # jobs neither recognised nor excluded
    stale: tuple[str, ...] = ()  # NIGHTLY_JOBS_WITHOUT_PREFIX names with no job
    stale_excluded: tuple[str, ...] = ()  # EXCLUDED names with no job
    overlap: tuple[str, ...] = ()  # names both classified and excluded
    prefixed: tuple[str, ...] = ()  # classified names the prefix already covers
    overridden: tuple[str, ...] = ()  # GHA test jobs not reporting their job_name input


def _classification_drift(job_names, classified, excluded, recognised=_classified):
    """Every job must be `recognised` as nightly or be in `excluded`, and every name
    in `classified` (NIGHTLY_JOBS_WITHOUT_PREFIX) or `excluded` must match a job."""
    return Drift(
        unclassified=tuple(sorted(j for j in job_names - excluded if not recognised(j))),
        stale=tuple(sorted(classified - job_names)),
        stale_excluded=tuple(sorted(excluded - job_names)),
        overlap=tuple(sorted(classified & excluded)),
        prefixed=tuple(sorted(j for j in classified if j.startswith(NIGHTLY_PREFIX))),
    )


# Report heading for each Drift field.
DRIFT_HEADINGS = {
    "unclassified": (
        f"nightly jobs not recognised by is_nightly_job or is_unstable_job (add to "
        f"NIGHTLY_JOBS_WITHOUT_PREFIX in {FLAKY_COMMON_PATH}, or to EXCLUDED in "
        f"{Path(__file__).name} if the job is not a nightly test):"
    ),
    "stale": "NIGHTLY_JOBS_WITHOUT_PREFIX names with no matching job (remove or rename):",
    "stale_excluded": "EXCLUDED names with no matching job (remove or rename):",
    "overlap": "jobs both in NIGHTLY_JOBS_WITHOUT_PREFIX and EXCLUDED (keep only one):",
    "prefixed": (
        f"`{NIGHTLY_PREFIX}` jobs listed in NIGHTLY_JOBS_WITHOUT_PREFIX, which the "
        "prefix already covers (remove them):"
    ),
    "overridden": (
        "GitHub Actions test jobs that do not report their `job_name` input, so the "
        "names checked above may not be what they report (set it to "
        "`${{ inputs.job_name }}`):"
    ),
}


def _render_drift(drift):
    """Return the human-readable report lines for `drift`."""
    lines = []
    for field, jobs in drift._asdict().items():
        if jobs:
            lines.append(f"  {DRIFT_HEADINGS[field]}")
            lines += [f"    - {job}" for job in jobs]
    return lines


def _load_yaml(path):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def _load_drift():
    """Return the run-time job names across both nightly workflows and their drift."""
    gha = _load_yaml(GHA_WORKFLOW)

    def load(path):
        return _load_yaml(REPO_ROOT / path)

    job_names = _circleci_job_names(_load_yaml(CIRCLECI_WORKFLOW)) | _gha_job_names(gha, load)
    drift = _classification_drift(job_names, set(NIGHTLY_JOBS_WITHOUT_PREFIX), EXCLUDED)
    return job_names, drift._replace(overridden=_gha_job_name_overrides(gha, load))


def main():
    job_names, drift = _load_drift()
    workflows = ", ".join(
        p.relative_to(REPO_ROOT).as_posix() for p in (CIRCLECI_WORKFLOW, GHA_WORKFLOW)
    )
    if any(drift):
        print(f"NIGHTLY_JOBS_WITHOUT_PREFIX has drifted from the nightly workflows ({workflows}):")
        for line in _render_drift(drift):
            print(line)
        sys.exit(1)

    print(
        f"All {len(job_names - EXCLUDED)} nightly test jobs in {workflows} are "
        f"recognised by is_nightly_job or is_unstable_job (excluding {len(EXCLUDED)} "
        "non-test jobs)."
    )


# --- tests (collected by the scripts pytest job) ---------------------------


def test_circleci_job_names_prefers_name_override():
    workflow = {"jobs": ["check_todos", {"compile": {}}, {"test": {"name": "test_36_with_java17"}}]}
    assert _circleci_job_names(workflow) == {"check_todos", "compile", "test_36_with_java17"}


RUN_TESTS = "./.github/actions/test/run_tests"
FAKE_ACTIONS = {
    f"{RUN_TESTS}/action.yml": {"runs": {"steps": [{"uses": GHA_CLEANUP_ACTION}]}},
    "./.github/actions/setup/action.yml": {"runs": {"steps": [{"run": "echo"}]}},
}


def test_gha_job_names_resolve_the_reported_name():
    run_tests = {"steps": [{"uses": RUN_TESTS}]}
    called = {
        **FAKE_ACTIONS,
        "./_test.yml": {
            True: {"workflow_call": {"inputs": {"job_name": {"default": "test"}}}},
            "jobs": {"resolve_timings": {"steps": []}, "test": run_tests},
        },
        "./_external_parties_test.yml": {
            True: {"workflow_call": {"inputs": {}}},
            "jobs": {"resolve_timings": {"steps": []}, "external_parties_test": run_tests},
        },
        "./_compile.yml": {"jobs": {"compile": {"steps": []}}},
    }
    workflow = {
        "jobs": {
            "pick_runner": {"steps": []},
            "compile": {"uses": "./_compile.yml"},
            "test_36_with_java17": {
                "uses": "./_test.yml",
                "with": {"job_name": "test_with_java17"},
            },
            "test_36": {"uses": "./_test.yml"},
            "external_parties_test_36": {"uses": "./_external_parties_test.yml"},
        }
    }
    assert _gha_job_names(workflow, called.__getitem__) == {
        "pick_runner",  # inline job: its key
        "compile",  # reusable workflow without a test action: its key
        "test_with_java17",  # `job_name` input passed by the caller
        "test",  # `job_name` input left at its default
        "external_parties_test",  # no `job_name` input: the inner test job's id
    }


def test_gha_test_job_found_through_wrapper_actions():
    wrapper = "./.github/actions/test/run_variations_tests"
    called = {
        **FAKE_ACTIONS,
        f"{wrapper}/action.yml": {"runs": {"steps": [{"uses": RUN_TESTS}]}},
        "./_variations_test.yml": {
            "jobs": {
                "setup": {"steps": [{"uses": "./.github/actions/setup"}]},
                "variations_test": {"steps": [{"uses": wrapper}]},
            }
        },
    }
    workflow = {"jobs": {"variations_test_36": {"uses": "./_variations_test.yml"}}}
    assert _gha_job_names(workflow, called.__getitem__) == {"variations_test"}


def test_gha_empty_job_name_falls_back_to_the_inner_job():
    # cleanup_test_job reports `inputs.job_name || github.job`, so an unset or empty
    # input reports the inner job's id.
    called = {
        **FAKE_ACTIONS,
        "./_test.yml": {
            True: {"workflow_call": {"inputs": {"job_name": {"type": "string"}}}},
            "jobs": {"test": {"steps": [{"uses": RUN_TESTS}]}},
        },
    }
    workflow = {
        "jobs": {
            "unset": {"uses": "./_test.yml"},
            "empty": {"uses": "./_test.yml", "with": {"job_name": ""}},
        }
    }
    assert _gha_job_names(workflow, called.__getitem__) == {"test"}


def test_gha_workflow_call_trigger_without_a_mapping():
    run_tests = {"steps": [{"uses": RUN_TESTS}]}
    called = {
        **FAKE_ACTIONS,
        "./_as_string.yml": {True: "workflow_call", "jobs": {"from_string": run_tests}},
        "./_as_list.yml": {True: ["workflow_call"], "jobs": {"from_list": run_tests}},
    }
    workflow = {
        "jobs": {"a": {"uses": "./_as_string.yml"}, "b": {"uses": "./_as_list.yml"}},
    }
    assert _gha_job_names(workflow, called.__getitem__) == {"from_string", "from_list"}


def test_gha_job_without_flaky_reporting_keeps_its_key():
    # cleanup_test_job only feeds the flaky pipeline when report_to_datadog is 'true'.
    called = {
        **FAKE_ACTIONS,
        "./_model_based_test.yml": {
            "jobs": {
                "model_based_test": {
                    "steps": [{"uses": RUN_TESTS, "with": {"report_to_datadog": "false"}}]
                }
            }
        },
    }
    workflow = {"jobs": {"model_based_test_36": {"uses": "./_model_based_test.yml"}}}
    assert _gha_job_names(workflow, called.__getitem__) == {"model_based_test_36"}


def test_gha_job_name_overrides_flag_anything_but_the_input():
    called = {
        **FAKE_ACTIONS,
        "./_test.yml": {
            "env": {"CANTON_CI_JOB_NAME": "${{ inputs.job_name }}"},
            "jobs": {
                "test": {
                    "env": {"CANTON_CI_JOB_NAME": "${{ inputs.job_name }}-shard"},
                    "steps": [
                        {"uses": RUN_TESTS, "with": {"job_name": "${{inputs.job_name}}"}},
                        {"uses": GHA_CLEANUP_ACTION, "with": {"job_name": "fixed"}},
                    ],
                },
                "compile": {"env": {"CANTON_CI_JOB_NAME": "not_a_test_job"}, "steps": []},
            },
        },
    }
    workflow = {"jobs": {"test_36": {"uses": "./_test.yml"}, "test_37": {"uses": "./_test.yml"}}}
    assert _gha_job_name_overrides(workflow, called.__getitem__) == (
        "_test.yml jobs.test.env.CANTON_CI_JOB_NAME: '${{ inputs.job_name }}-shard'",
        "_test.yml jobs.test.steps[1].with.job_name: 'fixed'",
    )


def test_gha_job_name_overrides_flag_a_missing_env_when_a_name_is_passed():
    called = {
        **FAKE_ACTIONS,
        "./_test.yml": {"jobs": {"test": {"steps": [{"uses": RUN_TESTS}]}}},
        "./_workflow_env.yml": {
            "env": {"CANTON_CI_JOB_NAME": "${{ inputs.job_name }}"},
            "jobs": {"test": {"steps": [{"uses": RUN_TESTS}]}},
        },
    }
    workflow = {
        "jobs": {
            "named": {"uses": "./_test.yml", "with": {"job_name": "test_36"}},
            "unnamed": {"uses": "./_test.yml"},
            "workflow_env": {"uses": "./_workflow_env.yml", "with": {"job_name": "test_37"}},
        }
    }
    assert _gha_job_name_overrides(workflow, called.__getitem__) == (
        "_test.yml jobs.test.env.CANTON_CI_JOB_NAME: not set",
    )


def _drift(job_names, classified, excluded):
    """_classification_drift with a stand-in for is_nightly_job over `classified`."""
    return _classification_drift(
        job_names,
        classified,
        excluded,
        recognised=lambda job: job.startswith(NIGHTLY_PREFIX) or job in classified,
    )


def test_classification_accepts_prefixed_listed_and_excluded():
    job_names = {"nightly_integration_test_36", "benchmark", "compile"}
    assert _drift(job_names, {"benchmark"}, {"compile"}) == Drift()


def test_classification_flags_unclassified_job():
    drift = _drift({"benchmark", "brand_new"}, {"benchmark"}, set())
    assert drift == Drift(unclassified=("brand_new",))


def test_classification_flags_stale_names():
    drift = _drift({"benchmark"}, {"benchmark", "renamed"}, {"gone"})
    assert drift == Drift(stale=("renamed",), stale_excluded=("gone",))


def test_classification_flags_overlap():
    drift = _drift({"benchmark", "compile"}, {"benchmark"}, {"benchmark", "compile"})
    assert drift == Drift(overlap=("benchmark",))


def test_classification_flags_redundant_prefix():
    drift = _drift({"nightly_x"}, {"nightly_x"}, set())
    assert drift == Drift(prefixed=("nightly_x",))


def test_render_drift_lists_each_nonempty_category():
    drift = Drift(unclassified=("brand_new",), overlap=("a", "b"))
    assert _render_drift(drift) == [
        f"  {DRIFT_HEADINGS['unclassified']}",
        "    - brand_new",
        f"  {DRIFT_HEADINGS['overlap']}",
        "    - a",
        "    - b",
    ]
    assert _render_drift(Drift()) == []


# TODO(i38321): Drop the skip once the nightly runs fully on GHA
@pytest.mark.skipif(
    not CIRCLECI_WORKFLOW.exists(),
    reason="the open-source code drop removes the CircleCI canton_nightly.yml",
)
def test_real_files_are_in_sync():
    """The shipped sets must classify every job the real nightly workflows report,
    including each name the GHA nightly reports, or its failures never raise a
    broken-nightly streak (#37656)."""
    assert _load_drift()[1] == Drift()


if __name__ == "__main__":
    main()
