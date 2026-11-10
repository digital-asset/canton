"""Every job_name canton_nightly.yml reports must classify as nightly.

Otherwise its failures never raise a broken-nightly streak (#37656).
"""

from pathlib import Path

import yaml

from flaky_common import is_nightly_job, is_unstable_job

NIGHTLY_WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/canton_nightly.yml"


def test_nightly_job_names_are_classified_as_nightly():
    """Each explicit `with.job_name` is a nightly (or unstable) job for the flaky pipeline."""
    jobs = yaml.safe_load(NIGHTLY_WORKFLOW.read_text())["jobs"]
    names = [str(j["with"]["job_name"]) for j in jobs.values() if "job_name" in j.get("with", {})]
    assert names, f"no job_name found in {NIGHTLY_WORKFLOW}"
    not_nightly = [n for n in names if not (is_nightly_job(n) or is_unstable_job(n))]
    assert not_nightly == [], (
        "add these to NIGHTLY_JOBS_WITHOUT_PREFIX or prefix them with nightly_"
    )
