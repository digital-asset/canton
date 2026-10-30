import os
import subprocess
import tempfile
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("sbt-ci-wrapper.sh")
REPO_ROOT = SCRIPT_PATH.parents[2]


def run_wrapper(
    extra_env: dict[str, str], fake_sbt_exit_code: int | None = None
) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as temp_dir:
        path = os.environ["PATH"]
        if fake_sbt_exit_code is not None:
            fake_bin = Path(temp_dir) / "bin"
            fake_bin.mkdir()
            fake_sbt = fake_bin / "sbt"
            fake_sbt.write_text(f"#!/usr/bin/env bash\nexit {fake_sbt_exit_code}\n")
            fake_sbt.chmod(0o755)
            path = f"{fake_bin}{os.pathsep}{path}"
        env = {
            "PATH": path,
            "HOME": temp_dir,
            "TEMPDIR": temp_dir,
            "REPORT_TO_DATADOG": "false",
            "FAIL_ON_ERROR_IN_OUTPUT": "false",
        }
        env.update(extra_env)
        return subprocess.run(
            ["bash", str(SCRIPT_PATH), "true"],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )


def test_abort_before_sbt_runs_fails_the_script() -> None:
    # The Maven mirror credential check aborts the wrapper before sbt starts.
    # That used to be reported as success, which made the CircleCI job green without running any test.
    result = run_wrapper({"USE_MAVEN_MIRROR": "true", "MAVEN_PASSWORD": ""})

    assert "MAVEN_PASSWORD must be set" in result.stderr
    assert result.returncode != 0


def test_successful_sbt_run_still_succeeds() -> None:
    result = run_wrapper({}, fake_sbt_exit_code=0)

    assert "The script has terminated successfully" in result.stderr + result.stdout
    assert result.returncode == 0


def test_failing_sbt_run_propagates_its_exit_code() -> None:
    result = run_wrapper({}, fake_sbt_exit_code=3)

    assert result.returncode == 3


def test_succeed_on_error_still_overrides_an_early_abort() -> None:
    result = run_wrapper(
        {"USE_MAVEN_MIRROR": "true", "MAVEN_PASSWORD": "", "SUCCEED_ON_ERROR": "true"}
    )

    assert "MAVEN_PASSWORD must be set" in result.stderr
    assert result.returncode == 0
