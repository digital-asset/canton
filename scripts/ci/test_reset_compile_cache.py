import os
import subprocess
import tempfile
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("reset_compile_cache.sh")


def run_script(
    *,
    reset_requested: str = "false",
    branch_name: str = "feature/test",
    force_recompile_patterns: str | None = None,
    create_targets: bool = False,
) -> str:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        github_output = temp_path / "github_output.txt"
        github_output.touch()

        if create_targets:
            for target_dir in [
                temp_path / "target",
                temp_path / "community" / "app" / "target",
                temp_path / "community" / "common" / "target",
            ]:
                target_dir.mkdir(parents=True, exist_ok=True)
                (target_dir / "marker.txt").write_text("stale\n")

        if force_recompile_patterns is not None:
            force_recompile_file = (
                temp_path / ".circleci" / "branches_to_be_fully_recompiled_in_ci.txt"
            )
            force_recompile_file.parent.mkdir(parents=True, exist_ok=True)
            force_recompile_file.write_text(force_recompile_patterns)

        env = os.environ.copy()
        env.update(
            {
                "RESET_REQUESTED": reset_requested,
                "BRANCH_NAME": branch_name,
                "GITHUB_OUTPUT": str(github_output),
            }
        )

        subprocess.run(
            ["bash", str(SCRIPT_PATH)],
            cwd=temp_path,
            env=env,
            check=True,
        )

        output = github_output.read_text()
        remaining_targets = list(temp_path.glob("**/target"))
        assert remaining_targets == [], f"Targets were not cleaned: {remaining_targets}"
        return output


def test_requests_network_restore_on_fresh_runner_with_no_targets() -> None:
    output = run_script(create_targets=False)
    assert "needs_network_restore=true" in output


def test_cleans_targets_and_requests_network_restore_by_default() -> None:
    output = run_script(create_targets=True)
    assert "needs_network_restore=true" in output


def test_cleans_targets_and_skips_network_restore_when_reset_requested() -> None:
    output = run_script(reset_requested="true", create_targets=True)
    assert "needs_network_restore=false" in output


def test_cleans_targets_and_skips_network_restore_for_force_recompile_branch() -> None:
    output = run_script(
        branch_name="release-line-9.9.x",
        force_recompile_patterns="release-line-9\\.9\\.x\n",
        create_targets=True,
    )
    assert "needs_network_restore=false" in output
