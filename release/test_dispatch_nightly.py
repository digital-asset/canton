"""Tests for release/dispatch_nightly.py."""

import subprocess

import pytest

import dispatch_nightly as dn


def test_refs_are_main_plus_supported_lines_only():
    # Ramped-down lines are absent from the manifest, so they must not get a nightly.
    manifest = "# comment\n3.8\n\n3.7  # trailing comment\n"
    assert dn.nightly_refs(manifest) == ["main", "release-line-3.7", "release-line-3.8"]


def test_real_manifest_parses():
    # A bad entry would cancel every release-line nightly.
    assert dn.nightly_refs(dn.DEFAULT_MANIFEST.read_text())[0] == "main"


def test_bad_manifest_still_dispatches_main_and_fails(tmp_path, monkeypatch):
    # A typo in the release-line list must not cost main its nightly, but must stay visible.
    manifest = tmp_path / "lines.txt"
    manifest.write_text("3.7\nrelease-line-3.8\n")
    dispatched = []
    monkeypatch.setattr(dn.subprocess, "run", lambda cmd, check: dispatched.append(cmd[-1]))
    with pytest.raises(SystemExit) as exit_info:
        dn.main(["--repo", "o/r", "--manifest", str(manifest)])
    assert dispatched == ["main"]
    assert exit_info.value.code == 1


def test_one_failing_ref_does_not_skip_the_rest():
    dispatched = []

    def dispatch(repo, ref):
        if ref == "release-line-3.7":
            raise subprocess.CalledProcessError(1, "gh")
        dispatched.append(ref)

    failed = dn.dispatch_all("o/r", ["main", "release-line-3.7", "release-line-3.8"], dispatch)
    assert dispatched == ["main", "release-line-3.8"]
    assert failed == ["release-line-3.7"]


def test_failed_dispatch_turns_the_scheduler_red(tmp_path, monkeypatch):
    manifest = tmp_path / "lines.txt"
    manifest.write_text("3.7\n")

    def run(cmd, check):
        if "release-line-3.7" in cmd:
            raise subprocess.CalledProcessError(1, cmd)

    monkeypatch.setattr(dn.subprocess, "run", run)
    with pytest.raises(SystemExit) as exit_info:
        dn.main(["--repo", "o/r", "--manifest", str(manifest)])
    assert exit_info.value.code == 1
