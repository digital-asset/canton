import subprocess
from pathlib import Path

import pytest
from is_docs_only import is_docs_only


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], text=True).strip()


def commit(message: str) -> str:
    git("add", "--all")
    git("-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", message)
    return git("rev-parse", "HEAD")


def write(path: str, contents: str = "content\n") -> None:
    file = Path(path)
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(contents)


@pytest.fixture
def base(tmp_path, monkeypatch) -> str:
    monkeypatch.chdir(tmp_path)
    git("init", "--quiet", "-b", "main")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.com")
    git("config", "core.symlinks", "true")
    write("UNRELEASED.md")
    write("community/Example.scala")
    return commit("base")


@pytest.mark.parametrize(
    ("paths", "expected"),
    [
        (["UNRELEASED.md"], True),
        (["release-notes/3.7.1.md", "UNRELEASED.md"], True),
        (["CONTRIBUTING.md", "contributing/release-process/guide.md"], True),
        (["contributing/screenshot.png"], True),
        (["contributing/note with\nnewline.md"], True),
        (["UNRELEASED.md", "community/Example.scala"], False),
        (["UNRELEASED.md", ".github/workflows/build.yml"], False),
        (["contributing/generate.py"], False),
        (["docs-open/src/sphinx/index.rst"], False),
        (["release-notes-backup/notes.md"], False),
        ([], False),
    ],
)
def test_classifies_candidate_tree(base, paths, expected):
    for path in paths:
        write(path, "updated\n")
    commit("candidate")
    assert is_docs_only(base) is expected


def test_rename_from_source_to_docs_requires_full_ci(base):
    Path("contributing").mkdir()
    git("mv", "community/Example.scala", "contributing/Example.md")
    commit("rename source")
    assert not is_docs_only(base)


def test_rename_within_docs_is_docs_only(base):
    Path("release-notes").mkdir()
    git("mv", "UNRELEASED.md", "release-notes/3.7.1.md")
    commit("archive release notes")
    assert is_docs_only(base)


def test_symlink_requires_full_ci(base):
    Path("UNRELEASED.md").unlink()
    Path("UNRELEASED.md").symlink_to("community/Example.scala")
    commit("replace notes with symlink")
    assert not is_docs_only(base)


def test_compares_entire_merge_group_to_base(base):
    write("community/Example.scala", "queued code change\n")
    code_commit = commit("first queued PR")
    write("UNRELEASED.md", "queued docs change\n")
    commit("second queued PR")

    assert is_docs_only(code_commit)
    assert not is_docs_only(base)


def test_unknown_base_cannot_produce_a_docs_only_result(base):
    with pytest.raises(subprocess.CalledProcessError):
        is_docs_only("0" * len(base))
