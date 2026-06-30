"""Tests for release/update_dependabot.py.

Run against the real .github/dependabot.yml (not an inline copy) so format drift
is caught here. They assert invariants: coverage matches the supported-lines
manifest exactly, lines not in the manifest are ramped down, base entries are
preserved verbatim, and the result is idempotent.
"""

import re
import textwrap

import pytest

import update_dependabot as ud

REAL = ud.DEFAULT_FILE.read_text()

# Classify on the `target-branch:` key (independent of the module) so the test
# oracle does not share any substring blind spot.
_TARGET_BRANCH_KEY = re.compile(r"^    target-branch:", re.MULTILINE)


def _entries(text):
    body = text.split("\nupdates:\n", 1)[1]
    return re.split(r"\n\n+(?=  - package-ecosystem:)", body.strip("\n"))


def _ecosystem(entry):
    return re.search(r'package-ecosystem: "(.+)"', entry).group(1)


def _base(text):
    return [e for e in _entries(text) if not _TARGET_BRANCH_KEY.search(e)]


def _per_line(text):
    return [e for e in _entries(text) if _TARGET_BRANCH_KEY.search(e)]


def _targets(entry):
    return re.search(r'target-branch: "(.+)"', entry).group(1)


# --- manifest parsing -------------------------------------------------------

def test_parse_manifest_ignores_comments_and_blanks_and_sorts():
    manifest = "# header\n\n3.6\n3.5  # trailing comment\n3.5\n"
    assert ud.parse_manifest(manifest) == ["3.5", "3.6"]  # de-duped, numeric sort


def test_parse_manifest_sorts_numerically_not_lexically():
    assert ud.parse_manifest("3.9\n3.10\n3.2\n") == ["3.2", "3.9", "3.10"]


@pytest.mark.parametrize("bad", ["3", "foo", "3.x", "3.6.0", "release-line-3.5"])
def test_parse_manifest_rejects_malformed(bad):
    with pytest.raises(SystemExit):
        ud.parse_manifest(bad + "\n")


# --- rewrite (manifest-driven coverage) -------------------------------------

def test_covers_each_supported_line_for_configured_ecosystems_only():
    out = ud.rewrite(REAL, ["3.5", "3.6"])
    for line in ("release-line-3.5", "release-line-3.6"):
        ecos = sorted(_ecosystem(e) for e in _per_line(out) if _targets(e) == line)
        assert ecos == sorted(ud.PER_LINE_ECOSYSTEMS)  # docker only for now


def test_per_line_ecosystems_are_a_subset_of_base():
    # Coverage can only clone ecosystems that exist as base (main) entries.
    base_ecos = {_ecosystem(e) for e in _base(REAL)}
    assert set(ud.PER_LINE_ECOSYSTEMS) <= base_ecos


def test_covers_exactly_the_supported_lines():
    out = ud.rewrite(REAL, ["3.6"])
    assert {_targets(e) for e in _per_line(out)} == {"release-line-3.6"}


def test_ramps_down_lines_not_in_manifest():
    # REAL currently carries release-line-3.5 entries; syncing to [9.9] drops them.
    out = ud.rewrite(REAL, ["9.9"])
    assert "release-line-3.5" not in out


def test_empty_manifest_leaves_only_base_entries():
    out = ud.rewrite(REAL, [])
    assert _per_line(out) == []
    assert _base(out) == _base(REAL)


def test_per_line_entries_are_faithful_clones_of_covered_base():
    out = ud.rewrite(REAL, ["9.9"])
    stripped = [
        re.sub(r'\n    target-branch: ".*?"', "", e, count=1) for e in _per_line(out)
    ]
    covered_base = [e for e in _base(REAL) if _ecosystem(e) in ud.PER_LINE_ECOSYSTEMS]
    assert sorted(stripped) == sorted(covered_base)


def test_idempotent():
    once = ud.rewrite(REAL, ["3.5", "3.6"])
    assert ud.rewrite(once, ["3.5", "3.6"]) == once


def test_base_entry_mentioning_target_branch_in_comment_is_not_dropped():
    # Regression: classification keys off the `target-branch:` key, not the bare
    # substring, so an entry that mentions it in a comment is still a base entry.
    doc = textwrap.dedent(
        '''\
        version: 2
        updates:
          - package-ecosystem: "docker"
            directory: "/x"
            schedule:
              interval: "daily"  # mentions target-branch: in a comment
        '''
    )
    out = ud.rewrite(doc, ["9.9"])
    assert out.count('package-ecosystem: "docker"') == 2  # base kept and cloned
    assert out.count('target-branch: "release-line-9.9"') == 1


# --- CLI --------------------------------------------------------------------

def test_cli_syncs_file_from_manifest(tmp_path):
    f = tmp_path / "dependabot.yml"
    f.write_text(REAL)
    m = tmp_path / "supported.txt"
    m.write_text("3.5\n3.6\n")
    ud.main(["--manifest", str(m), "--file", str(f)])
    assert {_targets(e) for e in _per_line(f.read_text())} == {
        "release-line-3.5",
        "release-line-3.6",
    }


def test_cli_rejects_malformed_manifest(tmp_path):
    f = tmp_path / "dependabot.yml"
    f.write_text(REAL)
    m = tmp_path / "supported.txt"
    m.write_text("3\nfoo\n")
    with pytest.raises(SystemExit):
        ud.main(["--manifest", str(m), "--file", str(f)])
    assert f.read_text() == REAL  # untouched on bad input
