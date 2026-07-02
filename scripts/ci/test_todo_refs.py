"""Tests for the shared TODO-reference utility (scripts/ci/todo_refs.py).

Run from scripts/ci with:  python3 -m unittest test_todo_refs

Uses only the standard library so it runs in the repo's nix shell without an
extra pytest dependency.
"""

import tempfile
import unittest
from pathlib import Path

import todo_refs


class TodoRefsTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def _write(self, relpath, text):
        path = self.root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def _has(self, number):
        return todo_refs.has_todo_reference(number, root=str(self.root))

    # --- matching forms ---

    def test_short_hash_form(self):
        self._write("a.scala", "// TODO(#123): fix later\n")
        self.assertTrue(self._has(123))

    def test_i_form(self):
        self._write("a.scala", "// TODO(i456): fix later\n")
        self.assertTrue(self._has(456))

    def test_url_form(self):
        self._write("a.md", "<!-- TODO https://github.com/DACH-NY/canton/issues/789 -->\n")
        self.assertTrue(self._has(789))

    def test_url_form_without_https_prefix(self):
        self._write("a.md", "<!-- TODO DACH-NY/canton/issues/789 -->\n")
        self.assertTrue(self._has(789))

    def test_reference_anywhere_on_line(self):
        self._write("a.scala", "// TODO: revisit this once #321 is resolved\n")
        self.assertTrue(self._has(321))

    def test_xxx_and_fixme_keywords(self):
        self._write("a.scala", "// XXX(#11) hack\n// FIXME #22\n")
        self.assertTrue(self._has(11))
        self.assertTrue(self._has(22))

    # --- boundaries / negatives ---

    def test_number_boundary_no_substring_match(self):
        # #123 must not match inside #1234.
        self._write("a.scala", "// TODO see #1234\n")
        self.assertFalse(self._has(123))

    def test_i_form_requires_non_alphanumeric_boundary(self):
        # "api456" must not be read as an "i456" reference.
        self._write("a.scala", "// TODO see api456 for details\n")
        self.assertFalse(self._has(456))

    def test_url_number_boundary(self):
        # issues/789 must not match inside issues/7890.
        self._write("a.md", "<!-- TODO https://github.com/DACH-NY/canton/issues/7890 -->\n")
        self.assertFalse(self._has(789))

    def test_requires_todo_keyword(self):
        # A bare issue reference without a TODO/XXX/FIXME keyword is not a match.
        self._write("a.scala", "// see #123 for context\n")
        self.assertFalse(self._has(123))

    def test_keyword_is_case_sensitive(self):
        self._write("a.scala", "// todo(#123): lowercase keyword is not a TODO\n")
        self.assertFalse(self._has(123))

    # --- excludes ---

    def test_excluded_directory_is_skipped(self):
        self._write("target/generated.scala", "// TODO(#123): leftover\n")
        self.assertFalse(self._has(123))

    def test_excluded_file_glob_is_skipped(self):
        self._write("UNRELEASED.md", "TODO(#123) release note\n")
        self.assertFalse(self._has(123))

    # --- find / multiple ---

    def test_find_returns_matching_lines(self):
        self._write("a.scala", "ok\n// TODO(#123): fix\nmore\n")
        matches = todo_refs.find_todo_references(123, root=str(self.root))
        self.assertEqual(len(matches), 1)
        self.assertIn("TODO(#123)", matches[0])

    def test_multiple_matches_across_files(self):
        self._write("a.scala", "// TODO(#55): one\n")
        self._write("b.scala", "// FIXME #55 two\n")
        matches = todo_refs.find_todo_references(55, root=str(self.root))
        self.assertEqual(len(matches), 2)

    def test_non_numeric_input_rejected(self):
        # The public API must not let a crafted string alter the regex.
        with self.assertRaises(ValueError):
            todo_refs.has_todo_reference("4|5", root=str(self.root))

    # --- exclude emitters ---

    def test_rg_exclude_args_format(self):
        args = todo_refs.rg_exclude_args()
        self.assertTrue(all(a.startswith("--glob=!") for a in args))
        self.assertEqual(
            len(args), len(todo_refs.EXCLUDE_DIRS) + len(todo_refs.EXCLUDE_FILE_GLOBS)
        )

    def test_grep_exclude_args_format(self):
        args = todo_refs.grep_exclude_args()
        dir_args = [a for a in args if a.startswith("--exclude-dir=")]
        file_args = [a for a in args if a.startswith("--exclude=")]
        self.assertEqual(len(dir_args), len(todo_refs.EXCLUDE_DIRS))
        self.assertEqual(len(file_args), len(todo_refs.EXCLUDE_FILE_GLOBS))
        self.assertEqual(len(dir_args) + len(file_args), len(args))


if __name__ == "__main__":
    unittest.main()
