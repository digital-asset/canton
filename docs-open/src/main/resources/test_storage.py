# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
import tempfile
from pathlib import Path
from storage import require_all_rst_files, require_dir_exists, write
import re


class TestRequireAllRstFilesMinimal(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_dir.name)

        # Case: Valid RST file
        self.valid_file = self.temp_dir_path / 'document.RST'
        write(self.valid_file, 'Valid RST content.')

        # Case: A directory which exists, but it is not a file
        self.directory_path = self.temp_dir_path / 'a_folder'
        self.directory_path.mkdir()

        # Case: An existing file with the wrong suffix
        self.wrong_suffix_file = self.temp_dir_path / 'document.txt'
        write(self.wrong_suffix_file, 'Not an RST file.')

        # 4. Path for a non-existent file (string version for exact message matching)
        self.non_existent_path_str = str(self.temp_dir_path / 'ghost.rst')

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_empty_list_succeeds(self):
        self.assertEqual(require_all_rst_files([]), [])

    def test_valid_file_succeeds(self):
        # Test with Path object
        result_from_path = require_all_rst_files([self.valid_file])
        self.assertEqual(result_from_path, [self.valid_file])

        # Test with string path
        result_from_str_path = require_all_rst_files([str(self.valid_file)])
        self.assertEqual(result_from_str_path, [self.valid_file])

    def test_fails_if_path_does_not_exist(self):
        with self.assertRaisesRegex(AssertionError, f"Path '{self.non_existent_path_str}' does not exist"):
            require_all_rst_files([self.non_existent_path_str])

    def test_fails_if_path_is_directory(self):
        with self.assertRaisesRegex(AssertionError, f"Path '{self.directory_path}' exists but is not a regular file"):
            require_all_rst_files([self.directory_path])

    def test_fails_if_file_has_wrong_suffix(self):
        with self.assertRaisesRegex(
                AssertionError,
                f"File '{self.wrong_suffix_file}' must have an '.rst' extension, but found suffix '.txt'"
        ):
            require_all_rst_files([self.wrong_suffix_file])


class TestRequireDirExistsMinimal(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_dir_root = Path(self.temp_dir.name)

        # Case: An existing directory
        self.existing_directory = self.temp_dir_root / 'a_folder'
        self.existing_directory.mkdir()

        # Case: An existing file
        self.existing_file = self.temp_dir_root / 'a_file.txt'
        write(self.existing_file, 'This is a file.')

        # Case: Path string for a non-existent entry
        self.non_existent_path_str = str(self.temp_dir_root / 'non_existent_dir')

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_existing_directory_succeeds(self):
        # Test with Path object
        result_from_path = require_dir_exists(self.existing_directory)
        self.assertEqual(result_from_path, self.existing_directory)

        # Test with string path
        result_from_str_path = require_dir_exists(str(self.existing_directory))
        self.assertEqual(result_from_str_path, self.existing_directory)

    def test_fails_if_path_does_not_exist(self):
        expected_msg = f"Path '{self.non_existent_path_str}' does not exist"
        with self.assertRaisesRegex(AssertionError, re.escape(expected_msg)):
            require_dir_exists(self.non_existent_path_str)

    def test_fails_if_path_is_file_not_directory(self):
        expected_msg = f"Path '{self.existing_file}' exists but is not a directory"
        with self.assertRaisesRegex(AssertionError, re.escape(expected_msg)):
            require_dir_exists(self.existing_file)


if __name__ == '__main__':
    unittest.main()
