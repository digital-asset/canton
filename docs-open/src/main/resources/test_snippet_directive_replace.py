# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
import textwrap
from io import StringIO
from json import JSONDecodeError
from pathlib import Path
from unittest.mock import patch, mock_open

from snippet_directive_replace import SNIPPET_REGEX, _replace_snippets_with_code_blocks, \
    _assert_snippet_and_code_block_count, process_snippet_directive


class SnippetDirectiveProcessing(unittest.TestCase):

    @patch('sys.stdout', new_callable=StringIO)
    @patch('pathlib.Path.open', new_callable=mock_open, read_data='{ "invalid": "JSON"!!!}')
    def test_failing_to_read_snippet_data(self, mock_file, fake_output):
        self.assertRaises(JSONDecodeError, process_snippet_directive, Path('test.rst'), Path('test.json'))

        mock_file.assert_called_once()
        expected_stdout = 'FAILED TO READ test.json because of ' \
                          'JSONDecodeError("Expecting \',\' delimiter: line 1 column 20 (char 19)")'
        self.assertEqual(expected_stdout, fake_output.getvalue().strip())


class SnippetRegex(unittest.TestCase):

    def test_regex(self):
        valid_snippet = textwrap.dedent('''\
        .. snippet:: getting_started
            .. success:: Seq(1,2,3).map(_ * 2)
            .. assert:: RES.length == 3
        ''')

        self.assertTrue(SNIPPET_REGEX.match(valid_snippet))


class ReplaceSnippetsWithCodeBlocks(unittest.TestCase):

    def test_replace_snippets_with_code_blocks(self):
        original = textwrap.dedent('''\
        Some awesome text.

        .. snippet:: first_snippet

        Some more awesome text.

        .. snippet:: second_snippet

        Even more awesome text.

        ''')

        expectation = textwrap.dedent('''\
        Some awesome text.

        10
        20

        Some more awesome text.

        30
        40
        50

        Even more awesome text.

        ''')

        self.assertEqual(expectation,
                         _replace_snippets_with_code_blocks(original, ['10\n20', '30\n40\n50']))

    def test_unequal_number_of_snippet_matches_and_code_blocks(self):
        original = textwrap.dedent('''\
        Some awesome text.

        .. snippet:: first_snippet
        Some more awesome text.
        .. snippet:: second_snippet

        Even more awesome text.

        ''')

        with self.assertRaises(AssertionError) as cm:
            _assert_snippet_and_code_block_count(Path('test.rst'), original, Path('test.json'), ['10\n20', '30\n40\n50'])
        self.assertEqual('Expect same number of snippet directives (found 1) in test.rst '
                         'as there are code blocks (found 2) resulting from test.json.\n\n'
                         'Possible Resolutions:\n'
                         '-> Check that there is at least one empty line between snippets and following text.\n'
                         '-> Generated snippet output data may need to be updated.\n',
                         str(cm.exception))


if __name__ == '__main__':
    unittest.main()
