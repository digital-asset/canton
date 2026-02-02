# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from pathlib import Path
from literalinclude_directive_replace import _resolve_canton_sources


class ResolveCantonSource(unittest.TestCase):

    def test_resolving_relative_paths(self):
        self.assertEqual(
            Path('/absolute/<path>/../../../../../../community/app/src/main/resources/repl/banner.txt'),
            _resolve_canton_sources(Path('/canton_repo_path'), Path('/absolute/<path>'),
                                    '../../../../../../community/app/src/main/resources/repl/banner.txt')
        )

    def test_resolving_canton_paths(self):
        self.assertEqual(
            Path('/canton_repo_path/community/app/src/main/resources/repl/banner.txt'),
            _resolve_canton_sources(Path('/canton_repo_path'), Path('/absolute/<path>'),
                                    'CANTON/community/app/src/main/resources/repl/banner.txt')
        )


if __name__ == '__main__':
    unittest.main()
