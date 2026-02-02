# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
This script replaces the snippet directives in-place with the pre-recorded snippet output code blocks.

Usage:
    snippet_directive_replace.py SNIPPET_DATA_DIRECTORY RST_FILE...

Where:
    SNIPPET_DATA_DIRECTORY
        Path to the directory containing snippet data.

    RST_FILE
        Absolute paths to RST files. Only files whose paths
        contain 'docs-open/target/preprocessed-sphinx/' are valid.
"""
import re
import sys
from pathlib import Path
import storage

# matches our custom snippet directive until there is an empty line
SNIPPET_REGEX = re.compile(r'\.{2} snippet.*(?:\n.+)*[^\n]', re.MULTILINE)


def process_snippet_directive(rst_file: Path, snippet_data_file: Path) -> None:
    # load snippet data
    try:
        code_blocks = storage.load_json(snippet_data_file)
    except Exception as e:
        print(f'FAILED TO READ {snippet_data_file} because of {repr(e)}')
        raise e

    original = storage.read(rst_file)

    _assert_snippet_and_code_block_count(rst_file, original, snippet_data_file, code_blocks)

    updated = _replace_snippets_with_code_blocks(original, code_blocks)

    storage.write(rst_file, updated)


def _assert_snippet_and_code_block_count(rst_file: Path,
                                         rst_content: str,
                                         snippet_data_file: Path,
                                         code_blocks: list[str]) -> None:
    snippet_count = len(SNIPPET_REGEX.findall(rst_content))
    code_block_count = len(code_blocks)
    assert snippet_count == code_block_count, \
        f'Expect same number of snippet directives (found {snippet_count}) in {rst_file} ' \
        f'as there are code blocks (found {code_block_count}) resulting from {snippet_data_file}.\n\n' \
        f'Possible Resolutions:\n' \
        f'-> Check that there is at least one empty line between snippets and following text.\n' \
        f'-> Generated snippet output data may need to be updated.\n'


def _replace_snippets_with_code_blocks(original_rst: str, code_blocks: list[str]) -> str:
    code_blocks_iterator = iter(code_blocks)
    return SNIPPET_REGEX.sub(lambda _: next(code_blocks_iterator), original_rst)


def _get_relative_path_split(rst_absolute_path: Path) -> Path:
    """
    Extracts the relative path part after 'docs-open/target/preprocessed-sphinx/'.
    Uses string splitting. Throws if the marker is not found.
    """
    marker = 'docs-open/target/preprocessed-sphinx/'
    native_path_string = str(Path(marker))  # Convert to OS-native path string if needed, that is Windows
    parts = str(rst_absolute_path).split(native_path_string, 1)  # Split only once

    if len(parts) == 2:
        path_str: str = parts[1]
        rst_file_path = Path(path_str)

        if rst_file_path.is_absolute():
            # relative_to('/') gives the path components after the root
            return rst_file_path.relative_to(rst_file_path.anchor)
        else:
            # If it's not absolute, it doesn't start with '/' (or drive letter on Windows)
            return rst_file_path
    else:
        raise Exception(f'Absolute RST file path {rst_absolute_path} does not contain expected marker {marker}')


if __name__ == '__main__':

    if len(sys.argv) >= 3:
        snippet_data_dir: Path = storage.require_dir_exists(sys.argv[1])
        rst_files: list[Path] = storage.require_all_rst_files(sys.argv[2:])

        for rst in rst_files:
            relative_path: Path = _get_relative_path_split(rst)
            snippet_json_file: Path = snippet_data_dir / relative_path.with_suffix('.json')
            if snippet_json_file.exists():
                process_snippet_directive(Path(rst), snippet_json_file)
    else:
        raise Exception(f'Unexpected number of arguments, found {len(sys.argv)}')
