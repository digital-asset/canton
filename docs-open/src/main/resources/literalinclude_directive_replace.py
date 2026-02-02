# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""\
This script replaces the RST literalinclude directives.

Usage:
    literalinclude_directive_replace.py REPOSITORY_ROOT RST_FILE...

Where:
    REPOSITORY_ROOT
        Path to the Canton repository root directory.

    RST_FILE
        Absolute paths to RST files. Only files whose paths
        contain 'docs-open/target/preprocessed-sphinx/' are valid.
"""
import re
import sys
from pathlib import Path
import textwrap
import ast
import storage


class LiteralInclude:
    """
        Represent a RST `.. literalinclude::` directive
    """

    def __init__(self, a_file: Path, indent=''):
        self.file = a_file
        self.language = 'none'
        self.caption = None
        self.start_after = None
        self.end_before = None
        self.dedent = False
        self.pyobject = None
        self.indent = indent
        self.append = None

    def resolve(self):
        def get_content():
            file_content = storage.read(self.file)
            if self.pyobject:
                return extract_pyobject(file_content)
            assert bool(self.start_after) == bool(self.end_before), \
                'Either the start_after and end_before are both set or not. No in-between allowed'
            if not self.start_after:
                return map(lambda a_line: self.indent + '    ' + a_line, file_content.splitlines(keepends=True))
            else:
                # easier than regex in this case
                res = []
                opened = False
                for line in file_content.splitlines(keepends=True):
                    if self.start_after in line:
                        opened = True
                    elif self.end_before in line:
                        break
                    elif opened:
                        res.append(self.indent + '    ' + line)
                assert res, \
                    f"No lines found between start_after '{self.start_after}' and end_before " \
                    f"'{self.end_before}'. Do these markers exist in '{self.file}'?"
                return res

        # Extracts python source code from a function or class name referenced in the ::pyobject:: option
        # Only works for syntactically valid python files
        def extract_pyobject(file_content):
            tree = ast.parse(file_content)
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name == self.pyobject:
                    start_line = node.lineno - 1
                    end_line = node.end_lineno
                    return map(lambda a_line: self.indent + '    ' + a_line, file_content.splitlines(keepends=True)[start_line:end_line])
            raise ValueError(f"Object '{self.pyobject}' not found in '{self.file}'")

        # Automatically set language to python if not set and file extension is .py
        if self.language == 'none':
            if self.file.suffix.lower == '.py':
                self.language = 'python'
            elif self.file.suffix.lower == '.sh':
                self.language = 'shell'
            elif self.file.suffix.lower == '.proto':
                self.language = 'protobuf'

        to_return = [f'{self.indent}.. code-block:: {self.language}\n']
        if self.caption:
            to_return.append(f'{self.indent}   :caption: {self.caption}\n')
        to_return.append('\n')
        content_lines = get_content()
        content = ''.join(content_lines)
        if self.dedent:
            dedent_text = textwrap.dedent(content)
            content = textwrap.indent(dedent_text, self.indent + "    ")
        to_return.append(content)
        if self.append:
            to_return.append(self.indent + '    ' + self.append + '\n')
        to_return.append('\n')
        return to_return


# Process literal includes, enabling RST files to build Canton docs without Canton source files
def process_literalinclude_directive(a_repo_root: Path, a_root: Path, a_file: Path) -> None:
    new_lines = []
    curr_directive = None
    rst_file = Path(a_root, a_file)
    original = storage.read(rst_file)

    for line in original.splitlines():
        stripped = line.strip()
        indent = line[:len(line) - len(stripped)]
        if not curr_directive and stripped.startswith('.. literalinclude::'):
            file_to_include: str = re.search('literalinclude:: (.*)', line).group(1)
            file_path: Path = _resolve_canton_sources(a_repo_root, a_root, file_to_include)
            curr_directive = LiteralInclude(file_path, indent)
        elif not curr_directive:  # by far most common case
            new_lines.append(line)
            new_lines.append('\n')
        elif curr_directive:
            if stripped.startswith('.. literalinclude::'):
                new_lines.extend(curr_directive.resolve())
                file_to_include = re.search('literalinclude:: (.*)', line).group(1)
                file_path: Path = _resolve_canton_sources(a_repo_root, a_root, file_to_include)
                curr_directive = LiteralInclude(file_path, indent)
            elif stripped.startswith(':language:'):
                language = re.search(':language: (.*)', stripped).group(1)
                curr_directive.language = language
            elif stripped.startswith(':caption:'):
                caption = re.search(':caption: (.*)', stripped).group(1)
                curr_directive.caption = caption
            elif stripped.startswith(':start-after:'):
                start_after = re.search(':start-after: (.*)', stripped).group(1)
                curr_directive.start_after = start_after
            elif stripped.startswith(':end-before:'):
                end_before = re.search(':end-before: (.*)', stripped).group(1)
                curr_directive.end_before = end_before
            elif stripped.startswith(':dedent:'):
                curr_directive.dedent = True
            elif stripped.startswith(':pyobject:'):
                pyobject = re.search(':pyobject: (.*)', stripped).group(1)
                curr_directive.language = "python"
                curr_directive.pyobject = pyobject
            elif stripped.startswith(':append:'):
                append = re.search(':append: (.*)', stripped).group(1)
                curr_directive.append = append
            else:
                new_lines.extend(curr_directive.resolve())
                new_lines.append(line)
                new_lines.append('\n')
                curr_directive = None

    # special case where literalinclude is last line
    if curr_directive:
        new_lines.extend(curr_directive.resolve())

    new_text = ''.join(new_lines)
    storage.write(rst_file, new_text)
    if '.. literalinclude::' in new_text:
        raise Exception('A literalinclude directive has not been replaced in file ', rst_file)


def _resolve_canton_sources(a_repo_root: Path, a_root: Path, file_to_include: str) -> Path:
    if file_to_include.startswith('CANTON/'):
        file_to_include_cut: str = file_to_include[len('CANTON/'):]
        return Path(f'{a_repo_root}/{file_to_include_cut}')
    else:
        return Path(f'{a_root}/{file_to_include}')


if __name__ == '__main__':

    if len(sys.argv) >= 3:
        repo_root: Path = storage.require_dir_exists(sys.argv[1])
        rst_files: list[Path] = storage.require_all_rst_files(sys.argv[2:])
        for rst_path in rst_files:
            process_literalinclude_directive(repo_root, rst_path.parent, Path(rst_path.name))

    else:
        raise Exception(f'Unexpected number of arguments, found {len(sys.argv)}')
