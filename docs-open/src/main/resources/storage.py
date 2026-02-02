# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Union
from pathlib import Path
import json


def write(file_path: Path, data: str) -> None:
    file_path.write_text(data, encoding='utf-8', newline='\n')


def read(file_path: Path) -> str:
    return file_path.read_text(encoding='utf-8')


def load_json(a_json_file_path: Path):
    return json.loads(a_json_file_path.read_bytes())


def require_all_rst_files(potential_rst_paths: List[Union[str, Path]]) -> List[Path]:
    """
    Asserts that all paths in the list have an '.rst' extension and are an existing file.
    Returns a list of Path objects if all checks pass.
    Raises AssertionError if any path does not comply.
    """
    validated_paths: List[Path] = []
    for rst_path in potential_rst_paths:
        p = Path(rst_path)
        assert p.exists(), f"Path '{p}' does not exist"
        assert p.is_file(), f"Path '{p}' exists but is not a regular file (e.g., it might be a directory)"
        assert p.suffix.lower() == ".rst", \
            f"File '{p}' must have an '.rst' extension, but found suffix '{p.suffix}'"
        validated_paths.append(p)
    return validated_paths


def require_dir_exists(potential_dir: Union[str, Path]) -> Path:
    """
    Asserts the path is an existing directory
    Returns the Path object if all checks pass.
    Raises AssertionError if the path does not comply.
    """
    p = Path(potential_dir)
    assert p.exists(), f"Path '{p}' does not exist"
    assert p.is_dir(), f"Path '{p}' exists but is not a directory"
    return p
