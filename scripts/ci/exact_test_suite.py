"""Resolve an explicitly requested suite against the compiled test catalogue."""

import os
import re
from pathlib import Path


def exact_pattern(suite: str, catalogue: Path) -> str:
    names = [line.strip() for line in catalogue.read_text().splitlines() if line.strip()]
    if names.count(suite) != 1:
        raise ValueError(f"Expected exactly one compiled suite named {suite!r}")
    return f"^{re.escape(suite)}$"


if __name__ == "__main__":
    pattern = exact_pattern(os.environ["TEST_SUITE"], Path(os.environ["TEST_FILE"]))
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"include-pattern={pattern}\n")
