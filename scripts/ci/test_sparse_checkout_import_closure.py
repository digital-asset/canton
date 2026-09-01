"""Guard against sparse-checkout jobs running a script without its imports.

If a workflow's `sparse-checkout` list names a script but omits a module it
imports, the step dies with `ModuleNotFoundError`, which a full local checkout
hides. This asserts every listed `.py` also checks out its transitive sibling
imports (the `sys.path.append(dirname(__file__))` idiom these scripts use).
Third-party, stdlib and dynamic (importlib) imports are out of scope.
"""

import ast
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"


def _find_sparse_checkout_entry_sets(node):
    """Yield each sparse-checkout value (block scalar or list) anywhere in a workflow."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "sparse-checkout":
                if isinstance(value, str):
                    yield [line.strip() for line in value.splitlines() if line.strip()]
                elif isinstance(value, list):
                    yield [str(item).strip() for item in value if str(item).strip()]
            yield from _find_sparse_checkout_entry_sets(value)
    elif isinstance(node, list):
        for item in node:
            yield from _find_sparse_checkout_entry_sets(item)


def _normalise(entry):
    return entry.strip().strip("/").lstrip("./")


def _workflow_sparse_entry_sets(path):
    """Each sparse-checkout block in a workflow, kept separate rather than merged.

    A checkout list is per job, so a dep in one job's list must not cover a script
    that only appears in another's.
    """
    doc = yaml.safe_load(path.read_text())
    return [
        {_normalise(entry) for entry in entry_set}
        for entry_set in _find_sparse_checkout_entry_sets(doc)
    ]


def _local_import_closure(script_path, root):
    """Paths, relative to `root`, of the sibling modules a script imports transitively."""
    root = root.resolve()
    closure = set()
    stack = [script_path.resolve()]
    while stack:
        current = stack.pop()
        if not current.is_file():
            continue
        try:
            tree = ast.parse(current.read_text(), str(current))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            module_names = []
            if isinstance(node, ast.Import):
                module_names = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                module_names = [node.module.split(".")[0]]
            for name in module_names:
                sibling = (current.parent / f"{name}.py").resolve()
                if sibling.is_file():
                    rel = sibling.relative_to(root).as_posix()
                    if rel not in closure:
                        closure.add(rel)
                        stack.append(sibling)
    return closure


def _is_covered(dependency, entries):
    """True when a repo-relative path is listed or sits under a listed directory."""
    return any(dependency == entry or dependency.startswith(entry + "/") for entry in entries)


def test_sparse_checkout_lists_include_local_import_closure():
    violations = []
    for workflow in sorted(WORKFLOWS_DIR.glob("*.yml")):
        for entries in _workflow_sparse_entry_sets(workflow):
            scripts = [entry for entry in entries if entry.endswith(".py")]
            for script in scripts:
                script_path = REPO_ROOT / script
                if not script_path.is_file():
                    continue
                for dependency in sorted(_local_import_closure(script_path, REPO_ROOT)):
                    if not _is_covered(dependency, entries):
                        violations.append(
                            f"{workflow.name}: '{script}' imports '{dependency}', "
                            "which is not in the same sparse-checkout list"
                        )
    assert not violations, "Sparse-checkout jobs missing an imported module:\n" + "\n".join(
        violations
    )


def test_local_import_closure_resolves_sibling_imports_transitively(tmp_path):
    (tmp_path / "leaf.py").write_text("VALUE = 1\n")
    (tmp_path / "mid.py").write_text("import os\nfrom leaf import VALUE\n")
    (tmp_path / "root.py").write_text("import mid\nimport json\n")
    closure = _local_import_closure(tmp_path / "root.py", tmp_path)
    assert closure == {"mid.py", "leaf.py"}


def test_sparse_entry_sets_are_kept_separate_per_block(tmp_path):
    workflow = tmp_path / "wf.yml"
    workflow.write_text(
        "jobs:\n"
        "  a:\n"
        "    steps:\n"
        "      - uses: actions/checkout@v5\n"
        "        with:\n"
        "          sparse-checkout: |\n"
        "            scripts/ci/foo.py\n"
        "  b:\n"
        "    steps:\n"
        "      - uses: actions/checkout@v5\n"
        "        with:\n"
        "          sparse-checkout: |\n"
        "            scripts/ci/bar.py\n"
    )
    assert _workflow_sparse_entry_sets(workflow) == [{"scripts/ci/foo.py"}, {"scripts/ci/bar.py"}]


def test_is_covered_matches_exact_and_directory_prefix():
    entries = {"scripts/ci/foo.py", "scripts/ci/pkg"}
    assert _is_covered("scripts/ci/foo.py", entries)
    assert _is_covered("scripts/ci/pkg/bar.py", entries)
    assert not _is_covered("scripts/ci/other.py", entries)
    assert not _is_covered("scripts/ci/pkg-extra.py", entries)
