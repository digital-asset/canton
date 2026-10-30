#!/usr/bin/env python3
"""Guard that every except-block in scripts/ci notifies someone, fails loudly, or says why not.

Some scripts here deliberately degrade rather than crash ("never break CI"): catch,
log a warning, carry on. Fine when genuinely harmless, but the same shape can silently
eat a real problem too (see notify_tooling_failure()). Every except-block in
scripts/ci/**/*.py (except test_*.py files and test/self-test functions) must do one of:

  - call a function whose name contains "notify", or sit inside a function whose own
    name does (the repo's "this already pages a human" convention)
  - raise (other than a `raise SystemExit` that exits successfully, e.g. status 0),
    call sys.exit/exit/quit/os._exit with any status that is not a literal 0, False or
    None, or return a non-zero integer literal
  - carry a `# silent-ok: <reason>` comment anywhere in the handler, when the error is
    genuinely ignored
  - carry a `# failure-surfaced: <where>` comment anywhere in the handler, when the
    failure is reported somewhere the checker cannot see (e.g. it sets an exit code or
    appends to a list of errors that is reported later), the reason says where

Anything else is reported (file:line) and the script exits non-zero.

Deliberately local: it checks each handler's own statements, not whether some later
`return` elsewhere in the function also makes the failure loud. A checker that tries to
prove that by tracing whole functions is more likely to have false negatives of its own.

Run locally with:  python3 scripts/ci/check_guarded_excepts.py
"""

import ast
import io
import re
import sys
import textwrap
import tokenize
from pathlib import Path

import pytest

CI_SCRIPTS_DIR = Path(__file__).resolve().parent
SILENT_OK_MARKER = "silent-ok:"
SURFACED_MARKER = "failure-surfaced:"
# Requires at least one non-whitespace character after the marker, so a bare
# "# silent-ok:" with no reason does not count.
_MARKER_RE = re.compile(
    "(?:" + "|".join(re.escape(m) for m in (SILENT_OK_MARKER, SURFACED_MARKER)) + r")\s*\S"
)

# Function-name prefixes whose body is test/self-check scaffolding, not a production
# guard path, matched against the *start* of the name (so _run_self_tests, a plural
# variant, is still covered by _run_self_test).
TEST_FUNCTION_PREFIXES = (
    "test_",
    "_test_",
    "self_test",
    "_run_self_test",
)


def _is_test_function(name: str) -> bool:
    return any(name.startswith(prefix) for prefix in TEST_FUNCTION_PREFIXES)


def _is_notify_function(name: str) -> bool:
    """A function whose own name already marks it as the notifier (same "notify" in
    name convention _handles_failure uses for calls), so every except-block inside it
    is already part of a best-effort notification attempt and needs no extra comment."""
    return "notify" in name


def _call_name(node: ast.AST) -> str | None:
    """The bare name of a Call's callee, e.g. "notify_tooling_failure" for a direct
    call. None for anything else."""
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _is_nonzero_int_literal(node: ast.AST) -> bool:
    # `-1` parses as UnaryOp(USub, Constant(1)), not as a Constant.
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        node = node.operand
    return (
        isinstance(node, ast.Constant)
        and isinstance(node.value, int)
        and not isinstance(node.value, bool)
        and node.value != 0
    )


def _is_nonzero_status(node: ast.AST) -> bool:
    """True for any exit status other than a literal 0, False or None. Whether an
    arbitrary expression evaluates to non-zero is undecidable, so a non-literal is
    given the benefit of the doubt (`sys.exit(code)` is not worth a marker)."""
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        node = node.operand
    return not (isinstance(node, ast.Constant) and node.value in (0, False, None))


def _is_loud_exit_call(node: ast.AST) -> bool:
    """True for sys.exit/os._exit/bare exit()/quit() with a status that is not a
    literal 0, False or None. Checks the receiver (so some_object.exit(0) isn't
    mistaken for sys.exit), a missing status does not count as loud."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        if func.id not in ("exit", "quit"):
            return False
    elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        if not (
            (func.value.id == "sys" and func.attr == "exit")
            or (func.value.id == "os" and func.attr == "_exit")
        ):
            return False
    else:
        return False
    return bool(node.args) and _is_nonzero_status(node.args[0])


def _is_loud_raise(node: ast.Raise) -> bool:
    """Loud unless this raises SystemExit with no status or a literal 0, False or None
    status (same rule as _is_loud_exit_call). A message string is a non-zero status:
    it is printed to stderr and exits 1."""
    exc = node.exc
    if exc is None:
        return True  # bare `raise`, re-raises the active exception
    if isinstance(exc, ast.Name) and exc.id == "SystemExit":
        return False  # `raise SystemExit`, no call at all -> exits 0
    if isinstance(exc, ast.Call) and isinstance(exc.func, ast.Name) and exc.func.id == "SystemExit":
        if not exc.args:
            return False
        return _is_nonzero_status(exc.args[0])
    return True


def _walk_handler_body(root: ast.ExceptHandler):
    """Like ast.walk, but only visits code the handler itself runs.

    Not descended into, a nested function (defined here, never called):

        except OSError:
            def later():
                raise  # does not make this handler loud
            log("ignored")

    Not descended into, a nested handler (it only runs if the cleanup fails, so it
    must pass the rule on its own):

        except OSError:
            try:
                cleanup()
            except OSError:
                sys.exit(1)  # does not make the outer handler loud
            log("ignored")

    Still descended into: a nested try's own body, so an unconditional
    `notify_tooling_failure(...)` there counts."""
    stack = [root]
    while stack:
        current = stack.pop()
        yield current
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        if isinstance(current, ast.ExceptHandler) and current is not root:
            continue
        stack.extend(ast.iter_child_nodes(current))


def _handles_failure(handler: ast.ExceptHandler) -> bool:
    """True if ``handler``'s own statements notify someone or fail loudly (see the
    module docstring for exactly which shapes count)."""
    for node in _walk_handler_body(handler):
        if isinstance(node, ast.Raise) and _is_loud_raise(node):
            return True
        name = _call_name(node)
        if name and "notify" in name:
            return True
        if _is_loud_exit_call(node):
            return True
        if isinstance(node, ast.Return) and _is_nonzero_int_literal(node.value):
            return True
    return False


def _comment_texts_by_line(source: str) -> dict[int, str]:
    """Map line number -> comment text, for actual `#`-comment tokens only (so a
    string literal that happens to contain the marker text does not count)."""
    comments: dict[int, str] = {}
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.COMMENT:
            comments[tok.start[0]] = tok.string
    return comments


def _has_silent_ok_comment(handler: ast.ExceptHandler, comments_by_line: dict[int, str]) -> bool:
    """True if a `# silent-ok:` or `# failure-surfaced:` comment sits in the handler's own lines. Lines of a nested
    function/lambda or nested except-handler don't count (same scope as
    _walk_handler_body), so an inner marker can't exempt the handler around it."""
    start, end = handler.lineno, handler.end_lineno or handler.lineno
    own_lines = set(range(start, end + 1))
    for node in ast.walk(handler):
        if node is handler:
            continue
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ExceptHandler)):
            own_lines -= set(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    return any(
        _MARKER_RE.search(comments_by_line[line]) for line in own_lines if line in comments_by_line
    )


def _iter_handlers(node: ast.AST, skip: bool = False):
    """Yield every ExceptHandler in ``node``, skipping test/self-test functions and
    notify-named functions (and anything nested inside either)."""
    for child in ast.iter_child_nodes(node):
        child_skip = skip
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            child_skip = skip or _is_test_function(child.name) or _is_notify_function(child.name)
        if isinstance(child, ast.ExceptHandler) and not skip:
            yield child
        yield from _iter_handlers(child, skip=child_skip)


def _find_violations_in_source(source: str, filename: str) -> list[str]:
    """Problems in ``source``, as printable "filename:line: message" strings. Takes
    text directly (not a path) so tests can check a snippet without touching disk."""
    tree = ast.parse(source, filename=filename)
    comments_by_line = _comment_texts_by_line(source)
    problems = []
    for handler in _iter_handlers(tree):
        if _handles_failure(handler):
            continue
        if _has_silent_ok_comment(handler, comments_by_line):
            continue
        problems.append(
            f"{filename}:{handler.lineno}: except-block neither notifies, fails loudly, "
            f"nor has a '# {SILENT_OK_MARKER} <reason>' or '# {SURFACED_MARKER} <where>' comment"
        )
    return problems


def find_violations(path: Path) -> list[str]:
    """Problems in a single file on disk, see _find_violations_in_source."""
    return _find_violations_in_source(path.read_text(encoding="utf-8"), str(path))


def check_tree(directory: Path) -> list[str]:
    problems = []
    for path in sorted(directory.rglob("*.py")):
        if path.name.startswith("test_"):
            continue
        problems.extend(find_violations(path))
    return problems


def main() -> None:
    problems = check_tree(CI_SCRIPTS_DIR)
    if problems:
        print(
            "Found except-block(s) that silently swallow a failure. Each one must "
            "either call a *notify*-named function, fail loudly (raise / sys.exit / "
            "return a non-zero int), carry a '# silent-ok: <reason>' comment "
            "explaining why swallowing it is fine, or carry a "
            "'# failure-surfaced: <where>' comment saying where it is reported:",
            file=sys.stderr,
        )
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        sys.exit(1)
    print(f"Every except-block in {CI_SCRIPTS_DIR} notifies, fails loudly, or is marked.")


# --- tests -------------------------------------------------------------------
# pytest runs the test_* functions below. scripts/pyproject.toml sets
# python_files = ["*.py"], so every module under scripts/ci is collected, not only
# test_*.py, which is why the tests live in this file.


def _violations_in_source(source: str) -> list[str]:
    return _find_violations_in_source(textwrap.dedent(source), filename="<test>")


def _violations_for_handler(handler_body: str) -> list[str]:
    """Problems for a minimal function whose except-handler is ``handler_body``."""
    return _violations_in_source(
        f"""
        def f():
            try:
                g()
            except Exception as e:
                {handler_body}
        """
    )


@pytest.mark.parametrize(
    ("handler_body", "expect_clean"),
    [
        ("print('oops')", False),
        ("notify_tooling_failure(1)", True),
        ("_notify_step_failure('x')", True),
        ("raise RuntimeError('x') from e", True),
        ("raise SystemExit", False),  # exits 0, not a failure
        ("raise SystemExit()", False),  # exits 0
        ("raise SystemExit(0)", False),  # exits 0
        ("raise SystemExit(1)", True),
        ("raise SystemExit(f'error: {e}')", True),  # a message exits 1
        ("raise SystemExit('error')", True),
        ("sys.exit(1)", True),
        ("sys.exit(0)", False),  # exits 0
        ("sys.exit()", False),  # no status -> exits 0
        ("sys.exit(status)", True),  # non-literal, given the benefit of the doubt
        ("sys.exit(None)", False),  # exits 0
        ("sys.exit(False)", False),  # exits 0
        ("exit(code)", True),
        ("quit(len(errors))", True),
        ("os._exit(code)", True),
        ("raise SystemExit(code)", True),
        ("raise SystemExit(None)", False),  # exits 0
        ("os._exit(1)", True),
        ("some_object.exit(0)", False),  # not sys/os, just a same-named method
        ("return 2", True),
        ("return -1", True),  # unary minus on a literal is still provably non-zero
        ("sys.exit(-1)", True),
        ("raise SystemExit(-1)", True),
        ("return -0", False),
        ("return 0", False),
        ("return _fail('x')", False),  # a call result, not a literal
        ("pass  # silent-ok: harmless", True),
        ("pass  # silent-ok:", False),  # no reason after the marker
        ("pass  # failure-surfaced: sets exit_code below", True),
        ("pass  # failure-surfaced:", False),  # no reason after the marker
        ('print("silent-ok: ignored")', False),  # marker in a string, not a comment
        ('print("failure-surfaced: x")', False),  # same for the second marker
    ],
)
def test_violations_for_handler_body(handler_body, expect_clean):
    assert (_violations_for_handler(handler_body) == []) == expect_clean


def test_violation_message_does_not_leak_the_handler_body():
    assert "oops" not in _violations_for_handler("print('oops')")[0]


def test_rejects_an_unreachable_raise_in_a_nested_function():
    # A raise inside a nested function the handler defines but never calls is not
    # executed by the handler itself.
    problems = _violations_in_source(
        """
        def f():
            try:
                g()
            except Exception:
                def _unused():
                    raise RuntimeError('never runs')
                log('oops')
        """
    )
    assert len(problems) == 1


def test_rejects_notify_only_in_a_nested_except_clause():
    # A notify call inside a *nested* except clause only runs if that nested cleanup
    # itself fails, it does not mean the outer handler reported the original failure
    # when the cleanup succeeds, so it must not satisfy the outer handler on its own.
    problems = _violations_in_source(
        """
        def f():
            try:
                g()
            except Exception:
                try:
                    cleanup()
                except Exception:
                    notify_tooling_failure('cleanup failed')
        """
    )
    assert len(problems) == 1


def test_silent_ok_in_a_nested_except_does_not_exempt_the_outer_handler():
    problems = _violations_in_source(
        """
        def f():
            try:
                g()
            except Exception:
                cleanup()
                try:
                    h()
                except Exception:  # silent-ok: inner only
                    pass
        """
    )
    assert len(problems) == 1


def test_silent_ok_in_a_nested_function_does_not_exempt_the_handler():
    problems = _violations_in_source(
        """
        def f():
            try:
                g()
            except Exception:
                def _unused():
                    pass  # silent-ok: never runs
                log('oops')
        """
    )
    assert len(problems) == 1


def test_accepts_notify_attempted_in_a_nested_try_body():
    # The notify call itself is attempted unconditionally in the nested try's body,
    # only a failure of *that* attempt is caught by the nested except, so the outer
    # handler is still satisfied. The inner handler is its own independent check
    # (marked silent-ok here, matching the real notify_tooling_failure pattern), this
    # test is only about whether the outer handler sees the unconditional attempt.
    assert (
        _violations_in_source(
            """
            def f():
                try:
                    g()
                except Exception:
                    try:
                        notify_tooling_failure('oops')
                    except Exception:
                        log('even notifying failed')  # silent-ok: nothing left to do
            """
        )
        == []
    )


def test_skips_except_blocks_inside_a_notify_named_function():
    # Every statement inside a notify-named function is already part of a
    # best-effort notification attempt, no per-handler comment needed.
    assert (
        _violations_in_source(
            """
            def _notify_something():
                try:
                    g()
                except Exception:
                    print('oops')
            """
        )
        == []
    )


def test_skips_test_named_functions():
    assert (
        _violations_in_source(
            """
            def test_f():
                try:
                    g()
                except Exception:
                    print('oops')
            """
        )
        == []
    )
    assert (
        _violations_in_source(
            """
            def self_test():
                try:
                    g()
                except Exception:
                    print('oops')
            """
        )
        == []
    )


def test_skips_nested_functions_inside_a_test_function():
    assert (
        _violations_in_source(
            """
            def test_f():
                def inner():
                    try:
                        g()
                    except Exception:
                        print('oops')
            """
        )
        == []
    )


def test_skips_test_files_entirely():
    # check_tree() filters by filename, not by find_violations() directly, so exercise
    # that filter rather than find_violations().
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        (directory / "test_example.py").write_text(
            "def f():\n    try:\n        g()\n    except Exception:\n        print('oops')\n",
            encoding="utf-8",
        )
        assert check_tree(directory) == []


def test_scans_subdirectories():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        sub = Path(tmp) / "nested"
        sub.mkdir()
        (sub / "example.py").write_text(
            "def f():\n    try:\n        g()\n    except Exception:\n        print('oops')\n",
            encoding="utf-8",
        )
        assert len(check_tree(Path(tmp))) == 1


def test_real_tree_is_clean():
    """The shipped scripts/ci/**/*.py must actually satisfy this check."""
    assert check_tree(CI_SCRIPTS_DIR) == []


if __name__ == "__main__":
    main()
