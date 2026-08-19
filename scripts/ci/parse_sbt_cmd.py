#!/usr/bin/env python3
"""Parse and validate an SBT command string into argv tokens.

Prints one token per line to stdout or writes to a file.
Returns exit code 2 on validation or parsing errors.
"""

import argparse
import contextlib
import io
import re
import shlex
import sys
import tempfile


def _fail(message: str) -> int:
    print(message, file=sys.stderr)
    return 2


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Parse and validate an SBT command string into argv tokens.",
        usage="%(prog)s [--output-file FILE] [--cmd-file FILE | CMD]",
    )
    parser.add_argument("cmd", nargs="?", default="", help="The SBT command to parse.")
    parser.add_argument(
        "--cmd-file",
        default="",
        help="Path to a file containing the SBT command to parse.",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Optional output file to write tokens to. If not provided, writes to stdout.",
    )
    args = parser.parse_args(argv)
    if args.cmd and args.cmd_file:
        parser.error("CMD and --cmd-file are mutually exclusive.")
    return args


def parse_cmd(cmd: str):
    disallowed_fragments = ["&&", "||", "`", "$("]
    if any(fragment in cmd for fragment in disallowed_fragments):
        raise ValueError("Invalid cmd value: shell control operators are not allowed.")

    if "\n" in cmd or "\r" in cmd:
        raise ValueError("Invalid cmd value: newline characters are not allowed.")

    if re.search(r"(^|\s)[;&|<>]+($|\s)", cmd):
        raise ValueError("Invalid cmd value: shell metacharacters are not allowed.")

    try:
        tokens = shlex.split(cmd, posix=True)
    except ValueError as exc:
        raise ValueError(f"Invalid cmd value for shell-style parsing: {exc}") from exc

    if not tokens:
        raise ValueError("Invalid cmd value: must contain at least one sbt command token.")

    for token in tokens:
        if token == "":
            raise ValueError("Invalid cmd value: empty tokens are not allowed.")
        if "\n" in token or "\r" in token:
            raise ValueError(
                "Invalid cmd value: newline characters inside a token are not allowed."
            )
        if any(ch in token for ch in {";", "&", "|", "<", ">"}):
            raise ValueError("Invalid cmd value: shell metacharacters are not allowed.")

    return tokens


def load_cmd(cmd: str, cmd_file: str) -> str:
    if cmd_file:
        with open(cmd_file, encoding="utf-8") as src:
            return src.read().rstrip("\r\n")
    return cmd


def main() -> int:
    args = parse_args()

    # Always run self-checks as an early guardrail.
    self_test()

    try:
        cmd = load_cmd(args.cmd, args.cmd_file)
    except OSError as exc:
        return _fail(f"Unable to read --cmd-file '{args.cmd_file}': {exc}")

    if not cmd:
        return _fail("Either CMD or --cmd-file must be provided.")

    output_file = args.output_file

    try:
        tokens = parse_cmd(cmd)
    except ValueError as exc:
        return _fail(str(exc))

    if output_file:
        with open(output_file, "w", encoding="utf-8") as out:
            for token in tokens:
                print(token, file=out)
    else:
        for token in tokens:
            print(token)

    return 0


def self_test():
    test_parse_args_accepts_cmd_file_only()
    test_parse_args_rejects_cmd_and_cmd_file_together()
    test_parse_cmd_accepts_multi_token_command()
    test_parse_cmd_accepts_quoted_group()
    test_parse_cmd_rejects_control_operators()
    test_parse_cmd_rejects_shell_metacharacters()
    test_parse_cmd_rejects_parse_errors()
    test_parse_cmd_rejects_empty_command()
    test_parse_cmd_rejects_raw_newlines()
    test_load_cmd_reads_command_from_cmd_file()
    test_load_cmd_strips_crlf_from_cmd_file()
    test_parse_cmd_writes_tokens_to_output_file()
    print("All self-checks passed", file=sys.stderr)


def _assert_parse_error(cmd: str, expected_substring: str):
    try:
        parse_cmd(cmd)
        assert False, f"Expected ValueError for command: {cmd!r}"
    except ValueError as exc:
        assert expected_substring in str(exc), f"Expected {expected_substring!r} in {exc!r}"


def _assert_parse_args_error(argv, expected_substring: str):
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        try:
            parse_args(argv)
            assert False, f"Expected SystemExit for argv: {argv!r}"
        except SystemExit as exc:
            assert exc.code == 2, f"Expected exit code 2, got {exc.code!r}"
    message = stderr.getvalue()
    assert expected_substring in message, f"Expected {expected_substring!r} in {message!r}"


def test_parse_args_accepts_cmd_file_only():
    args = parse_args(["--cmd-file", "/tmp/example-cmd.txt"])
    assert args.cmd == "", f"Unexpected positional cmd: {args.cmd!r}"
    assert args.cmd_file == "/tmp/example-cmd.txt", f"Unexpected cmd_file: {args.cmd_file!r}"


def test_parse_args_rejects_cmd_and_cmd_file_together():
    _assert_parse_args_error(
        ["--cmd-file", "/tmp/example-cmd.txt", "testOnly com.example.FooTest"],
        "CMD and --cmd-file are mutually exclusive.",
    )


def test_parse_cmd_accepts_multi_token_command():
    tokens = parse_cmd('testOnly com.example.FooTest')
    assert tokens == ['testOnly', 'com.example.FooTest'], f"Unexpected tokens: {tokens}"


def test_parse_cmd_accepts_quoted_group():
    tokens = parse_cmd('testOnly "a b"')
    assert tokens == ['testOnly', 'a b'], f"Unexpected tokens: {tokens}"


def test_parse_cmd_rejects_control_operators():
    _assert_parse_error('testOnly Foo && rm -rf /', 'shell control operators are not allowed')


def test_parse_cmd_rejects_shell_metacharacters():
    _assert_parse_error('testOnly Foo ;', 'shell metacharacters are not allowed')


def test_parse_cmd_rejects_parse_errors():
    _assert_parse_error('testOnly "unterminated', 'shell-style parsing')


def test_parse_cmd_rejects_empty_command():
    _assert_parse_error('', 'must contain at least one sbt command token')


def test_parse_cmd_rejects_raw_newlines():
    _assert_parse_error('testOnly Foo\nrm -rf /', 'newline characters are not allowed')


def test_load_cmd_reads_command_from_cmd_file():
    with tempfile.TemporaryDirectory() as tmp:
        cmd_path = f"{tmp}/cmd.txt"
        with open(cmd_path, "w", encoding="utf-8") as out:
            out.write('testOnly "a b"\n')

        cmd = load_cmd("", cmd_path)

        assert cmd == 'testOnly "a b"', f"Unexpected command read from file: {cmd!r}"


def test_load_cmd_strips_crlf_from_cmd_file():
    with tempfile.TemporaryDirectory() as tmp:
        cmd_path = f"{tmp}/cmd-crlf.txt"
        with open(cmd_path, "wb") as out:
            out.write(b'testOnly "a b"\r\n')

        cmd = load_cmd("", cmd_path)

        assert cmd == 'testOnly "a b"', f"Unexpected CRLF command read from file: {cmd!r}"


def test_parse_cmd_writes_tokens_to_output_file():
    with tempfile.TemporaryDirectory() as tmp:
        out_path = f"{tmp}/tokens.txt"
        tokens = parse_cmd('testOnly "a b"')
        with open(out_path, "w", encoding="utf-8") as out:
            for token in tokens:
                print(token, file=out)
        with open(out_path, encoding="utf-8") as src:
            content = src.read().splitlines()
        assert content == ["testOnly", "a b"], f"Unexpected output file content: {content}"


if __name__ == "__main__":
    raise SystemExit(main())
