#!/usr/bin/env python3
"""Parse and validate an SBT command string into argv tokens.

Prints one token per line to stdout or writes to a file.
Returns exit code 2 on validation or parsing errors.
"""

import argparse
import re
import shlex
import sys
import tempfile


def _fail(message: str) -> int:
    print(message, file=sys.stderr)
    return 2


def parse_args():
    parser = argparse.ArgumentParser(
        description="Parse and validate an SBT command string into argv tokens.",
        usage="%(prog)s [--output-file FILE] CMD",
    )
    parser.add_argument(
        "cmd",
        help="The SBT command to parse (required).",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Optional output file to write tokens to. If not provided, writes to stdout.",
    )
    return parser.parse_args()


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
            raise ValueError("Invalid cmd value: newline characters inside a token are not allowed.")
        if any(ch in token for ch in {";", "&", "|", "<", ">"}):
            raise ValueError("Invalid cmd value: shell metacharacters are not allowed.")

    return tokens


def main() -> int:
    args = parse_args()

    # Always run self-checks as an early guardrail.
    self_test()

    cmd = args.cmd
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
    test_parse_cmd_accepts_multi_token_command()
    test_parse_cmd_accepts_quoted_group()
    test_parse_cmd_rejects_control_operators()
    test_parse_cmd_rejects_shell_metacharacters()
    test_parse_cmd_rejects_parse_errors()
    test_parse_cmd_rejects_empty_command()
    test_parse_cmd_rejects_raw_newlines()
    test_parse_cmd_writes_tokens_to_output_file()
    print("All self-checks passed", file=sys.stderr)


def _assert_parse_error(cmd: str, expected_substring: str):
    try:
        parse_cmd(cmd)
        assert False, f"Expected ValueError for command: {cmd!r}"
    except ValueError as exc:
        assert expected_substring in str(exc), f"Expected {expected_substring!r} in {exc!r}"


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
