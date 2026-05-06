# TODO checker

Validates that every `TODO`/`FIXME`/`XXX` comment in the codebase references an open GitHub issue.
Run automatically in CI on every PR. See `src/checkTodos.sc` for implementation details.

## Running locally

Requires `amm` (Ammonite) and `gh` (GitHub CLI) authenticated with a token that can read
`DACH-NY/canton`, `digital-asset/canton`, and `digital-asset/daml`.

```bash
# From the repository root
amm .circleci/todo-script/src/checkTodos.sc
```

The script exits 0 if all TODOs are valid, 1 otherwise. Output is written to `todo-out/`.

## Testing a new TODO format

To verify a new issue reference type is accepted/rejected correctly:

1. Add a temporary TODO to any `.scala` file, e.g.:
   ```scala
   // TODO(https://github.com/digital-asset/daml/issues/22983): open issue — should pass
   ```
2. Run the script. It should complete without error.
3. Replace with a closed issue number, e.g. `22935`, and run again. The script should exit 1
   with `TODOs for closed issues: Daml Issue 22935`.
4. Remove the temporary TODO before committing.

To find open/closed issue numbers for a given repo:
```bash
# Open
gh issue list --repo digital-asset/daml --limit 5 --json number --jq '.[].number'
# Closed
gh issue list --repo digital-asset/daml --state closed --limit 5 --json number --jq '.[].number'
```

## Supported TODO formats

| Format | Validated against |
|---|---|
| `TODO(#<n>)` or `TODO(i<n>)` | Open issues in `DACH-NY/canton` |
| `TODO(https://github.com/digital-asset/canton/issues/<n>)` | Open issues in `digital-asset/canton` |
| `TODO(https://github.com/digital-asset/daml/issues/<n>)` | Open issues in `digital-asset/daml` |

See `contributing/3_scala-coding-guidelines.md` for the recommended format for new TODOs.
