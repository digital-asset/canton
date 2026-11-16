# EVM test vectors

`evm_vectors.jsonl` holds hand-picked edge cases and regular cases for the Daml-LF
Uint256 operations. Three tests read it:

- `Uint256Spec` (Scala, in `daml-lf-data-tests`, runs in CI) checks the `Uint256` model
  against the vectors.
- `EVMBuiltinSpec` (Scala, in `daml-lf-interpreter`, runs in CI) checks the Speedy builtins
  against the vectors.
- `EvmVectors.t.sol` (Foundry, run by hand) checks the vectors against a real EVM.

Together they show that the model and the builtins behave like the EVM opcodes.

## When to run the forge checker

Whenever `evm_vectors.jsonl` changes. Reviewers of such a change should run it too.

## How to run it

1. Install Foundry (tested with forge 1.9.0, solc 0.8.26):

   ```bash
   curl -L https://foundry.paradigm.xyz | bash
   foundryup
   ```

2. Make sure `forge` is on your `PATH`. `foundryup` installs it into `~/.foundry/bin`:

   ```bash
   export PATH="$HOME/.foundry/bin:$PATH"
   forge --version
   ```

3. Run the checker from this folder:

   ```bash
   cd community/daml-lf/data-tests/src/test/resources/evm
   forge test -vv
   ```

   The single test `test_vectors` passes if every line matches the EVM. Otherwise it lists
   each mismatching line with the expected and actual value.

   To check another file (it must be readable under `fs_permissions` in `foundry.toml`):

   ```bash
   EVM_VECTORS=other.jsonl forge test -vv
   ```

## Vector format

One JSON object per line, no blank lines:

```json
{"opcode": "SDIV_UINT256", "a": "0x8000000000000000000000000000000000000000000000000000000000000000", "b": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "expected": "0x8000000000000000000000000000000000000000000000000000000000000000", "note": "MIN / -1 overflows to MIN"}
```

- Values are `0x`-prefixed hex strings.
- `b` and `c` are only present for opcodes with 2 or 3 arguments.
- Arguments are in **EVM order**. For `SHL`/`SHR`/`SAR`, `a` is the shift and `b` the value.
  The Daml-LF builtins take (value, shift), and `EVMBuiltinSpec` swaps the arguments.
- `SLT`/`SGT` expect `0x0` or `0x1`.
- `note` is optional and ignored by both checkers. Use it to say why a case exists.

## What the forge checker does not cover

Argument order and result encoding on the Daml-LF side (the shift swap, `SBool` for
`SLT`/`SGT`) live in `EVMBuiltinSpec`. Review that mapping when it changes.
