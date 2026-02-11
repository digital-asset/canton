# The REPL

This project defines a REPL (Read-Eval-Print Loop) commandline tool. It has the following uses:

- `repl [file]`: Run the interactive repl. Load the given packages if any. The in the interactive repl, you can evaluate
  daml expressions.
- `validate [file]`: Load the given packages and validate them. Validation asserts that the package can
  be decoded and typechecked.
- `[file]`: Same as 'repl' when all given files exist.

## How to run the repl

### Option 1: in SBT

```
> sbt
sbt:canton> daml-lf-repl/run <ARGUMENTS>
--e.g.
sbt:canton> daml-lf-repl/run validate community/daml-lf/snapshot/target/scala/scala-2.13/resource_managed/test/ReplayBenchmark.dar
```

### Option 2: outside SBT

```
> sbt "daml-lf-repl/run <ARGUMENTS>"
--e.g.
> sbt "daml-lf-repl/run validate community/daml-lf/snapshot/target/scala/scala-2.13/resource_managed/test/ReplayBenchmark.dar"
```

### Option 3: double dash syntax (should work but seems not to)

```
> sbt daml-lf-repl/run -- <ARGUMENTS>"
--e.g.
> sbt daml-lf-repl/run -- validate community/daml-lf/snapshot/target/scala/scala-2.13/resource_managed/test/ReplayBenchmark.dar
```
