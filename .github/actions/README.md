# Composite actions

Shared composite actions used by the Canton GitHub Actions workflows.

## Conventions

- **Validate step-to-step handoffs at the point of use.** Each composite step has its own `env:` block, so a value produced by one step (`steps.<id>.outputs.<name>`) has to be listed again in every downstream step that consumes it. That is easy to forget, and a missing propagation leaves the variable unset, which many shell constructs swallow silently (for example a command substitution buried in an `echo` argument, which keeps the step green even when it fails). When a step depends on a file path or other required value from an earlier step, assert it up front and exit non-zero with a clear message if it is absent, the way `sbt/execute_sbt_command` guards its resolved command file.
