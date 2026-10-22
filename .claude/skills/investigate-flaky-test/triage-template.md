# Triage templates

Keep both short: no restating the package, no padding (`AI_POLICY.md`). Replace every `<...>`; drop optional sections that would be empty.

## Issue comment (`triage.md`)

The first line is a machine-readable marker, so a later stage can find earlier triage on the issue. Keep it exactly in this shape.

```markdown
<!-- flaky-triage v1 suite=<Suite> sha=<ci.sha> run=<ci.run_id> -->
### Triage: `<Suite>` at `<short sha>` (<ci.job>, <ci.date>)

**Summary:** <one line: what fails and why>

**Category:** <A timing | B log assertions | C test race | D environment | E production | F not a flake: real failure>, confidence <high | med | low>: <one-line reason>

**Root cause:** <causal chain in 2-4 sentences, with code references `path:line` at `<short sha>`>

<at most 5 quoted log lines, each with its file:line>
`suite.log.gz:<n>` `<timestamp> ... <message>`

Falsified by: <what would show this is wrong>

**Fix:** <none yet | proposed: one-line description | applied locally: files changed>

**Verification:** <none yet | `sbt "testManyTimes <N> <FQCN>"`: <completed>/<N> runs passed, <reproduced before the fix: yes/no>>

**Open questions:** <for the human, e.g. runner load in Datadog for <ci.runner>; omit if none>

**Guide update:** <optional: symptom / cause / fix entry for the Flaky Tests Guide>
```

## PR description draft

Title in the repo's style: `Fix flaky <Suite>: <cause in a few words>`.

```markdown
Refs #<issue number>

## Analysis
<root cause from the triage note, with the key log lines and code references>

## Fix
<what changed and why it addresses the cause, not the symptom; for a suppression, timeout or ignore entry, the reason it is legitimate>

## Verification
<command, runs completed and failed, whether the flake reproduced before the fix; CI many-times pipeline if run>
```
