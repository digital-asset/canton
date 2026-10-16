# `/investigate-flaky-test`

A Claude Code skill for flaky test duty. Given the investigation package of a failing suite, it diagnoses the failure, drafts a triage note for the flaky issue, and optionally proposes a minimal fix, verified locally with `testManyTimes`.

## Setup

None. Claude Code loads the skill from this directory when it runs in a canton checkout (after you accept the folder in its trust prompt). It needs `git`, `unzip` and `zgrep`, and sbt for the optional verification.

## Usage

1. On the flaky issue, open the evidence comment and download the `flake-pkg-*` artifact.
2. Copy the artifact to the sandbox, eg `sbx cp ~/Downloads/flake-pkg-xxxx.zip <sandbox-name>:/home/agent/`
3. In the repo root, run `/investigate-flaky-test <zip or directory>` in Claude Code. If the package holds several suites, it asks which one.
4. Review the triage note it writes next to the package (`<suite dir>/triage.md`), then paste it on the issue.
5. If it offers a fix, it only edits the working tree. Review the change and the PR description draft, then commit and open the PR yourself.

If the failure is not a flake but a real failure, it says so and names the commit that likely broke the test.

## Boundaries

- It works from the package and the repository only: no access to GitHub issues, the GitHub API or the web. It may `git fetch` the failing commit.
- It never commits, pushes or opens PRs. You are the author of the PR, so it needs a single approval (`AI_POLICY.md`).
- It never adds `@UnstableTest`, never uses `utils.synchronize_topology()`, and suppresses logs or raises timeouts only with a stated reason.

## Files

| File | Content |
|---|---|
| `SKILL.md` | the workflow the agent follows |
| `fix-patterns.md` | symptom, cause and fix catalog, condensed from the Canton Flaky Tests Guide and past flake fixes. Extend it when you find a recurring pattern |
| `triage-template.md` | the issue comment and PR description templates |

The package format and the pipeline that produces it are described in `scripts/ci/README.md` ("Investigation package").
