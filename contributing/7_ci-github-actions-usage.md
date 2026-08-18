CI Usage: GitHub Actions
========================

Canton is using Github Actions for the CI pipelines.
This page gives a practical entry point for contributors, including users who are new to GitHub Actions.

GitHub Actions workflow files in this repository live in:
`/.github/workflows`

# GitHub Actions summary

Quick glossary:
- **Pipeline**: an informal name for a full CI flow. In this document, it usually means a GitHub Actions workflow.
- **Workflow**: one CI pipeline file in `/.github/workflows`. There are two kinds:
  - **Top-level workflow**: triggered directly by events (for example `push`, `pull_request`, `workflow_dispatch`). This is what shows up in the GitHub `Actions` tab and PR checks. Example: `canton_build_required.yml`.
  - **Reusable workflow**: a workflow designed to be called by other workflows via `uses:`. It is not triggered by events directly. Files prefixed with `_` follow this convention in this repo (for example `_compile.yml`, `_test.yml`). When it fails, the failure surfaces in the calling top-level workflow. This is the closest equivalent to a reusable job or command in CircleCI.
- **Job**: one block inside a workflow (for example compile, test, docs).
- **Run**: one execution of a workflow.
- **Check**: the status shown on a PR.

Common GitHub Actions job results:
- `success`
- `failure`
- `cancelled`
- `skipped`

---

## How to trigger manually

1. Open repository `Actions`.
2. Select `Canton Build Required`.
3. Click `Run workflow`.
4. Select branch and start the run.
5. Open failed job logs/artifacts if debugging is needed.

Example view of the manual trigger for `Canton Build Required`:

![GitHub Actions manual run for Canton Build Required](7_gha_manual_run_canton_build_required.png)

---

# How to inspect auto-rerun attempts

When a workflow is rerun, GitHub keeps attempts under the same run.

1. Open the workflow run page in `Actions`.
2. In the run header, find the attempt selector (for example `Attempt #1`, `Latest attempt #2`).
3. Switch between attempts and compare failed jobs.
4. For each attempt, open the same job name and check:
   - error message
   - failing step
   - timestamps
5. Use the latest attempt for final status, but keep earlier attempts for diagnosis.

![GitHub Actions attempt selector for rerun workflow](7_gha_attempt_selector.png)

Tip: if attempt 1 failed and attempt 2 passed, treat this as a possible flake.

---

# Related docs

- Legacy CircleCI usage and background: [5_ci-usage.md](5_ci-usage.md)
- Git and PR process: [6_github.md](6_github.md)


