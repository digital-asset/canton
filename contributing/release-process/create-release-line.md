Create a release line
=====================

As an example, we suppose that we want to create release line `release-line-3.5`.

To create a release line, use the `propose.sh` script:

```bash
./release/propose.sh release-line 3.5
```

# Updating main

Following the creation of a release line, `main` needs to be updated
so that Canton version becomes `3.6.0-SNAPSHOT`.

The invocation of the `propose.sh` script should do most of the steps and push a branch with the changes.

Note the following prerequisites to be able to have green CI on the PR:

- First Daml sdk snapshot with a 3.6 version needs to be available.

# Updating Dependabot

The supported release lines are listed in `release/supported-release-lines.txt`
(one `major.minor` per line), the source of truth for Dependabot coverage.

Creating a release line with `propose.sh` adds the new line to this file and runs
`release/update_dependabot.py` automatically, so `.github/dependabot.yml` is
updated as part of the "update main" PR (review the diff before merging).

To ramp a line **down** (no longer supported), remove it from
`release/supported-release-lines.txt` and run:

```bash
python3 release/update_dependabot.py
```

This rewrites `.github/dependabot.yml` so each supported line has a `docker`
entry (release lines are covered for docker only for now) and drops entries for
lines no longer listed.

# Modify release/propose-open-source-code-drop.sh

This step will only be required until we transition to the open source repo.

Modify `release/propose-open-source-code-drop.sh` such that the `BASE_PUBLIC` and `BRANCH_PUBLIC` point
to branches the include the release-line. For example see https://github.com/DACH-NY/canton/pull/37023


