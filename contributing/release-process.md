Release Process
===============

**Table of contents**

- [Different types of releases](#different-types-of-releases)
- [How-to do a release](#how-to-do-a-release)
    - [Stable release](#stable-release)
    - [Snapshot release](#snapshot-release)
- [Creating a release line](#creating-a-release-line)
- [Performing manual steps locally](#performing-manual-steps-locally)
- [How-to recover from a failed release](#how-to-recover-from-a-failed-release)

This guide explains the Canton release process.
If you want to understand how it fits into the bigger Daml release process, please refer
to [this document](https://github.com/digital-asset/daml/blob/main/sdk/release/RELEASE.md).

# Different types of releases

| Kind     | Trigger           | Run on | Branch         | Publish to             | Other artifacts                         |
|----------|-------------------|--------|----------------|------------------------|-----------------------------------------|
| Snapshot | Automatic, manual | CI     | Any            | S3, JFrog*, OCI*       | No                                      |
| RC       | Manual            | Local  | release-line-* | S3, GH, JFrog, OCI     | No                                      |
| Stable   | Manual            | CI     | release-line-* | S3, GH, JFrog, OCI     | Data continuity dumps and release notes |

*Yes: for nightly releases, only on Tuesday (weekly release).

Notes:
- S3: S3 is public (which means we publish only community only)
- GH: We publish on the private and public GH

We have several kind of releases that have different processes:

- **Stable release**

  Such releases are done hand-in-hand with a Daml release and are coordinated across several teams.
  Schedule, process and roles of such releases can be found in
  the [Daml Release Planning](https://docs.google.com/document/d/1FaBFuYweYt0hx6fVg9rhufCtDNPiATXu5zwN2NWp2s4).


- **Snapshot**

  Unlike stable releases, snapshots are released independent of a Daml release.
  We distinguish between two types of snapshots:

    - Named snapshots (e.g., release candidates)
    - Daily or manual snapshots

      Daily snapshots are published nightly from the latest commit of main.
      Alternatively, they can be triggered manually from any commit.
      They can be found on [Artifactory](https://digitalasset.jfrog.io/ui/repos/tree/General/assembly/canton).

# How-to do a release

We first focus on the stable releases, which require more steps than snapshots.
For the sake of the example, we suppose that we want to release Canton 3.4.4

## Stable release

Before diving into the step-by-step instructions, here is what the release process does at a high level:

- Announcing a code freeze for the corresponding release line in `#team-canton`\
  No PR should be merged during the release.

- Release PR ***"release: canton 3.4.4"***, targeting the `release-line-3.4` branch\
  Note: all PRs targeting release lines fail CI once because of the TODO check, this is expected

  This PR contains at least the following commits:
    - Release notes: release notes are prepared and moved to `release-notes/`
    - Checksums: checksums for the DB migrations are generated (can be empty if there are no migrations)
    - Release commit: changing Canton version
    - Snapshot commit: update Canton version to the next snapshot version (3.4.5-SNAPSHOT), release notes are reset.

  These commits are added to the PR by the CI machinery of the release PR itself. This might take a while.

- Synchronization with `main` PR ***"Release wrap-up: adding 3.4.4 ... "***, targeting `main`

  This PR contains several commits:
    - Release notes
    - Flyway checksums for the new migration (if any change to the DB schema was done)

- Merging the release PR will trigger building and publishing the artifacts.

- Code freeze can be lifted by sending another slack message to #team-canton

- Once everything is done, merge the PR targeting `main`.

### Pre-release steps

Before starting the release, check the following:

- Release notes are written and up-to-date in [UNRELEASED](../UNRELEASED.md) (in the corresponding `release-line`
  branch).
  If unsure, check with Sören or Marcin. Note that the release notes might be different from the product-curated
  release notes in the [Daml release planning](https://docs.google.com/document/d/1FaBFuYweYt0hx6fVg9rhufCtDNPiATXu5zwN2NWp2s4).

- No blocker is mentioned in release tracker.

#### Update the release line Daml version

- The Daml version can be read in `project/project/DamlVersions.scala`.

- The Daml version must be a non-snapshot (== a stable) version.

- The Canton and Daml versions may be different.

- As Daml releases become available notifications are published in `#team-internal-releases`.

- To update the Daml version follow these [instructions](managing-dependencies.md#updating-the-daml-version).

### Preparing the release PR

First, announce the code freeze in #team-canton.

To kick off the task that creates the two PRs, open the CircleCI page corresponding to the required release line.
For 3.4, this
is [https://app.circleci.com/pipelines/github/DACH-NY/canton?branch=release-line-3.4](https://app.circleci.com/pipelines/github/DACH-NY/canton?branch=release-line-3.4)

Trigger pipeline with `manual_job` set to `start_release_process` which kicks off `manual_start_release_process`
workflow. Once the workflow succeeds, a PR will be opened with several commits.

The PR opens the branch `release-3.4.4`, targeting to merge on `release-line-3.4`. Any further change to the release
needs to happen
on `release-3.4.4`. The release commit has the title `release: 3.4.4` and any information relevant to the release (like
the release notes) need to be included in the history _before or at_ such commit.

If there's any need to do in-flight changes to the release PR you can touch up the history of the PR commits with an
interactive rebase;
remember that any change needs to be inserted _before or at_ the release commit (commit title: `release: 3.4.4`). This
is useful to make last minute changes to the release notes.

### Reviewing and merging the PR

Review:

- The PR should contain at least the commits mentioned in [Stable release](#stable-release)
- Check that release notes are complete (compare
  with [Daml Release Planning](https://docs.google.com/document/d/1FaBFuYweYt0hx6fVg9rhufCtDNPiATXu5zwN2NWp2s4) and the
  merged commits).
- Check a last time for any typo in the release notes.
- Check that the substitutions in the *Compatibility* table are correct.

You need a release owner to be able to merge the release PR. Ask a member of
the [Canton Release Owners](https://github.com/orgs/DACH-NY/teams/canton-release-owners) directly for a review.

**Important**
Merge strategy of the PR should be ***rebase and merge***, otherwise the release will not be done.

Once the PR is merged, the release commit (`release: 3.4.4`) will be tagged (`3.4.4`) and the last part of the release
process will kick off.
You can follow the process on CircleCI:

- Open the list of commits for the corresponding release-line:

  [https://github.com/DACH-NY/canton/commits/release-line-3.4](https://github.com/DACH-NY/canton/commits/release-line-3.4)

- Open the CircleCI workflow of the release-commit (`release: 3.4.4`)

- Code freeze can be lifted by sending another slack message to `#team-canton`

- Once the artifacts have been published (i.e., the `publish_release` job of the `canton_build` workflow is green),
post an update in the Slack channel dedicated to the release.

- If there are any failed job, you **must** understand the reason and take appropriate actions.

  You can reach out to Raf or Sören for guidance.

## Snapshot release

### Creating a manual snapshot (Canton only)

On any commit, you can trigger a manual snapshot that will be pushed to artifactory and S3.
To do so, start the workflow `manual_snapshot_release` on your commit.

### Creating a release candidate

Notes:

- The release-line should be created before doing the first rc for a minor release.
- Ensure you can use the `hub` tool, that is you've set up your [GitHub token](https://github.com/settings/tokens).

For the sake of the example, suppose that we want to create `3.4.0-rc2`.

Creating a release candidate is simpler than performing a stable release because we don't ship release notes.
As such, the release PR consists only of two simple commits:

- The release commit (changes the Canton version from `3.4.0-SNAPSHOT` to `3.4.0-rc2`).
- The revert commit (changes the Canton version from `3.4.0-rc2` to `3.4.0-SNAPSHOT`).

Additionally, for the first RC, a third commit will be inserted between the two above to update Flyway hashes for the
version being released

Locally, check out the release line branch (`release-line-3.4`) and ensure it is up to date.
Then, invoke the script:

```
./release/propose.sh snapshot rc2
```

and call the script again.

Once the script is finished, you can push the resulting branch (`release-3.4.0-rc2`) and open a PR targetting the
corresponding release line (`release-line-3.4`).

Similarly to a stable release PR, you need:

- A release owner approval.
- To merge the PR with *rebase and merge*.

# Creating a release line

As an example, we suppose that we want to create release line `release-line-3.5`.

To create a release line, use the `propose.sh` script:

```bash
./release/propose.sh release-line 3.5
```

## Updating main

Following the creation of a release line, `main` needs to be updated
so that Canton version becomes `3.6.0-SNAPSHOT`.

The invocation of the `propose.sh` script should do most of the steps and push a branch with the changes.

Note the following prerequisites to be able to have green CI on the PR:

- First Daml sdk snapshot with a 3.6 version needs to be available.

# Performing manual steps locally

Behind the scenes, the workflow that results in the release PR calls several scripts of the Canton repo that can also be
invoked manually locally.
This can be useful if you need to investigate a failure or do a small fix.

- Creating the release commit

  Invoke `release/propose.sh create_release_pr`

- Creating the data continuity dumps

  This part is a bit more difficult and can be done after the process if needed.
  Reach out to one of the [release owners](https://github.com/orgs/DACH-NY/teams/canton-release-owners).

- Changing Canton version to the next snapshot version

  Invoke `release/propose.sh finish_release_process`

# How-to recover from a failed release

Most of the steps around releases can be retried if they fail.
In that case, always prefer "Rerun workflow from failed" over "Rerun workflow from start" (otherwise, pushing some of
the commits to the release branch will fail).

If you need to restart from scratch, check the following before starting the process again:

- Tags corresponding to the release (`v3.4.4`) do not exist on both the private and the public repo.
  A tag can be deleted using
  ```
  git push --delete origin nameOfTheTag
  ```

- Release branch (`release-3.4.4`) does not exist on GitHub.

# Making a patch release for 2.3

The tooling for the release on the 2.3 line is less polished than for recent release lines.
As such, more manual steps have to be performed.
Moreover, Canton does not use Nix on this line, which means that you may need to install some tools yourself (Java,
sbt).

In case of issue, please reach out to Rafael, Oliver or Sören.

For the sake of the example, suppose that we want to release `2.3.13`.

## Bumping Daml

First, create and merge a PR to bump Daml.

- Checkout and pull a fresh `release-line-2.3` branch
- Bump the version in `project/project/DamlVersions.scala` to `2.3.13`.
- Updates in `community/common/src/main/scala/com/digitalasset/canton/version/CantonVersion.scala`
    - Add two new versions in `ReleaseVersion` (stable release and next snapshot).
    - Add two mappings in `CantonVersion`
- Run `sbt updateDamlProjectVersions`
- Push into a branch and run manual job `manual_update_canton_build_docker_image`
- TODO checker will fail and the automated build should be cancelled to first have the build Docker image ready
- Merge the PR in the `release-line-2.3`

## Release

- Kick-off the process by running the manual job `manual_start_release_process` on branch `release-line-2.3`.
- The first step will run and create the release commit and the second step will automatically add the data continuity
  dumps.
- Locally, checkout the branch (`release-2.3.13`)
- Check and if necessary edit the release notes (i.e. use interactive rebasing of the `release-2.3.13` branch on
  `release-line-2.3` and mark the commit with release notes for editing - need to preserve the commit history for the
  release process to work).
- Run `release/propose.sh finish_release_process` (this prepares the next snapshot versions in a new commit).
- Push to Github.
- Review and merge (as usual, with rebase strategy).

## Clean up

A PR targeting main should be opened to add the release notes (`release-notes/2.3.13.md`) and the data continuity dumps.
