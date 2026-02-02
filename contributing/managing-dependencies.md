Managing Dependencies
=====================

Canton depends on various internal libraries (e.g. Daml-LF) and external libraries (gRPC, cats, slick, ...).
This guide explains how to manage (e.g. add, remove, update) these dependencies.

# External Dependencies

The versions of external libraries are specified in [Dependencies](../project/Dependencies.scala).
The dependencies used by individual sbt subprojects are specified in [build.sbt](../build.sbt) and
files referenced therein, such as [BuildCommon.scala](../project/BuildCommon.scala).

To update a library, change the version number in [Dependencies](../project/Dependencies.scala).

For adding or removing a library, you need to change both [Dependencies](../project/Dependencies.scala)
and [build.sbt](../build.sbt) (and referenced files) accordingly.

## Debugging Dependency Conflicts

When changing dependencies, it can happen that Canton depends on several versions of the same library.
This can surface in various ways, ranging from compile errors or warnings up to VirtualMachineErrors at runtime.

Use [sbt-dependency graph](https://www.scala-sbt.org/sbt-dependency-graph/) to investigate dependency conflicts.
In order to use it, you need the full Java JDK and not the headless version.
That can be achieved by changing, in the file `shell.nix`, the value of `jre` from `pkgs.openjdk17_headless` to `pkgs.openjdk17`.

## Blackduck Scans

We use Blackduck to scan libraries used by Canton for security vulnerabilities and licensing issues.
By default, Blackduck runs only daily but not on every pull request.
Whenever you introduce a new third party library (or try to resolve a reported security or licensing issue),
you should run Blackduck and verify the results before merging the changes to a protected branch.

To do this, make sure the name of the branch with the changes begins with `blackduck....`.
Then, the Blackduck scan will run on your pull request under the branch name you have created.

You can see the results of your scan through the [Blackduck UI](https://digitalasset.blackducksoftware.com/).
If you have never logged into the UI before,
first hit that URL using Google SSO to establish your user identity,
then contact help@digitalasset.com to be provisioned to the relevant project(s) you work on within Blackduck.

A full FAQ on setting up and using Blackduck at Digital Asset can be found here: https://github.com/DACH-NY/security-blackduck

# Internal Dependencies

## Updating the Daml version

We have tools to update the underlying Daml version consistently.

1. Update `version` in [`project/project/DamlVersions.scala`](../project/project/DamlVersions.scala) to the new Daml version.
2. Run `sbt updateDamlProjectVersions` to update the `daml.yaml` project files.
3. Create a PR with the resulting changes and if all required checks on CI succeed merge to the underlying protected branch (e.g. `main`).

The `sbt` build should automatically pull down the new damlc, daml-script dars, and scala codegen of the corresponding Daml version,
so no action should be required locally.


## Testing Canton Using Custom Daml Artifacts

Canton relies on the Daml engine and other components from the [Daml repo](https://github.com/digital-asset/daml).
At times, you may want to try out custom changes only available in a development environment or changes that have not been released yet.
This works only locally and not in CI.

1. Publish Daml artifacts to the local maven repository (at `~/.m2/repository`) from a daml repo clone:
   ```
   cd daml
   daml-sdk-head --skip-jar-docs --sha
   ```

2. Last line of the previous command should look like:
   ```
   Done installing JARs as 0.0.0-head.20220427.9808.f0303b69.
   ```

3. Then set the `DAML_HEAD_VERSION` environment variable in the environment from which you start sbt:
   ```
   export DAML_HEAD_VERSION="0.0.0-head.20220427.9808.f0303b69"
   ```

4. To be extract careful, consider cleaning and removing all target directories to be sure you don't end up with a
   "mixed" set of artifacts particularly before the initial compile.
   ```
   git clean -Xfd
   ```
5. Update the daml.yaml sdk-versions to the head version to ensure damlc does not get upset.
   ```
   sbt updateDamlProjectVersions
   ```

## Updating static Daml test artifacts

Some tests require DARs that can only be generated with a 2.x Damlc compiler.
Because the Canton repo would have to download several hundred MB of Damlc
compiler to regenerate them, and because they're unlikely to ever change, we
instead generated them in Bazel and then checked them in to S3 at
`s3://canton-public-releases/test-artifacts/`. SBT then downloads these
artifacts.

To upload a new version of an artifact, stage its new copy at the appropriate
location identified in the `test_dars` array in `scripts/ci/update-s3-test-artifact-dars.sh`,
then run the manual job `manual_upload_new_test_artifact_dars`. To update tests
to use the new artifact, update the appropriate call to `testingDarsFromS3` with
the new file's hash.
