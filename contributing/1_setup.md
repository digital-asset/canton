Environment Setup
=================

This guide explains how to set up your environment to contribute to Canton.

# Git

Install git, if necessary. To verify, run:
```shell
git --version
```

Set up git with your full name:
```shell
git config --global user.name <yourFullName>
```

Configure "reuse recorded resolution" for better conflict resolution:
```shell
git config --global rerere.enabled true
```

Clone the repository:
```shell
git clone git@github.com:DACH-NY/canton.git
```

# GitHub User Name

Make sure that your full name is set up in your [GitHub profile](https://github.com/settings/profile).
So that merges to the `main` branch get properly tagged with your name.

# Install nix

This repo assumes the use of `direnv` for local development, along with a working `Nix` installation.

1. install [`nix`](https://nixos.org/download.html) (in case of failed installation [uninstall nix](https://nix.dev/manual/nix/2.26/installation/uninstall) before trying again.)
2. install [`direnv`](https://direnv.net/#basic-installation)
3. run `direnv allow` in the project directory (re-run this in case the `.envrc` is changed)

The first time you `cd` into the project folder, Nix will download required libraries which may take a while.
Every time you switch to the repository, you will see output similar to the following:

```
$ cd canton
direnv: loading ~/source/da/shell-nix/.envrc
direnv: using nix
direnv: loading .envrc.private
direnv: export +AR +ARTIFACTORY_PASSWORD +ARTIFACTORY_USER +AS +CC +CONFIG_SHELL +CXX +DETERMINISTIC_BUILD +DOCKER_HOST +HOST_PATH +IN_NIX_SHELL +LD +NIX_BINTOOLS
        :
        :
```

# Install docker

[Install docker](https://docs.docker.com/get-started/get-docker/), if necessary. To verify, run:
```shell
docker --version
```

# Setup `lnav`

We use `lnav` to analyze Canton log files.
The format file `canton.lnav.json` is required to navigate Canton log files.
To install, run
```shell
lnav -i canton.lnav.json
lnav -i canton-json.lnav.json
```

# Setup circleci-cli

We use CircleCI as our CI, and we use the `circleci` CLI tool to build its configuration locally.
Our configuration depends on private "orbs", which are reusable configuration elements in CircleCI, therefore you need to authorize the CLI.

Login to circleci.com with your GitHub user.
Then generate a circleci access token for your user at https://app.circleci.com/settings/user/tokens.
Finally run `circleci setup` from a checked out canton repo, it will ask you for the token you just generated.

To verify, run `./.circleci/build-config.sh`. It should pass without errors.

# IntelliJ IDEA

We use IntelliJ IDEA as IDE for Scala.

## Installation

Install [IntelliJ IDEA](https://www.jetbrains.com/idea/download).

Normally, the Community edition is sufficient.
If you need to use the Ultimate edition, contact help@digitalasset.com to obtain a license.

In general, go for the latest version, but avoid the first minor version.
For example, prefer 2025.1.3 over 2025.2.

## Startup

To inherit a `direnv` environment for IntelliJ do the following:
* Change directory into the repository root. Make sure that `direnv` loads the environment.
* Start IntelliJ from that directory.
  For Mac, call `open -a /Applications/IntelliJ\ IDEA.app/`.

## Open the Canton source code in IntelliJ IDEA

* One of the following:
  * Choose File | Open...
  * Click on the Open button.
* Choose the repository root directory.

## IntelliJ IDEA Configuration

IDEA is configured by files in version control (.idea folder).
So it will mostly work out of the box.

Additionally:
* Go to Help | Edit Custom Properties... and add a line `idea.max.intellisense.filesize=6000` to support big generated files.
  This is needed to avoid red references due to target of the reference is residing in such generated files.
* Go to IntelliJ IDEA | Settings | Plugins, select marketplace and search for the Scala plugin. Install it.
  * You need to click on Apply and to click on "Enable" from the popup window to finalize the plugin setup.

## Select a JDK

* From the repository root, run `java -version` to determine the current java version (configured through nix).
* Go to File | Project Structure...
  * Go to Project. Select the JDK you have determined in the previous step, it should be visible from the SDK dropdown menu with the exact same version.
    * If there is no SDK visible, double check that you have the Scala plugin enable, or restart IntelliJ.
  * Go to SDKs. Add the JDK you have determined in the previous step if it has not been done automatically.
* More info about runtime versions [here](https://github.com/DACH-NY/canton/blob/main/contributing/runtime-versions.md)
## SBT Configuration

Firstly, configure the version of `sbt` that is used by IntelliJ:
* Go to "IntelliJ IDEA" | Settings, search for "sbt".
  * Set the maximum heap size to at least 8000M.
  * Check both "project reload" and "builds" in sbt shell section.

Next, also configure the version of `sbt` that you can invoke from the command line:
* Make sure to configure the JVM heap size to at least 8G.
  If you use bash/zsh, that means adding `export SBT_OPTS="-Xmx8G -Xms2G -XX:+UseG1GC"` to `.bash_profile`/`.zshrc`.

# Test Your Dev Environment

In a terminal, run `sbt format`. This should run without errors.

Once you're done with running `sbt` in a terminal, run `git clean -Xfd`, as the files generated with the terminal sbt
may interfere with IntelliJ's bundled `sbt`.

Open IntelliJ, check if there is an exclamation mark in the bottom right corner.
Such an exclamation mark indicates internal errors, e.g., due to incompatible plug-ins or a buggy version of IntelliJ IDEA.

Next, go to View | Tool Windows | sbt and click on "Sync All sbt Projects" icon (top left).
On failure, double-check your [IntelliJ IDEA settings](#intellij-idea-configuration).

Next, go to View | Tool Windows | sbt shell.
Run, the commands `compile`, `format`, `unidoc`, `bundle`.

Finally, run the following integration tests from within IntelliJ (cntl+n(Linux) / cmd+o(Mac) and insert test name in search box):

- `SimplestPingIntegrationTestInMemory`
- `SimplestPingBftOrderingIntegrationTestH2`
- `SimplestPingBftOrderingIntegrationTestPostgres`
