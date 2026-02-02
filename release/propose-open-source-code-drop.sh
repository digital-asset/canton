#!/usr/bin/env bash

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

source "$ABSDIR/../scripts/ci/common.sh"

# stop on errors
set -ueo pipefail

#
# Propose open source code drop
#
# Flags (with defaults):
#   -h : help
#   -t : github token
#   -c : executor cpus (4)
#   -m : jvm heap size (5000M)
#   -s : jvm metaspace size (1000M)
#

function usage {
	echo "Usage: $0 [-h] [ -t {github-token} ] [ -c {number-cpus} ]  [ -m {heap-memory-size} ] [ -s {metaspace-size} ]"
	[ $# -gt 0 ] && echo "Error: $1"
	exit 1
}

export GITHUB_TOKEN=
EXECUTOR_NUM_CPUS=4
EXECUTOR_JVM_HEAP_SIZE=5000M
EXECUTOR_JVM_METASPACE_SIZE=1000M

while getopts "t:c:m:s:h" OPT
do
  case "${OPT}" in
    t) export GITHUB_TOKEN="${OPTARG}" ;;
    c) EXECUTOR_NUM_CPUS="${OPTARG}" ;;
    m) EXECUTOR_JVM_HEAP_SIZE="${OPTARG}" ;;
    s) EXECUTOR_JVM_METASPACE_SIZE="${OPTARG}" ;;
    *) usage ;;
  esac
done
shift $((OPTIND-1))

if [ -z "$GITHUB_TOKEN" ]; then
  echo github-token not set
  usage
fi

# sanity check for local testing that no targets have been built
# this ensures that no compiled artifacts are included in the open source code
TARGETS=$(find . -name target)
if [ ! -z "$TARGETS" ]; then
  echo "PRIVATE CANTON REPO NOT CLEAN. FOUND targets:"
  echo "${TARGETS}"
  exit 2
fi

# executable aliases
_GIT="git"
_HUB="hub"
_SBT=(sbt -verbose)

REMOTE=origin

# When not running on CI but locally, check that the branch is up to date
if [[ "${CI}" != "true"  ]]; then
  # sanity checks for non-main (e.g. release) branch that we are up to date
  # (main branch may be subject to race conditions due to parallel merges)
  BRANCH_PRIVATE=$("$_GIT" branch --show-current)
  if [ "$BRANCH_PRIVATE" != "main" ]; then

    # check that local branch is not behind remote or that there are no local changes
    if ! ("$_GIT" remote update --prune 2>&1 >/dev/null && "$_GIT" status) | grep -q "\(Your branch is up to date with '$REMOTE/$BRANCH_PRIVATE'\)\|\(nothing to commit, working tree clean\)"; then
      echo "BRANCH $BRANCH_PRIVATE NOT IN SYNC WITH $REMOTE/$BRANCH_PRIVATE. git status:"
      git status
      exit 3
    fi

    # check that local branch does not have additional changes compared to remote
    if ! ("$_GIT" diff --quiet $BRANCH_PRIVATE $REMOTE/$BRANCH_PRIVATE); then
      echo "LOCAL BRANCH $BRANCH_PRIVATE DIFFERS FROM REMOTE"
      exit 4
    fi
  fi
fi

# public branch parameters
DATE=$(date -u "+%Y-%m-%d.%H")
BRANCH_PUBLIC="main-open-source-${DATE}"
OWNER_PUBLIC="digital-asset"
REPO_PUBLIC=canton
BASE_PUBLIC=main
REPO_HTTP_URL_PUBLIC=https://${GITHUB_TOKEN}@github.com/$OWNER_PUBLIC/$REPO_PUBLIC.git

# fetch the private repo root from current circle-ci/git checkout" directory
ROOT_PRIVATE=$("$_GIT" rev-parse --show-toplevel)
COMMIT_HASH_PRIVATE=$("$_GIT" rev-parse --short HEAD)
ROOT_PUBLIC=~/canton-public

# Set up execution context size
_SBT+=("-J-Dscala.concurrent.context.numThreads=$EXECUTOR_NUM_CPUS")
_SBT+=("-J-Dscala.concurrent.context.maxThreads=$EXECUTOR_NUM_CPUS")

# Set up heap size
_SBT+=("-J-Xmx$EXECUTOR_JVM_HEAP_SIZE" "-J-Xms$EXECUTOR_JVM_HEAP_SIZE")

export MAX_CONCURRENT_SBT_TEST_TASKS=$EXECUTOR_NUM_CPUS
# Necessary workaround to prevent sbt from setting default JVM options
export SBT_OPTS="-Xmx$EXECUTOR_JVM_HEAP_SIZE"

# Set up metaspace
SBT_CMD+=("-J-XX:MaxMetaspaceSize=$EXECUTOR_JVM_METASPACE_SIZE")

DEBUG=1

# clone the public repo
if [ ! -d "$ROOT_PUBLIC" ]; then
  run "Cloning the public canton repo" $_GIT clone -b $BASE_PUBLIC --single-branch $REPO_HTTP_URL_PUBLIC $ROOT_PUBLIC
fi

# ensure user email and name are set
USER_EMAIL=$("$_GIT" config --get user.email || true)
if [ -z "$USER_EMAIL" ]; then
  run "Set user email to canton@digitalasset.com" $_GIT config --global user.email "canton@digitalasset.com"
fi
USER_NAME=$("$_GIT" config --get user.name || true)
if [ -z "$USER_NAME" ]; then
  run "Set user name to Canton" $_GIT config --global user.name "Canton"
fi

refresh() {
  FILE_OR_DIR=$1
  PARENT_DIR_NAME=$(dirname $FILE_OR_DIR)
  if [ -e $FILE_OR_DIR ]; then
    run "Removing $FILE_OR_DIR in preparation for refresh" $_GIT rm -rf $FILE_OR_DIR
  fi
  if [ ! -d $PARENT_DIR_NAME ]; then
    run "Creating the $PARENT_DIR_NAME directory" mkdir -p $PARENT_DIR_NAME
  fi
  run "Copying from $ROOT_PRIVATE/$FILE_OR_DIR to $ROOT_PUBLIC/$FILE_OR_DIR" cp -a $ROOT_PRIVATE/$FILE_OR_DIR $ROOT_PUBLIC/$FILE_OR_DIR
  run "Adding $FILE_OR_DIR to git" $_GIT add $FILE_OR_DIR
}

cd $ROOT_PUBLIC
run "Checking out public branch $BRANCH_PUBLIC" $_GIT checkout -b $BRANCH_PUBLIC

# Include everything for open sourcing with specific exceptions
for artifact in $(git -C "$ROOT_PRIVATE" ls-tree --name-only HEAD); do

  # Top-level exclusions from the code drop
  # docs: early access internal documentation with unpublished papers
  # theory: non-public reviews
  # .github: currently different github templates and workflows
  # LICENSE.txt: currently handled separately in the public repo
  case "$artifact" in docs|theory|.github|LICENSE.txt)
    echo "Skipping $artifact for refresh"
    continue
    ;;
  esac

  run "Refreshing $artifact" refresh $artifact
done

# Clean-up files/directories in the public repo that are no longer present in the private repo
for artifact in $(git ls-tree --name-only HEAD); do

  if [ ! -e "$ROOT_PRIVATE/$artifact" ]; then
    run "Removing $artifact that is no longer present in private repo" $_GIT rm -rf $artifact
  fi

done

# Exclude non-top-level files
for exclude in "scripts/planning.sc"; do
  run "Excluding $exclude" $_GIT rm -rf $exclude
done


# Test canton-community using unit tests
test_canton_community() {
  EXIT_CODE=0
  $_SBT "Test / compile" || EXIT_CODE=$?

  test_result=$($_SBT "community-app/testOnly com.digitalasset.canton.integration.tests.SimplestPingReferenceCommunityIntegrationTest" || EXIT_CODE=$?)
  tests_runs=$(echo "$test_result" | grep -oE "Total number of tests run: ([0-9]+)" | grep -v ": 0")

  if [ -z "${tests_runs}" ]; then
    echo "No tests ran. Check the path of the ping test."
    exit 1
  fi

  mv log $ROOT_PRIVATE/public-log
  return $EXIT_CODE
}

# Only commit if config has been changed
if git diff-index --quiet HEAD; then
  echo "No code changes - skipping PR"
else
  # Executing long-running, "interesting" commands by hand instead of via run makes it so that we can observe detailed
  # failure information "live".
  run "Running canton community ping test" test_canton_community

  MESSAGE="[${BASE_PUBLIC}] Update ${DATE}"
  REFERENCE_COMMIT="Reference commit: $COMMIT_HASH_PRIVATE"
  CIRCLECI_DATA="CCI url: $CIRCLE_BUILD_URL"

  # This config can help git handle big file, at the expense of extra memory usage.
  $_GIT config --global http.postBuffer 524288000

  run "Committing $MESSAGE $COMMIT_HASH_PRIVATE" $_GIT commit -m "$MESSAGE" -m "$REFERENCE_COMMIT"

  run "Pushing $REMOTE $BRANCH_PUBLIC" $_GIT push $REMOTE $BRANCH_PUBLIC

  run "Preparing pull request" $_HUB pull-request -b "$BASE_PUBLIC" -h "$BRANCH_PUBLIC" --message "$MESSAGE" --message "$REFERENCE_COMMIT" --message "$CIRCLECI_DATA" --reviewer "daravep,rgugliel-da,soren-da"
fi
