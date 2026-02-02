#!/usr/bin/env bash

# Common helpers and values to all the sub-parts of `propose.sh`

determine_base() {
  if [[ -n $BASE ]]; then
    echo $BASE
  else
    local -r current=$("$_GIT" rev-parse --abbrev-ref HEAD)
    # should be either main or a release branch
    regexp="^${RELEASE_LINE_BRANCH_PREFIX}-[0-9]+\.[0-9]+$"
    # the base branch is either the release-line branch or main
    if [[ ! $current =~ $regexp ]]; then
      info "Using main as base branch. Overwrite if this is not what you want."
      echo "main"
    else
      echo "$current"
    fi
  fi
}

# executable aliases
_CURL="curl"
_GIT="git"
_HUB="hub"

REMOTE=origin
RELEASE_LINE_BRANCH_PREFIX="release-line"
RELEASE_BRANCH_PREFIX=${RELEASE_BRANCH_PREFIX_OVERRIDE:-""}
BASE=$(determine_base)
OWNER=DACH-NY
REPO=canton
REPO_SSH_URL=git@github.com:$OWNER/$REPO.git
REPO_HTTP_BASE_URL=github.com/$OWNER/$REPO.git
REPO_HTTP_URL=https://$REPO_HTTP_BASE_URL

# fetch the root directory of the repository for file operations
REPO_ROOT=$("$_GIT" rev-parse --show-toplevel)
[[ -z "$REPO_ROOT" ]] && err "Failed to determine repository root directory" && exit 1

export RELEASE_SCRIPT_VERSION_FILE="${REPO_ROOT}/community/common/src/main/scala/com/digitalasset/canton/version/ReleaseVersions.scala"
export RELEASE_SCRIPT_VERSION_MAPPING_FILE="${REPO_ROOT}/community/common/src/main/scala/com/digitalasset/canton/version/ReleaseVersionToProtocolVersions.scala"

# Path to database migrations relative to repo root
# Kept relative for use as git pathspecs (git checkout/add)
export DB_MIGRATION_PATH="community/common/src/main/resources/db/migration/canton"
