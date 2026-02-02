#!/usr/bin/env bash
set -eo pipefail

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
# shellcheck source=./common.sh
source "$ABSDIR/../daml.sh"
source "$ABSDIR/common.sh"

DEBUG=1
_CURL=curl

CANTON_DAML_VERSION=$(fetch_daml_version $PWD)
info "Current Daml version: \"$CANTON_DAML_VERSION\""
DPM_REGISTRY=$(fetch_dpm_registry $PWD)

NEWEST_DAML_VERSION=$(fetch_newest_daml_version)
info "Newest Daml version: \"$NEWEST_DAML_VERSION\""

# change branch
BRANCH_NAME="upstream-upgrade-$NEWEST_DAML_VERSION"
BRANCH_EXISTS=$($_GIT ls-remote --heads origin refs/heads/"${BRANCH_NAME}" | wc -l)

if [[ "${BRANCH_EXISTS}" -gt "0" ]]; then
    err "Branch $BRANCH_NAME already exists. Aborting."
    exit 0
else
  info "Check out $BRANCH_NAME"
  $_GIT checkout -b $BRANCH_NAME

  # first update daml project
  update_daml_project_versions $CANTON_DAML_VERSION $NEWEST_DAML_VERSION

  # create the pull request
  PR_URL=$(create_daml_upgrade_pull_request $NEWEST_DAML_VERSION)

  # Check the canton version is >= daml version, as it should be
  # Run at the end such that even if we fail here, the above logic can still do its job
  check_canton_version_is_not_smaller_than_daml_version $NEWEST_DAML_VERSION

  # export necessary variables for slack notification
  echo "export PR_URL=\"$PR_URL\"" >> $BASH_ENV
  echo "export CANTON_DAML_VERSION=\"$CANTON_DAML_VERSION\"" >> $BASH_ENV
  echo "export NEWEST_DAML_VERSION=\"$NEWEST_DAML_VERSION\"" >> $BASH_ENV
fi
