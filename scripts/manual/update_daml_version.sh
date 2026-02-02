#!/usr/bin/env bash

set +eo pipefail

. scripts/daml.sh
DEBUG=1
_CURL=curl


NEWEST_DAML_VERSION=$(fetch_newest_daml_version)
echo "Newest Daml version in artifactory: \"$NEWEST_DAML_VERSION\""
CANTON_DAML_VERSION=$(fetch_daml_version $PWD)
echo "Current Daml version in artifactory: \"$CANTON_DAML_VERSION\""

# change branch
BRANCH_NAME="upstream-upgrade-$NEWEST_DAML_VERSION"
echo "Check out $BRANCH_NAME"
git checkout -b $BRANCH_NAME

# first update daml project
update_daml_project_versions $CANTON_DAML_VERSION $NEWEST_DAML_VERSION

# create the pull request
create_daml_upgrade_pull_request $NEWEST_DAML_VERSION
