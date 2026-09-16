#!/usr/bin/env bash
#
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Starts the ledger API performance test.
# Checks out the git branch specified and passes to the test runner
###############################################################################

set -eu -o pipefail

LOCK="${HOME}/lock"

on_exit() {
        rm -rf $LOCK
}

trap on_exit EXIT
# Wait until the lock can be acquired
echo -n "Trying to acquire lock"
while ! mkdir "$LOCK" &>/dev/null; do
        echo -n "."
        sleep 33
done

if [[ -d "./canton" ]]; then
        cd canton
else
        # suppose that git ssh access is configured
        git clone git@github.com:DACH-NY/canton.git
        cd canton
fi
# load nix env
eval "$(direnv export bash)"

# setting the branch here, because load-settings.sh sets BRANCH as well and therefore would
# overwrite the branch passed in as an argument
BRANCH="${1:-main}"
echo
echo "***** Pulling latest git revision..."
git fetch --all
git checkout "$BRANCH"
sudo git clean -xfd # sudo to also delete files if sbt has accidentally been run as root
git reset --hard
git pull --prune

echo
echo "***** Testing the following version:"
git log -1

LAPI_PERF_TEST_CONFIG_FILE=${2:-lapi-performance-test-config.json}

scripts/canton-testing/lapi-performance-test.sh $LAPI_PERF_TEST_CONFIG_FILE
