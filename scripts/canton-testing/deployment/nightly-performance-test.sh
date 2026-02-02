#!/usr/bin/env bash

set -eu -o pipefail

LOCK="${HOME}/lock"

# Wait until the lock can be acquired
echo -n "Trying to acquire lock"
while ! mkdir "$LOCK" &>/dev/null; do
  echo -n "."
  sleep 33 # small value, not a divisor of 5 minutes to increase chances of running before next iteration of regression-test.sh.
done
trap 'rmdir $LOCK' EXIT
echo " done"

# Make sure that docker can be accessed
sudo chmod 666 /var/run/docker.sock

cd canton

# load nix env
eval "$(direnv export bash)"

# needed so that bail-out-on-unrelated-processes.sh can send a slack message
. scripts/canton-testing/util/load-settings.sh scripts/canton-testing/canton-testing.env
scripts/canton-testing/util/bail-out-on-unrelated-processes.sh

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

# Setup network
scripts/canton-testing/setup-network.sh

echo
echo "***** Testing the following version:"
git log -1

scripts/canton-testing/nightly-performance-test.sh scripts/canton-testing/canton-testing.env

