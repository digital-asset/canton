#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Runs the nightly performance test to record events.
###############################################################################

set -eu -o pipefail

echo
echo "*************************************************************************"
echo "* Starting test-with-recording..."
echo "*************************************************************************"

export CURRENT_JOB_NAME="test-with-recording"
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
. "$SRCDIR"/util/load-settings.sh -db postgres "$@"
set -o allexport
. "$DB_DOCKER_HOME/disable-replication.env"
. "$DB_DOCKER_HOME/disable-toxiproxy.env"
set +o allexport

bail-out-on-unrelated-processes.sh
# We always shutdown everything we set up before the script exits
trap teardown.sh EXIT

setup.sh

echo
echo "***** First full system test to initialize nodes..."

export TEST_DURATION=1
export TEST_DURATION_UNIT=second
export PARTICIPANT_HA=false
export SYNCHRONIZER_HA=false
test-full-system.sh

echo
echo "***** Storing db dumps..."

mkdir -p "$RECORDINGS_DIR"
dump() {
  pg_dump -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PRIMARY_APPLICATION_PORT" -w -F c -f "$RECORDINGS_DIR"/"$1" "$2"
}
dump participant1.dump participant1
dump mediator.dump mediator
dump sequencer.dump sequencer
dump block_reference.dump block_reference

echo
echo "***** Second full system test to record test events..."

export RECORDING_PREFIX="test"
export TEST_DURATION="$RECORDING_DURATION"
export TEST_DURATION_UNIT="$RECORDING_DURATION_UNIT"
export PARTICIPANT_HA=false
export SYNCHRONIZER_HA=false
test-full-system.sh
