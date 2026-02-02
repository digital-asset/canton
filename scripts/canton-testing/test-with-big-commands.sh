#!/usr/bin/env bash
#
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates
#
# Proprietary code. All rights reserved.
#

###############################################################################
# Runs the nightly performance test with a fast db and big commands.
###############################################################################

set -eu -o pipefail

export CURRENT_JOB_NAME="test-with-big-commands"

echo
echo "*************************************************************************"
echo "* Starting ${CURRENT_JOB_NAME} ..."
echo "*************************************************************************"

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

export TEST_DURATION="$TEST_WITH_BIG_COMMANDS_DURATION"
export TEST_DURATION_UNIT="$TEST_WITH_BIG_COMMANDS_DURATION_UNIT"

export PAYLOAD_SIZE=10000

# test-with-fast-db.sh serves as a baseline for this test.
# Therefore choosing the same parameters as in test-with-fast-db.sh.
export PARTICIPANT_HA=false
export SYNCHRONIZER_HA=false
export BATCH_SIZE=10

test-full-system.sh

compute-and-publish-metrics.sh 30 90
