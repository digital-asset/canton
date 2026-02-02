#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Runs the nightly performance test with a fast db and big command batches.
###############################################################################

set -eu -o pipefail

export CURRENT_JOB_NAME="test-with-fast-db"

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

export TEST_DURATION="$TEST_WITH_FAST_DB_DURATION"
export TEST_DURATION_UNIT="$TEST_WITH_FAST_DB_DURATION_UNIT"
export PARTICIPANT_HA=false
export SYNCHRONIZER_HA=false
export BATCH_SIZE=10
test-full-system.sh

compute-and-publish-metrics.sh 30 90
