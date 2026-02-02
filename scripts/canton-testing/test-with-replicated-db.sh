#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Runs the nightly performance test with a replicated db.
###############################################################################

set -eu -o pipefail

echo
echo "*************************************************************************"
echo "* Starting test-with-replicated-db..."
echo "*************************************************************************"

export CURRENT_JOB_NAME="test-with-replicated-db"
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
. "$SRCDIR"/util/load-settings.sh -db postgres "$@"

bail-out-on-unrelated-processes.sh
# We always shutdown everything we set up before the script exits
trap teardown.sh EXIT
setup.sh

export TEST_DURATION="$TEST_WITH_REPLICATED_DB_DURATION"
export TEST_DURATION_UNIT="$TEST_WITH_REPLICATED_DB_DURATION_UNIT"
export PARTICIPANT_HA=true
export SYNCHRONIZER_HA=true
test-full-system.sh

compute-and-publish-metrics.sh 30 90
