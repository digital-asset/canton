#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Short test to quickly find performance regressions.
# Intended to be invoked after every change on main.
# Uses a replicated db and DEBUG log level.
###############################################################################

set -eu -o pipefail

export CURRENT_JOB_NAME="test-after-main-merge"
echo
echo "*************************************************************************"
echo "* Starting test-after-main-merge (setup) ..."
echo "*************************************************************************"

SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
. "$SRCDIR"/util/load-settings.sh "$@"

. slack-exit-status.sh
# this trap is overwritten below
trap slack-exit-status EXIT

bail-out-on-unrelated-processes.sh

build.sh

export CURRENT_JOB_NAME="test-after-main-merge-postgres"
echo
echo "*************************************************************************"
echo "* Starting test-after-main-merge-postgres..."
echo "*************************************************************************"
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
echo "load settings"
. "$SRCDIR"/util/load-settings.sh -db postgres "$@"
# we need to slack the exit status as first thing because we don't want to slack the exit status of the teardown script
trap "slack-exit-status || true; teardown.sh" EXIT

setup.sh
export TEST_DURATION="$TEST_AFTER_MAIN_MERGE_DURATION"
export TEST_DURATION_UNIT="$TEST_AFTER_MAIN_MERGE_DURATION_UNIT"
export PARTICIPANT_HA=false
export SYNCHRONIZER_HA=false
export LOG_LEVEL_CANTON=DEBUG
echo "test full system"
test-full-system.sh 2>&1 | tee --append "$LOGS_DIR"/stdout.log
compute-and-publish-metrics.sh 30 90

trap "" EXIT # all good, disabling trap before shutting down everything
teardown.sh
