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
echo "* Starting test-participant-replay..."
echo "*************************************************************************"

export CURRENT_JOB_NAME="test-participant-replay"
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
. "$SRCDIR"/util/load-settings.sh -db postgres "$@"

bail-out-on-unrelated-processes.sh

# source subprocess elimination
. terminate-subprocesses-on-exit.sh

# We always teardown the DBs before the script exits
# Since this script runs jobs in the background, we also need to explicitly cleanup subprocesses.
# But we need to await the termination of synchronizers/subprocesses before tearing down the db.
trap "set +e; terminate-and-log; teardown.sh" EXIT

setup.sh

echo
echo "***** Restoring db dumps..."
restore() {
  pg_restore -U "$POSTGRES_USER" -h "$POSTGRES_HOST" -p "$POSTGRES_PRIMARY_APPLICATION_PORT" -w -d "$1" "$RECORDINGS_DIR"/"$2"
}
restore participant1 participant1.dump
restore mediator mediator.dump
restore sequencer sequencer.dump
restore block_reference block_reference.dump

echo
echo "***** Starting synchronizers..."
export JAVA_OPTS="$COMMON_JAVA_OPTS $SYNCHRONIZERS_JAVA_OPTS"
export JVM_NAME="synchronizers"
export SYNCHRONIZER_HA=false
run-in-namespace.sh "$SYNCHRONIZERS_NAMESPACE" \
"$CANTON_SCRIPT" daemon \
--manual-start \
--config "$CONFIG_DIR"/synchronizers.conf \
--config "$CONFIG_DIR"/"$DB_TYPE"/_persistence.conf \
--config "$CONFIG_DIR"/"$SEQUENCER_TYPE"/_sequencer_type.conf \
--bootstrap "$CONFIG_DIR"/run-synchronizers.canton \
--log-file-name "$SYNCHRONIZERS_LOG_FILE" &

echo
echo "***** Replaying events..."
(
  # Using sbt to start test, because this allows for faster iterations during performance tuning.

  cd "$REPOSITORY_ROOT"
  export LOG_FILE_NAME="$PARTICIPANTS_LOG_FILE"
  export LOG_FILE_ROLLING=1
  export PARTICIPANT_HA=true
  export SYNCHRONIZER_HA=false
  export JVM_NAME="participants"

  export JAVA_OPTS="$COMMON_JAVA_OPTS $PARTICIPANTS_JAVA_OPTS"

  run-in-namespace.sh "$PARTICIPANTS_NAMESPACE" \
  sbt -J-Dlogback.configurationFile=logback.xml "testOnly **.ParticipantReplayBenchmark"
)

# so metrics don't get confused if the synchronizer keeps runnning
terminate-subprocesses
compute-and-publish-metrics.sh 5 90
