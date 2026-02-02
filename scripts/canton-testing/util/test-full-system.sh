#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Runs a full system performance test.
# You may want to define:
# - TEST_DURATION: duration of the test
# - TEST_DURATION_UNIT: time unit of the duration
# - RECORDING_PREFIX: enables recording if non-empty
# - PARTICIPANT_HA (true/false): enables high-availability on participants
# - SYNCHRONIZER_HA (true/false): enables high-availability on synchronizers
###############################################################################

echo "Log files are written to $LOGS_DIR"
LOG_FILE_PATTERN="yyyy-MM-dd-HH"
# keep three days of data
LOG_FILE_HISTORY=72

set -eu -o pipefail

# Since this script runs jobs in the background, we need to explicitly cleanup subprocesses.
. terminate-subprocesses-on-exit.sh
trap terminate-and-log EXIT

echo
echo "***** Starting synchronizers..."
export JAVA_OPTS="$COMMON_JAVA_OPTS $SYNCHRONIZERS_JAVA_OPTS"
export JVM_NAME="synchronizers"
run-in-namespace.sh "$SYNCHRONIZERS_NAMESPACE" \
"$CANTON_SCRIPT" daemon \
--manual-start \
--config "$CONFIG_DIR/synchronizers.conf" \
--config "$CONFIG_DIR"/"$DB_TYPE"/_persistence.conf \
--config "$CONFIG_DIR"/"$SEQUENCER_TYPE"/_sequencer_type.conf \
--bootstrap "$CONFIG_DIR"/run-synchronizers.canton \
--log-file-rolling-pattern $LOG_FILE_PATTERN \
--log-file-rolling-history $LOG_FILE_HISTORY \
--log-file-name "$SYNCHRONIZERS_LOG_FILE" &

if [[ $SYNCHRONIZER_HA == "true" ]]; then
  echo
  echo "***** Starting secondary synchronizers..."
  export JAVA_OPTS="$COMMON_JAVA_OPTS $SYNCHRONIZERS_SEC_JAVA_OPTS"
  export JVM_NAME="synchronizers-sec"

  run-in-namespace.sh "$SYNCHRONIZERS_NAMESPACE" \
  "$CANTON_SCRIPT" daemon \
  --manual-start \
  --config "$CONFIG_DIR/synchronizers-sec.conf" \
  --config "$CONFIG_DIR"/"$DB_TYPE"/_persistence.conf \
  --config "$CONFIG_DIR"/"$SEQUENCER_TYPE"/_sequencer_type.conf \
  --bootstrap "$CONFIG_DIR"/run-synchronizers-sec.canton \
  --log-file-rolling-pattern $LOG_FILE_PATTERN \
  --log-file-rolling-history $LOG_FILE_HISTORY \
  --log-file-name "$SYNCHRONIZERS_SEC_LOG_FILE" &
fi

echo
echo "***** Starting participants..."
export JAVA_OPTS="$COMMON_JAVA_OPTS $PARTICIPANTS_JAVA_OPTS"
export JVM_NAME="participants"
run-in-namespace.sh "$PARTICIPANTS_NAMESPACE" \
"$CANTON_SCRIPT" daemon \
--manual-start \
--config "$CONFIG_DIR"/participants.conf \
--config "$CONFIG_DIR"/"$DB_TYPE"/_persistence.conf \
--config "$CONFIG_DIR"/"$SEQUENCER_TYPE"/_sequencer_type.conf \
--bootstrap "$CONFIG_DIR"/run-participants.canton \
--log-file-rolling-pattern $LOG_FILE_PATTERN \
--log-file-rolling-history $LOG_FILE_HISTORY \
--log-file-name "$PARTICIPANTS_LOG_FILE" &

if [[ ${PARTICIPANT_HA:-} == "true" ]]; then
  echo
  echo "***** Starting secondary participants..."
  export JAVA_OPTS="$COMMON_JAVA_OPTS $PARTICIPANTS_SEC_JAVA_OPTS"
  export JVM_NAME="participants-sec"
  run-in-namespace.sh "$PARTICIPANTS_NAMESPACE" \
  "$CANTON_SCRIPT" daemon \
  --manual-start \
  --config "$CONFIG_DIR"/participants-sec.conf \
  --config "$CONFIG_DIR"/"$DB_TYPE"/_persistence.conf \
  --config "$CONFIG_DIR"/"$SEQUENCER_TYPE"/_sequencer_type.conf \
  --bootstrap "$CONFIG_DIR"/run-participants-sec.canton \
  --log-file-rolling-pattern $LOG_FILE_PATTERN \
  --log-file-rolling-history $LOG_FILE_HISTORY \
  --log-file-name "$PARTICIPANTS_SEC_LOG_FILE" &
fi

echo
echo "***** Starting performance runners..."
export JAVA_OPTS="$COMMON_JAVA_OPTS $PERFORMANCE_RUNNERS_JAVA_OPTS"
export JVM_NAME="performance-runners"
run-in-namespace.sh "" \
"$CANTON_SCRIPT" run "$CONFIG_DIR"/run-performance-runners.canton \
--config "$CONFIG_DIR"/performance-runners.conf \
--log-file-rolling-pattern $LOG_FILE_PATTERN \
--log-file-rolling-history $LOG_FILE_HISTORY \
--log-file-name "$PERFORMANCE_RUNNERS_LOG_FILE"
