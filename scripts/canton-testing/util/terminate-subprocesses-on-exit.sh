#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Source this to gain access to functions to terminate all processes started with
# 'store-pid-and-run.sh'. Consider orchestrating the functions via a trap (`trap terminate-and-log EXIT`).
###############################################################################

SCRIPT_NAME="$(basename "${BASH_SOURCE[1]}")"

get-subprocesses() {
  # shellcheck disable=SC2012
  ls "$PIDS_DIR" 2> /dev/null | tr '\n' ' ' || true
}

cleanup-subprocesses() {
  for pid in $(get-subprocesses); do
    if ! ps "$pid" &> /dev/null; then
      echo "***** $SCRIPT_NAME: subprocess $pid has terminated"
      rm -f "${PIDS_DIR:?}/${pid:?}"
    fi
  done
}

schedule-kill-unresponsive-subprocesses() {
  sleep "$CANTON_TERMINATION_TIMEOUT_SECONDS"
  for pid in "$@"; do
    if ps "$pid" &> /dev/null; then
      echo
      echo "***** $SCRIPT_NAME: killing subprocess with PID: $pid"
      kill -s KILL "$pid" &> /dev/null || true
    fi
  done
  cleanup-subprocesses
}

terminate-subprocesses() {
  local STORED_PIDS
  STORED_PIDS="$(get-subprocesses)"
  echo
  echo "***** $SCRIPT_NAME: terminating subprocesses. PIDs: $STORED_PIDS"
  if [[ -n $STORED_PIDS ]]; then
    # Kill with TERM to make sure all received events will get recorded during test-with-recording.
    # shellcheck disable=SC2086
    kill -s TERM $STORED_PIDS &> /dev/null || true

    # shellcheck disable=SC2086
    schedule-kill-unresponsive-subprocesses $STORED_PIDS &

    echo
    echo "***** $SCRIPT_NAME: awaiting termination of subprocesses. PIDs: $STORED_PIDS"
    # shellcheck disable=SC2086
    while ps $STORED_PIDS &> /dev/null; do
      sleep 1
      cleanup-subprocesses
    done
    cleanup-subprocesses
  fi
}

terminate-and-log() {
  local EXIT_CODE=$?
  echo
  echo "***** $SCRIPT_NAME: exit code is $EXIT_CODE."

  terminate-subprocesses
}


