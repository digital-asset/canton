#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Source this if you want to slack the exit status of the last-executed command.
# See `test-after-main-merge.sh` for example usage.
###############################################################################

slack-exit-status() {
  local EXIT_CODE=$?
  set +eu +o pipefail
  TEST_NAME=${CURRENT_JOB_NAME:-unknown}

  if [[ "${IS_LOCAL_DEV_RUN:-false}" == "true" ]]; then
    echo
    echo "[LOCAL RUN] Exit status for '$TEST_NAME' at $HOSTNAME: code $EXIT_CODE"
    if [ "$EXIT_CODE" -ne 0 ]; then
      LOGS_LOCATION=$( [ -d "$LOGS_DIR" ] && echo "$LOGS_DIR" || echo "${CRON_OUTPUT_FILE:-unknown}" )
      echo "[LOCAL RUN] Test failed. Logs location: $LOGS_LOCATION"
    fi
    echo
  else
    echo
    echo "Posting exit code $EXIT_CODE to slack..."
    if [ "$EXIT_CODE" -eq 0 ]; then
      send-slack-message.sh "Performance test '$TEST_NAME' has terminated normally at $HOSTNAME with exit code $EXIT_CODE."
    else
      # If the build failed, the logs directory does not yet exist; point to the CRON output file instead.
      LOGS_LOCATION=$( [ -d "$LOGS_DIR" ] && echo "$LOGS_DIR" || echo "${CRON_OUTPUT_FILE:-unknown}" )
      # Ping the CI rota (scripts/ci/select_rota.py) instead of the whole channel;
      # it degrades to a deterministic fallback pick if the rota sheet is unreachable.
      MENTION=""
      for id in $(python3 "$REPOSITORY_ROOT/scripts/ci/select_rota.py" --rotation ci 2>/dev/null); do
        if [[ "$id" =~ ^U[A-Z0-9]+$ ]]; then
          MENTION="$MENTION<@$id> "
        fi
      done
      send-slack-message.sh ":bangbang: Performance test '$TEST_NAME' has terminated at $HOSTNAME with non-zero exit code $EXIT_CODE! Log location is \`$LOGS_LOCATION\`. You are on rota, so please take a look. ${MENTION}"
    fi
  fi

  echo
}
