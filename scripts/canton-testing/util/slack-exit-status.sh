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

  echo
  echo "Posting exit code $EXIT_CODE to slack..."
  if [ "$EXIT_CODE" -eq 0 ]; then
    send-slack-message.sh "Performance test '$TEST_NAME' has terminated normally at $HOSTNAME with exit code $EXIT_CODE."
  else
    # If the build failed, the logs directory does not yet exist; point to the CRON output file instead.
    LOGS_LOCATION=$( [ -d "$LOGS_DIR" ] && echo "$LOGS_DIR" || echo "${CRON_OUTPUT_FILE:-unknown}" )
    send-slack-message.sh "<!here> :bangbang: Performance test '$TEST_NAME' has terminated at $HOSTNAME with non-zero exit code $EXIT_CODE! Log location is \`$LOGS_LOCATION\`. The person on support rotation should investigate."
  fi

  echo
}
