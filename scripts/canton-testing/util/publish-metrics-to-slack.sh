#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Publishes metrics from $SLACK_METRICS_FILE to Slack.
###############################################################################

set -eu -o pipefail

echo
echo "***** Publishing metrics to slack..."

send-slack-message.sh "$(cat "$SLACK_METRICS_FILE")"
