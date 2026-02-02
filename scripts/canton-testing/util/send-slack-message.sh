#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Posts a message to a Slack channel, if the webhook URL of the channel has
# been setup in the shell environment.
###############################################################################

set -eu -o pipefail

data="$(jo text="$1")"

if [[ -n $SLACK_CHANNEL ]] ; then
  curl -s -S -X POST -H 'Content-type: application/json' \
  --data "$data" \
  "$SLACK_CHANNEL"
  echo
else
  echo "Unable to send, as the channel is not setup."
fi
