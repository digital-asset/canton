#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Publishes metrics from $DATADOG_METRICS_FILE to DataDog.
###############################################################################

set -eu -o pipefail

. "$REPOSITORY_ROOT"/scripts/ci/io-utils.sh

echo
echo "***** Publishing metrics to datadog..."

timestamp="$TX_LATE_TS"

hash="$(cd "$REPOSITORY_ROOT"; git log -1 --pretty=%h)"
tags="$(jo -a "branch:$BRANCH" "hash:$hash" "job:$CURRENT_JOB_NAME" "host:$HOSTNAME")"

if [ -z "${DATADOG_API_KEY:-}" ];then
	echo "No Datadog API key detected, skipping Datadog metric publishing"
else
	with_input_as_params "$REPOSITORY_ROOT"/scripts/ci/post-to-datadog.sh "$timestamp" rate "$tags" < "$DATADOG_METRICS_FILE"
fi

