#!/usr/bin/env bash
#
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Runs the nightly performance test.
###############################################################################

set -eu -o pipefail

export CURRENT_JOB_NAME="nightly-main"
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
. "$SRCDIR"/util/load-settings.sh "$@"

. slack-exit-status.sh
trap slack-exit-status EXIT

bail-out-on-unrelated-processes.sh

build.sh

test-with-replicated-db.sh "$@"

test-with-big-commands.sh "$@"

test-with-recording.sh "$@"

# This relies on the result of `test-with-recording.sh`
test-participant-replay.sh "$@"

test-sequencer-catchup.sh "$@"

test-mediator-replay.sh "$@"

test-sequencer-replay.sh "$@"

test-bong-benchmark.sh "$@"

test-sync-disconnect-reconnect.sh "$@"

echo
echo "***** Deleting recordings..."
rm -rf "$RECORDINGS_DIR"


if [[ -f "${METRICS_BASE_DIR:-}/summary.json" ]]; then
    echo
    echo "***** Summary of Nightly performance tests"
    render-metrics-table.py "${METRICS_BASE_DIR:-}/summary.json"
fi


