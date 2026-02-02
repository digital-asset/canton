#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Starts a command and stores the pid so that it can be terminated with kill.
###############################################################################

set -eu -o pipefail

mkdir -p "$PIDS_DIR"

"$@" &
pid=$!

touch "$PIDS_DIR/$pid"

wait $pid

# This will delete the pid file on graceful termination and
# leave it, if the script is terminated with CTRL-C, because CTRL-C will only terminate the script, but not the command.
rm -rf "${PIDS_DIR:?}/${pid:?}"
