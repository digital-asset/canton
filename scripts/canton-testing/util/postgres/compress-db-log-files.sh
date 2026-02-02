#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Compresses DB log files.
###############################################################################

set -eu -o pipefail

echo
echo "***** Compressing DB log files..."

gzip "$PRIMARY_LOGS_MOUNTPOINT"/*.log
gzip "$SECONDARY_LOGS_MOUNTPOINT"/*.log
