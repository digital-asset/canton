#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Compresses log files.
###############################################################################

set -eu -o pipefail

echo
echo "***** Compressing log files..."

gzip "$LOGS_DIR"/*.log

$DB_UTIL_DIR/compress-db-log-files.sh

