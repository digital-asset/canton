#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Set up the db for running Canton synchronizers, participants and performance runners.
###############################################################################

set -eu -o pipefail

echo
echo "***** Starting database..."

"$DB_UTIL_DIR"/db-setup.sh
mkdir -p $LOGS_DIR
