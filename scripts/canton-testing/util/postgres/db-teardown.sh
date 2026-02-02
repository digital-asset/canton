#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Tears down the DB and deletes data created by tests.
###############################################################################

set -eu -o pipefail

echo
echo "***** Stopping database..."
"$DB_DOCKER_HOME"/stop.sh --inherit-settings
"$DB_DOCKER_HOME"/purge-data.sh --inherit-settings
