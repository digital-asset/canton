#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Tears down the DB and deletes / compresses data created by tests.
###############################################################################

set -eu -o pipefail

"$DB_UTIL_DIR"/db-teardown.sh

compress-log-files.sh
