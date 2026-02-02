#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Purge all persistent data except for logs.
###############################################################################

set -eu -o pipefail

# Get the full path to the deployment directory
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

. "$SRCDIR"/scripts/load-settings.sh "$@"

"$SRCDIR/purge-replication-data.sh" --inherit-settings_
remove-directory.sh "$PRIMARY_PGDATA_MOUNTPOINT"
remove-directory.sh "$SECONDARY_PGDATA_MOUNTPOINT"
