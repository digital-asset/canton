#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Purge all persistent data.
###############################################################################

set -eu -o pipefail

# Get the full path to the deployment directory
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

. "$SRCDIR"/scripts/load-settings.sh "$@"

"$SRCDIR/purge-primary.sh" --inherit-settings_
"$SRCDIR/purge-secondary.sh" --inherit-settings_
"$SRCDIR/purge-replication-data.sh" --inherit-settings_
