#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Reset everything, i.e., stop the services, purge all persistent data
# and start the services again.
###############################################################################

set -eu -o pipefail

# Get the full path to the deployment directory
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

. "$SRCDIR"/scripts/load-settings.sh "$@"

"$SRCDIR/stop.sh" --inherit-settings_
"$SRCDIR/purge-all.sh" --inherit-settings_
"$SRCDIR/start.sh" --inherit-settings_
