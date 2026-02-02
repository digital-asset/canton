#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Stop the services and keep persistent data.
###############################################################################

set -eu -o pipefail

# Get the full path to the deployment directory
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

. "$SRCDIR"/scripts/load-settings.sh "$@"

docker-compose -f "$SRCDIR/docker-compose.yml" --project-directory . --project-name "$PROJECT_NAME" \
down --remove-orphans
