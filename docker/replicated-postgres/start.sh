#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Starts all services reusing persisted data.
###############################################################################

set -eu -o pipefail

# Get the full path to the deployment directory
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

. "$SRCDIR"/scripts/load-settings.sh "$@"

create_dir() {
# shellcheck disable=SC2174
  mkdir -p -m777 "$1"
}

create_dir "$PRIMARY_PGDATA_MOUNTPOINT"
create_dir "$PRIMARY_LOGS_MOUNTPOINT"
create_dir "$SECONDARY_PGDATA_MOUNTPOINT"
create_dir "$SECONDARY_LOGS_MOUNTPOINT"
create_dir "$BASEBACKUP_MOUNTPOINT"
create_dir "$WALARCHIVE_MOUNTPOINT"

docker-compose -f "$SRCDIR/docker-compose.yml" --project-directory . --project-name "$PROJECT_NAME" \
up -d

# Configure Toxiproxy from the host through its HTTP API: the multi-arch image has no shell to run a script in.
"$SRCDIR/scripts/setup-toxiproxy/setup-toxiproxy-via-api.sh"

await-db.sh "$POSTGRES_PRIMARY_DIRECT_PORT" "${PROJECT_NAME}_primary"
await-db.sh "$POSTGRES_PRIMARY_APPLICATION_PORT" "${PROJECT_NAME}_toxiproxy"
await-db.sh "$POSTGRES_PRIMARY_REPLICATION_PORT" "${PROJECT_NAME}_toxiproxy"
await-db.sh "$POSTGRES_SECONDARY_DIRECT_PORT" "${PROJECT_NAME}_secondary"

"$SRCDIR/test-replication.sh" "testtable" --inherit-settings_
