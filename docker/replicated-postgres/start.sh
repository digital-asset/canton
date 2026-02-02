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

docker-compose -f "$SRCDIR/docker-compose.yml" --project-directory . --project-name "$PROJECT_NAME" \
exec -T toxiproxy /scripts/setup-toxiproxy/setup-toxiproxy.sh
# The -T switch is required to run in from cron.

await-db.sh "$POSTGRES_PRIMARY_DIRECT_PORT" "${PROJECT_NAME}_primary"
await-db.sh "$POSTGRES_PRIMARY_APPLICATION_PORT" "${PROJECT_NAME}_toxiproxy"
await-db.sh "$POSTGRES_PRIMARY_REPLICATION_PORT" "${PROJECT_NAME}_toxiproxy"
await-db.sh "$POSTGRES_SECONDARY_DIRECT_PORT" "${PROJECT_NAME}_secondary"

"$SRCDIR/test-replication.sh" "testtable" --inherit-settings_
