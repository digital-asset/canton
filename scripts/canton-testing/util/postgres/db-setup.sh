#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Set up the db for running Canton synchronizers, participants and performance runners.
###############################################################################

set -eu -o pipefail

"$DB_DOCKER_HOME"/restart-with-purge.sh --inherit-settings

echo "***** Initialize database..."
PGPASSWORD="$POSTGRES_PASSWORD" \
psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "$POSTGRES_PRIMARY_APPLICATION_PORT" -U "$POSTGRES_USER" "$POSTGRES_DB" <<EOI > /dev/null
  create database "da";
  create database "sequencer";
  create database "mediator";
  create database "mediator_sec";
  create database "participant1";
  create database "ledger1";
  create database "block_reference";
EOI
