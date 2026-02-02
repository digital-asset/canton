#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Test whether the replication works as expected.
###############################################################################

set -eu -o pipefail

if [[ -n "${1:-}" ]] ; then
  table_name="$1"
  shift
else
  table_name=testtable
fi

# Get the full path to the deployment directory
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

. "$SRCDIR"/scripts/load-settings.sh "$@"

if [[ $ENABLE_WAL_ARCHIVING != "yes" && $ENABLE_STREAMING_REPLICATION != "yes" ]] ; then
  echo "Skipping replication test, as both WAL archiving and streaming replication are switched off."
  exit 0
fi

sleep_if_necessary() {
  if [[ $ENABLE_STREAMING_REPLICATION != "yes" ]] ; then
    SLEEP_TIME=$((WAL_ARCHIVE_TIMEOUT + 5))
    echo "- Sleeping for $SLEEP_TIME seconds so that the replication can catch up..."
    sleep $SLEEP_TIME
  fi
}

on_exit() {
  local CODE="$?"
       if [[ $CODE != 0 ]] ; then
         echo "- Failed ($CODE)"
       fi
}
trap on_exit EXIT 

echo "Testing whether the replication works as expected."
echo
echo "Testing replication of empty table:"
echo "- Creating non-empty table $table_name..."
psql -v ON_ERROR_STOP=1 -h localhost -p "$POSTGRES_PRIMARY_APPLICATION_PORT" -U "$POSTGRES_USER" "$POSTGRES_DB" <<EOI > /dev/null
  set synchronous_commit=remote_apply;
  set client_min_messages = warning;
  create table $table_name(my_column varchar primary key);
  insert into $table_name(my_column) values ('magicstring');
EOI

sleep_if_necessary

echo "- Testing replication..."
psql -t -v ON_ERROR_STOP=1 -h localhost -p "$POSTGRES_SECONDARY_DIRECT_PORT" -U "$POSTGRES_USER" "$POSTGRES_DB" <<EOI |
  select * from $table_name;
EOI
grep magicstring > /dev/null
echo "- Success!"

echo
echo "Testing replication of non-empty table:"
echo "- Removing rows from table $table_name..."
psql -v ON_ERROR_STOP=1 -h localhost -p "$POSTGRES_PRIMARY_APPLICATION_PORT" -U "$POSTGRES_USER" "$POSTGRES_DB" <<EOI > /dev/null
  set synchronous_commit=remote_apply;
  delete from $table_name;
EOI

sleep_if_necessary

echo "- Testing replication..."
psql -t -v ON_ERROR_STOP=1 -h localhost -p "$POSTGRES_SECONDARY_DIRECT_PORT" -U "$POSTGRES_USER" "$POSTGRES_DB" <<EOI |
  select * from $table_name;
EOI
grep magicstring && exit 1
echo "- Success!"

echo
echo "Testing replication of non-empty table:"
echo "- Deleting table $table_name..."
psql -v ON_ERROR_STOP=1 -h localhost -p "$POSTGRES_PRIMARY_APPLICATION_PORT" -U "$POSTGRES_USER" "$POSTGRES_DB" <<EOI > /dev/null
  set synchronous_commit=remote_apply;
  drop table $table_name;
EOI

sleep_if_necessary

echo "- Testing replication..."
psql -t -v ON_ERROR_STOP=1 -h localhost -p "$POSTGRES_SECONDARY_DIRECT_PORT" -U "$POSTGRES_USER" "$POSTGRES_DB" <<EOI |
  select * from pg_tables where tablename = '$table_name';
EOI
grep "$table_name" && exit 1
echo "- Success!"
