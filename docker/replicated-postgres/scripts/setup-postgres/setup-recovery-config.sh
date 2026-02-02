#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

if pg_ctl status &> /dev/null ; then
  echo "Unable to setup recovery.conf while postgres is running..."
  exit 1
fi

if [[ $ENABLE_STREAMING_REPLICATION == "yes" ]] ; then
  PRIMARY_CONNINFO_PROPERTY="primary_conninfo = 'host=toxiproxy port=$POSTGRES_PRIMARY_REPLICATION_PORT user=$POSTGRES_USER password=$POSTGRES_PASSWORD'"
else
  PRIMARY_CONNINFO_PROPERTY=""
fi

if [[ $ENABLE_REPLICATION_SLOT == "yes" ]] ; then
  PRIMARY_SLOT_PROPERTY="primary_slot_name = 'replication_slot'"
else
  PRIMARY_SLOT_PROPERTY=""
fi

touch "$PGDATA/standby.signal"

cat >> "$PGDATA/postgresql.conf" <<EOF

$PRIMARY_CONNINFO_PROPERTY
restore_command = 'cp /data/wal-archive/%f %p >/dev/null 2>&1'
archive_cleanup_command = 'pg_archivecleanup /data/wal-archive %r'
$PRIMARY_SLOT_PROPERTY

EOF
