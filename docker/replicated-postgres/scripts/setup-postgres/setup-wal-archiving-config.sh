#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

if [[ $ENABLE_WAL_ARCHIVING == "yes" ]] ; then
  ARCHIVE_MODE=on
else
  ARCHIVE_MODE=off
fi

if [[ $ENABLE_WAL_ARCHIVING == "yes" && $ENABLE_STREAMING_REPLICATION != "yes" ]] ; then
  # Without streaming replication, force archival of WAL segments so that the replication can be tested.
  ARCHIVE_TIMEOUT="$WAL_ARCHIVE_TIMEOUT"
else
  ARCHIVE_TIMEOUT=0
fi

cat >> "$PGDATA/postgresql.conf" <<EOF

# Enable wal archiving
wal_level = replica
archive_mode = $ARCHIVE_MODE
archive_command = 'cp %p /data/wal-archive/%f'
archive_timeout = $ARCHIVE_TIMEOUT

EOF
