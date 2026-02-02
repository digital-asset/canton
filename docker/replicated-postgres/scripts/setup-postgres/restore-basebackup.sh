#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

BACKUP_DIR=/data/basebackup

if pg_ctl status &> /dev/null ; then
  echo "Unable to restore base backup while postgres is running..."
  exit 1
fi

echo -n "Awaiting base backup"
while [[ ! -f "$BACKUP_DIR/completed" ]] ; do
  echo -n .
  sleep 1
done
echo

echo "Restoring base backup..."
rm -rf "${PGDATA:?}/*"
tar xf "$BACKUP_DIR/base.tar" -C "$PGDATA"
tar xf "$BACKUP_DIR/pg_wal.tar" -C "$PGDATA/pg_wal"
