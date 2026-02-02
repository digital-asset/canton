#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

BACKUP_DIR=/data/basebackup

echo "Creating base backup for primary..."
pg_basebackup -D "$BACKUP_DIR" -F t -U "$POSTGRES_USER"
touch "$BACKUP_DIR"/completed
