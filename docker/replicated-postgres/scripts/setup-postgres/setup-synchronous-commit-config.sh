#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

if [[ $ENABLE_STREAMING_REPLICATION == "yes" ]] ; then
  STANDBY_NAMES="'*'"
else
  # Without streaming replication, the standby cannot report back whether a transaction has been successfully applied.
  STANDBY_NAMES="''"
fi

cat >> "$PGDATA/postgresql.conf" <<EOF

synchronous_commit = $SYNCHRONOUS_COMMIT
synchronous_standby_names = $STANDBY_NAMES

EOF
