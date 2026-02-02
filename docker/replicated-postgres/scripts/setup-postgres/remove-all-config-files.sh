#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

rm -f "$PGDATA/postgresql.conf"
rm -f "$PGDATA/pg_hba.conf"
rm -f "$PGDATA/recovery.conf"
