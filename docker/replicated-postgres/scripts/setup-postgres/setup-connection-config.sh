#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

# Get the full path to the deployment directory
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

cat >> "$PGDATA/postgresql.conf" <<EOF

# Required to make pg accessible from outside of the docker container
listen_addresses = '*'

EOF

cp "$SRCDIR/pg_hba.conf" "$PGDATA/"
