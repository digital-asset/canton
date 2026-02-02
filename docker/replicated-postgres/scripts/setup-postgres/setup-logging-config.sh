#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

cat >> "$PGDATA/postgresql.conf" <<EOF

# Configure logging
logging_collector = on
log_directory = '/data/logs'
log_file_mode = 0644
log_filename = 'postgresql-%Y-%m-%d-%H.log'

# Make view pg_stat_statements available
# This is handy for finding the most expensive queries
shared_preload_libraries = 'pg_stat_statements'

EOF
