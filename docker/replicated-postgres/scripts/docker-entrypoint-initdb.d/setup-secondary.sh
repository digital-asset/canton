#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

PATH="/scripts/setup-postgres:$PATH"

pg_ctl -D "$PGDATA" -w stop

restore-basebackup.sh
remove-all-config-files.sh
setup-connection-config.sh
setup-logging-config.sh
setup-performance-config.sh
setup-recovery-config.sh

pg_ctl -D "$PGDATA" -w start
