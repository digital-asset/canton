#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

PATH="/scripts/setup-postgres:$PATH"

create-basebackup.sh

remove-all-config-files.sh

setup-connection-config.sh

setup-logging-config.sh

setup-performance-config.sh

setup-synchronous-commit-config.sh

setup-wal-archiving-config.sh

setup-replication-slot.sh
