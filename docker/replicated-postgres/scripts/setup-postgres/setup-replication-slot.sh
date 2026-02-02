#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

if [[ $ENABLE_REPLICATION_SLOT == "yes" ]] ; then

  psql --quiet -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB" <<EOI
    SELECT * FROM pg_create_physical_replication_slot('replication_slot');
EOI

fi
