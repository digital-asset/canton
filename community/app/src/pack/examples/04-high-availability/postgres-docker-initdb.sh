#!/bin/bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER canton WITH PASSWORD 'supersafe';

    CREATE DATABASE sequencer_a OWNER canton;
    CREATE DATABASE sequencer_b OWNER canton;
    CREATE DATABASE mediator OWNER canton;
    CREATE DATABASE participant OWNER canton;
    CREATE DATABASE driver OWNER canton;
EOSQL

# Transfer public schema ownership in each database.
# Required for PostgreSQL 15+ where CREATE on public schema is no longer granted by default.
for db in sequencer_a sequencer_b mediator participant driver; do
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" <<-EOSQL
        ALTER SCHEMA public OWNER TO canton;
EOSQL
done
