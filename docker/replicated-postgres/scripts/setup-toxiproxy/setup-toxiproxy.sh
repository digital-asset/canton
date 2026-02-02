#!/bin/sh
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu

/go/bin/toxiproxy-cli -h toxiproxy:8474 create application_to_primary --listen 0.0.0.0:"$POSTGRES_PRIMARY_APPLICATION_PORT" --upstream postgres_primary:5432
/go/bin/toxiproxy-cli -h toxiproxy:8474 toxic add application_to_primary -t latency \
-a latency="$APPLICATION_TO_DB_REQUEST_LATENCY" -u
/go/bin/toxiproxy-cli -h toxiproxy:8474 toxic add application_to_primary -t latency \
-a latency="$APPLICATION_TO_DB_RESPONSE_LATENCY" -d

/go/bin/toxiproxy-cli -h toxiproxy:8474 create replication_to_primary --listen 0.0.0.0:"$POSTGRES_PRIMARY_REPLICATION_PORT" --upstream postgres_primary:5432
/go/bin/toxiproxy-cli -h toxiproxy:8474 toxic add replication_to_primary -t latency \
-a latency="$SECONDARY_TO_PRIMARY_REQUEST_LATENCY" -u

/go/bin/toxiproxy-cli -h toxiproxy:8474 toxic add replication_to_primary -t latency \
-a latency="$SECONDARY_TO_PRIMARY_RESPONSE_LATENCY" -d
