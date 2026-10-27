#!/usr/bin/env bash

set -euo pipefail

if [ $# -ne 1 ]; then echo "Usage: $0 <config-file>"; exit 1; fi

CONFIG_FILE="$1"

if [ ! -f "$CONFIG_FILE" ]; then echo "Error: file not found: $CONFIG_FILE"; exit 1; fi

# Convert to an absolute path so that the file:// URI is valid regardless of
# the working directory.
CONFIG_FILE="$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE")"

export LOG_LEVEL_CANTON=INFO

export CANTON_PROTOCOL_VERSION=37

# target: db-testing
#export CI=1
#export POSTGRES_HOST=db-testing.da-int.net
#export POSTGRES_USER=test
#export POSTGRES_PASSWORD=supersafe
#export POSTGRES_DB=postgres

# target: local
export POSTGRES_USER="test-user"
export POSTGRES_PASSWORD="test-password"
export POSTGRES_DB=test-user
export POSTGRES_PORT=5432
export POSTGRES_HOST="localhost"

# When CI is defined, it ensures no dockerized Postgres is being used.
# export CI=1

export SBT_OPTS="-Xmx90G \
  -Dbft-ordering-benchmark.config=file://$CONFIG_FILE"

sbt "dumpClassPath; testOnly \
  com.digitalasset.canton.integration.tests.manual.BftOrderingBenchmark"
