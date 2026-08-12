#!/usr/bin/env bash
set -euo pipefail

# Run during Postgres first-time initialization.
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -c "ALTER USER \"$POSTGRES_USER\" CREATEDB CREATEROLE;"
