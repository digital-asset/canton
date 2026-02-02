#!/bin/bash
set -x
set -e

# --- CONFIGURATION ---
DB_HOST="${POSTGRES_HOST:-localhost}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_USER="${POSTGRES_USER:-postgres}"
DB_NAME="${POSTGRES_DB:-postgres}"
export PGPASSWORD="${POSTGRES_PASSWORD:-postgres}"
OUTPUT_FILE="postgres-query-stats.sql"
TEMP_SCHEMA="query_stats_temp"

echo "--- Dump postgres query statistics, requires 'query-stats-postgres' container image ---"

# Create extensions gracefully
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -w -d "$DB_NAME" -q -c "
DO \$\$
BEGIN
    BEGIN
        CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
        CREATE EXTENSION IF NOT EXISTS pg_store_plans;
    EXCEPTION WHEN OTHERS THEN
        NULL; -- Ignore errors silently
    END;
END \$\$;
"

# Check for pg_store_plans extension. We ask the DB if the view exists.
#    -tAc: tuples only (no headers), unaligned (no whitespace padding).
HAS_PLANS=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -w -d "$DB_NAME" -tAc "
-- Return YES only if the view exists
SELECT CASE WHEN to_regclass('pg_store_plans') IS NOT NULL THEN 'YES' ELSE 'NO' END;
")

# Quit if pg_store_plans is missing
if [ "$HAS_PLANS" != "YES" ]; then
    echo "pg_store_plans extension is missing, skipping the dump"
    exit 0
fi

echo "pg_store_plans extension found. Proceeding with dump..."

# Materialize stats into temporary schema
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -w -d "$DB_NAME" <<EOF
BEGIN;
    DROP SCHEMA IF EXISTS $TEMP_SCHEMA CASCADE;
    CREATE SCHEMA $TEMP_SCHEMA;

    CREATE TABLE $TEMP_SCHEMA.stat_statements AS SELECT * FROM pg_stat_statements;
    CREATE TABLE $TEMP_SCHEMA.store_plans AS SELECT * FROM pg_store_plans;
COMMIT;
EOF

# Dump
pg_dump -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -w -d "$DB_NAME" \
    -n "$TEMP_SCHEMA" \
    --no-owner \
    --file "$OUTPUT_FILE"

# Compress
gzip -f "$OUTPUT_FILE"

# Cleanup
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -w -d "$DB_NAME" -c "DROP SCHEMA $TEMP_SCHEMA CASCADE;"

echo "--- Success: ${OUTPUT_FILE}.gz ---"
