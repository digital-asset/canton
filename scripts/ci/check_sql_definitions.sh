#!/usr/bin/env bash
set -eo pipefail

echo "running static checks on our schemas"
for folder in "community"; do
    for db in "h2" "postgres"; do
      python3 scripts/ci/check_sql_definitions.py --type $db $folder/common/src/main/resources/db/migration/canton/$db
    done
done
