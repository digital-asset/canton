#!/usr/bin/env bash

# This script
# - Starts a postgres docker container on port 5544
# - Creates a user 'fly'
# - Run the flyway migrations (for ledger or canton databases)
# - Lists database tables
# - Enters a psql session
# - On exit from the psql session the docker container is removed

set -e -u

[ ! -d "community" ] && echo "Please run in top level canton directory" && exit 1

community="filesystem://$PWD/community"
cantonPostgres="$community/common/src/main/resources/db/migration/canton/postgres"
cantonLocations="$cantonPostgres/stable,$cantonPostgres/dev"
cantonStableLocations="$cantonPostgres/stable"
ledgerLocations="$community/ledger/ledger-api-core/src/main/resources/db/migration/postgres"
port=5544

case "${1:-}" in
    canton) locations="$cantonLocations" ;;
    canton-stable) locations="$cantonStableLocations" ;;
    ledger) locations="$ledgerLocations" ;;
    *) echo "Usage: $0 {canton|ledger|canton-stable}" && exit 1 ;;
esac

function banner {
  echo -e "\n# $1"
}

banner "Starting PostgreSQL"
docker run -d --name "psql-$port" -e POSTGRES_USER=pguser -e POSTGRES_PASSWORD=pgpass --rm -p "$port:5432" "postgres:17"
echo PGPASSWORD=pgpass pg_isready -h localhost -p "$port" -U pguser -t 10
while ! PGPASSWORD=pgpass pg_isready -h localhost -p "$port" -U pguser;do
  sleep 1;
done

function cleanup {
    banner "Stopping Postgres"
    docker stop "psql-$port"
}

trap cleanup EXIT


banner "Reset User Fly"
cat <<! | PGPASSWORD=pgpass psql -h localhost -p "$port" -U pguser
create database fly_db;
create user fly with password 'pgpass';
grant all privileges on database fly_db to fly;
\connect fly_db;
grant all on schema public to fly;
!

banner "Running flyway"
url="jdbc:postgresql://localhost:$port/fly_db?user=fly&password=pgpass"
flyway -url="$url" -locations="$locations" migrate

banner "Tables"
echo "\d" | PGPASSWORD=pgpass psql -h localhost -p $port -U fly fly_db

banner "pSQL"
PGPASSWORD=pgpass psql -h localhost -p $port -U fly fly_db

