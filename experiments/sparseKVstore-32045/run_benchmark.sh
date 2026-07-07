#!/bin/bash

# --- Defaults ---
VERSION="17"
SHARDS=4
KEYS=10000
PORT="5432"
USER="myuser"
PASS="mypassword"
DB="mydb"
PGS="10"
HOST="127.0.0.1"
CONTAINER_NAME="pg_validator"
DATA_TYPE="uuid"

# Current timestamp in milliseconds - only used in the log file name, but can be useful for performing manual queries
SQLTS=$(echo $(date +%s) | awk '{print $1 * 1000}')

# --- Usage Helper ---
usage() {
  echo "Usage: $0 [-v version] -t [data_type] [-s shards] [-k keys] [-p port] [-u user] [-d database] [-c page_size]"
  echo "  -v : postgres version (default: $VERSION)"
  echo "  -t : postgres data type for storing compound timestamp (default: $DATA_TYPE) - available options: int16|uuid|numeric|tuple"
  echo "  -s : Number of shards (default: $SHARDS)"
  echo "  -k : Total keys per shard (default: $KEYS)"
  echo "  -p : Postgres Port (default: $PORT)"
  echo "  -u : Postgres User (default: $USER)"
  echo "  -d : Database Name (default: $DB)"
  echo "  -c : Page size (default: $PGS)"
  exit 1
}

# --- Parse Arguments ---
while getopts "v:t:s:k:p:u:d:c:" opt; do
  case $opt in
    v) VERSION="$OPTARG" ;;
    t) case "$OPTARG" in
                int16|numeric|uuid|tuple)
                    DATA_TYPE="$OPTARG"
                    ;;
                *)
                    echo "Invalid timestamp data type: $OPTARG"
                    usage
                    ;;
            esac
            ;;
    s) SHARDS="$OPTARG" ;;
    k) KEYS="$OPTARG" ;;
    p) PORT="$OPTARG" ;;
    u) USER="$OPTARG" ;;
    d) DB="$OPTARG" ;;
    c) PGS="$OPTARG" ;;
    *) usage ;;
  esac
done

echo "Step 1: Spinning up Postgres $VERSION..."

# Remove any docker postgres with the same port (incl. volumes)
# Optimized cleanup to target only the specific container name or port conflict
docker stop $CONTAINER_NAME > /dev/null 2>&1
docker rm -f -v $CONTAINER_NAME > /dev/null 2>&1
docker rm -f -v $(docker ps | egrep postgres | egrep "${PORT}.+5432" | cut -f1 -d ' ') > /dev/null 2>&1


if [[ "$DATA_TYPE" == "int16" ]]; then

  IMAGE_NAME="postgres-uint128-v${VERSION}"

  echo "Building custom Postgres image with int16 support..."
  docker build --build-arg PG_VERSION=$VERSION -t $IMAGE_NAME .

  # Launch Docker
  docker run --name $CONTAINER_NAME \
    -p ${PORT}:5432 \
    -e POSTGRES_USER=$USER \
    -e POSTGRES_PASSWORD=$PASS \
    -e POSTGRES_DB=$DB \
    -d $IMAGE_NAME

else
  # Launch Docker
  docker run --name $CONTAINER_NAME \
    -p ${PORT}:5432 \
    -e POSTGRES_USER=$USER \
    -e POSTGRES_PASSWORD=$PASS \
    -e POSTGRES_DB=$DB \
    -d postgres:$VERSION

fi

# Wait for Postgres to be ready
echo -n "Waiting for database to initialize"
until docker exec $CONTAINER_NAME pg_isready -U $USER > /dev/null 2>&1; do
  printf "."
  sleep 1
done

if [[ "$DATA_TYPE" == "int16" ]]; then
  docker exec -it $CONTAINER_NAME psql -U "$USER" -d "$DB" -c "CREATE EXTENSION IF NOT EXISTS uint128;"
fi

echo " [READY]"

echo "----------------------------------------------------"
echo " Starting Postgres Validation Benchmark"
echo " Target: $USER@$HOST:$PORT/$DB"
echo " Scale: $SHARDS shards, $KEYS keys per shard"
echo "----------------------------------------------------"

# Folder where SQL files live
SQL_DIR="psql/${DATA_TYPE}"

# --- Execution ---
# We enter the SQL directory in a subshell so relative \i paths work in psql,
# but pipe the output to a log file in the original starting directory.
(
  cd "$SQL_DIR" || { echo "Error: Cannot find directory $SQL_DIR"; exit 1; }
  
  PGPASSWORD="$PASS" psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" \
       -v num_shards="$SHARDS" \
       -v keys_per_shard="$KEYS" \
       -v sh="$SHARDS" \
       -v page_size="$PGS" \
       -f run_all.sql
) 2>&1 | tee $SQL_DIR/full_postgres_${VERSION}_log_ts${SQLTS}.txt

# because of the pipe we run two commands
# So we need to use PIPESTATUS[0] to check the exit code for the command before the tee
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "----------------------------------------------------"
    echo "Benchmark completed successfully."
    echo "Log saved to: $(pwd)/full_postgres_${VERSION}_log_ts${SQLTS}.txt"
else
    echo "----------------------------------------------------"
    echo "Benchmark failed. Check full_postgres_${VERSION}_log_ts${SQLTS}.txt for details."
fi