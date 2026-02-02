#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Set up DB specific environment
###############################################################################

UTIL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
DOCKER_DIR="$UTIL_DIR/../../../docker"

export DB_TYPE="$1"

case "$DB_TYPE" in
postgres)
  export DB_DOCKER_HOME="$DOCKER_DIR/replicated-postgres"
  ;;
static-postgres)
  ;;
*)
  echo "Usage $0 {postgres|static-postgres}" && exit 1
  ;;
esac

export DB_UTIL_DIR="$UTIL_DIR/$DB_TYPE"

# Load default database settings
if [[ -v DB_DOCKER_HOME && -e "$DB_DOCKER_HOME/default-settings.env" ]]; then
  . "$DB_DOCKER_HOME/default-settings.env"
else
  echo "skipping default docker settings"
fi

# Test database overrides
. "$UTIL_DIR/${DB_TYPE}/db-settings.env"
