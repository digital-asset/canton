#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Loads settings to the shell environment
###############################################################################

UTIL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

DEFAULT_SETTINGS_FILE="default-settings.env"

set -o allexport

CANTON_TESTING_DIR="$UTIL_DIR"/..

PATH="$CANTON_TESTING_DIR:$UTIL_DIR:$PATH"

# shellcheck disable=SC2034
CONFIG_DIR="$CANTON_TESTING_DIR"/config

REPOSITORY_ROOT="$CANTON_TESTING_DIR"/../..

BRANCH="$(cd "$REPOSITORY_ROOT"; git branch --show-current)"

echo "Loading default settings from $DEFAULT_SETTINGS_FILE..."
# shellcheck disable=SC1090
. "$CANTON_TESTING_DIR/$DEFAULT_SETTINGS_FILE"

echo "Loading db settings..."
# Load DB setting if DB environment provided
if [ $# -ge 2 ] && [ "$1" == "-db" ];then
  . "$UTIL_DIR/db-env.sh" "$2"
	shift;shift;
fi

# Set sequencer type if provided
if [ $# -ge 2 ] && [ "$1" == "-sequencer" ];then
  export SEQUENCER_TYPE="$2"
	shift;shift;
else
  export SEQUENCER_TYPE="db-sequencer"
fi

# Load overrides
for FILE in "$@" ; do
  echo "Loading custom settings from $FILE..."
  # shellcheck disable=SC1090
  . "$FILE"
done

ensure_dir_exists() {
  local dir_var="$1"
  if [ -n "$dir_var" ]; then
    mkdir -p "$dir_var"
  fi
}

ensure_dir_exists "${LOGS_DIR:-}"
ensure_dir_exists "${METRICS_BASE_DIR:-}"
ensure_dir_exists "${METRICS_DIR:-}"
ensure_dir_exists "${RECORDINGS_DIR:-}"
ensure_dir_exists "${PIDS_DIR:-}"

set +o allexport
