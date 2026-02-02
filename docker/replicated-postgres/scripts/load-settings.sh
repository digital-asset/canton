#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

DEFAULT_SETTINGS_FILE=default-settings.env

set -o allexport

# Get the full path to the script directory
SCRIPTDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

PATH="$SCRIPTDIR:$PATH"

# shellcheck disable=SC2034
RED="\033[0;31m"
# shellcheck disable=SC2034
GREEN="\033[0;32m"
# shellcheck disable=SC2034
NC="\033[0m"

if [[ -z ${1:-} ]] ; then
  echo "Loading default settings from $DEFAULT_SETTINGS_FILE..."
  # shellcheck disable=SC1090
  . "$SCRIPTDIR/../$DEFAULT_SETTINGS_FILE"
elif [[ $1 == "--inherit-settings" ]] ; then
  echo "Inheriting settings from calling environment..."
elif [[ $1 == "--inherit-settings_" ]] ; then
  true
else
  echo "Loading default settings from $DEFAULT_SETTINGS_FILE..."
  # shellcheck disable=SC1090
  . "$SCRIPTDIR/../$DEFAULT_SETTINGS_FILE"

  for FILE in "$@" ; do
    echo "Loading custom settings from $FILE..."
    # shellcheck disable=SC1090
    . "$FILE"
  done
fi

set +o allexport
