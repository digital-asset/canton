#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Starts a command in the given network namespace.
###############################################################################

set -eu -o pipefail

NAMESPACE="$1"
shift

COMMAND=("$@")

if [[ -n $NAMESPACE ]]; then
  echo "Starting process from network namespace '$NAMESPACE'..."
  sudo -E \
  ip netns exec "$NAMESPACE" \
  sudo -E -u "$USER" \
  env PATH="$PATH" \
  store-pid-and-run.sh "${COMMAND[@]}"
else
  # If we don't echo, the script would exit with code 1 after being killed.
  echo "Starting process from default network namespace..."
  store-pid-and-run.sh "${COMMAND[@]}"
fi
