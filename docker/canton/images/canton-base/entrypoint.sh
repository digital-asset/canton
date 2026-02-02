#!/usr/bin/env bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

source /app/tools.sh

EXE=bin/canton

declare -a ARGS=()

# support starting the image as a remote console
if [[ ! " ${*} " =~ " --console " ]]; then
    ARGS+=( daemon --no-tty )
fi

ARGS+=( --log-encoder=json --log-level-stdout="${LOG_LEVEL_STDOUT:-DEBUG}" --log-level-canton="${LOG_LEVEL_CANTON:-DEBUG}" --log-file-appender=off )

if [ -f /app/logback.xml ]; then
   export JAVA_TOOL_OPTIONS="-Dlogback.configurationFile=/app/logback.xml ${JAVA_TOOL_OPTIONS:-}"
fi

if [ -f /app/pre-bootstrap.sh ]; then
  json_log "Running /app/pre-bootstrap.sh" "entrypoint.sh"
  source /app/pre-bootstrap.sh
fi

if [ -f /app/bootstrap.sc ]; then
  cp /app/bootstrap.sc /app/user-bootstrap.sc
  ARGS+=( --bootstrap /app/bootstrap-entrypoint.sc )
fi

if [ -f /app/app.conf ]; then
   ARGS+=( --config /app/app.conf )
fi

# Concatenate all additional configurations passed through env variables of the form ADDITIONAL_CONFIG*
for cfg in ${!ADDITIONAL_CONFIG@}; do
   echo "${!cfg}"
done >> /app/additional-config.conf

if [ -s "/app/monitoring.conf" ]; then
   ARGS+=( --config /app/monitoring.conf )
fi

if [ -s "/app/parameters.conf" ]; then
   ARGS+=( --config /app/parameters.conf )
fi

if [ -s "/app/additional-config.conf" ]; then
   ARGS+=( --config /app/additional-config.conf )
fi

json_log "Starting '${EXE}' with arguments: ${ARGS[*]}" "entrypoint.sh"

exec "$EXE" "${ARGS[@]}"
