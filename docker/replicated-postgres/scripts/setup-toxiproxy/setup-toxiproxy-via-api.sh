#!/usr/bin/env bash
#
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Creates the Toxiproxy proxies and latency toxics through Toxiproxy's HTTP API.
# Same setup as setup-toxiproxy.sh, but runs on the host, so the Toxiproxy
# image doesn't need a shell.
###############################################################################

set -eu -o pipefail

api="http://localhost:${TOXIPROXY_PORT}"

# Wait until the API answers
wait_for_api() {
  local attempt
  for attempt in $(seq 1 30); do
    curl -sf "$api/version" > /dev/null && return 0
    sleep 1
  done
  echo "Toxiproxy API at $api did not answer within 30 seconds" >&2
  return 1
}

# POSTs JSON to the Toxiproxy API; prints the response body, fails unless the status is 2xx
post_json() {
  local path="$1" body="$2" response status
  response=$(curl -s -w $'\n%{http_code}' -X POST "$api$path" \
    -H "Content-Type: application/json" -d "$body") || true
  status="${response##*$'\n'}"
  if [[ $status != 2?? ]]; then
    echo "POST $path failed (status ${status:-none}): ${response%$'\n'*}" >&2
    return 1
  fi
}

create_proxy() {
  local name="$1" port="$2"
  post_json /proxies \
    "{\"name\": \"$name\", \"listen\": \"0.0.0.0:$port\", \"upstream\": \"postgres_primary:5432\", \"enabled\": true}"
  echo "Created new proxy $name"
}

add_latency() {
  local proxy="$1" stream="$2" latency="$3"
  post_json "/proxies/$proxy/toxics" \
    "{\"name\": \"latency_$stream\", \"type\": \"latency\", \"stream\": \"$stream\", \"attributes\": {\"latency\": $latency}}"
  echo "Added $stream latency toxic 'latency_$stream' on proxy '$proxy'"
}

wait_for_api
create_proxy application_to_primary "$POSTGRES_PRIMARY_APPLICATION_PORT"
add_latency application_to_primary upstream "$APPLICATION_TO_DB_REQUEST_LATENCY"
add_latency application_to_primary downstream "$APPLICATION_TO_DB_RESPONSE_LATENCY"

create_proxy replication_to_primary "$POSTGRES_PRIMARY_REPLICATION_PORT"
add_latency replication_to_primary upstream "$SECONDARY_TO_PRIMARY_REQUEST_LATENCY"
add_latency replication_to_primary downstream "$SECONDARY_TO_PRIMARY_RESPONSE_LATENCY"
