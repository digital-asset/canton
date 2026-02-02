#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

port=$1
container_name=$2

timeout=1
max_timeout=30

echo -ne "Waiting until $container_name is serving postgres on port $port. (On Mac this may take a while.)\t..."
while ! (pg_isready -h localhost -p "$port" -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t "$timeout") ; do
  if [[ $(docker inspect --format="{{.State.Running}}" "$container_name") == "false" ]] ; then
    echo
    echo -e "${RED}Docker container $container_name terminated unexpectedly.${NC} Here is the log output..."
    docker logs "$container_name"
    exit 1
  fi
  echo -n "."
  timeout=$((timeout * 2 <= max_timeout ? timeout * 2 : max_timeout))
  sleep 1
done

echo -e " ${GREEN}done${NC}"
