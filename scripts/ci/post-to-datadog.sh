#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

TIMESTAMP="$1" # epoch seconds
shift
METRIC_TYPE="$1" # gauge, count or rate
shift
TAGS="$1" # json array with elements of form "tagname:tagvalue"
shift

series=()
for point in "$@"; do
  metric=${point%=*}
  value=${point#*=}
  if [[ -z "$value" ]]
  then
    echo "Empty value for $metric. Skipping..."
  else
    series+=("$(jo metric="$metric" type="$METRIC_TYPE" tags="$TAGS" points="[[$TIMESTAMP, $value]]")")
  fi
done

if [[ ${#series[@]} == 0 ]]
then
  echo "Series is empty. Skipping upload..."
else
  echo "Posting to Datadog: $*"
  json=$(jo -p series="$(jo -a "${series[@]}")")

  while ! curl -s -S -X POST -H "Content-type: application/json" -d "$json" "https://api.datadoghq.com/api/v1/series?api_key=$DATADOG_API_KEY" ; do
    sleep 3
    echo "Retrying..."
  done
  echo
fi
