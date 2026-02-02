#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

if grep -E -q -R --exclude-from=$SCRIPT_DIR/check-protobuf-wrappers.exclusion -e "google\.protobuf\.[[:alnum:]]*Value" $1; then
  echo "$1 contains Protobuf wrappers. Use optional instead."
  grep -E -R --exclude-from=$SCRIPT_DIR/check-protobuf-wrappers.exclusion -e "google\.protobuf\.[[:alnum:]]*Value" $1
  exit 1
fi
