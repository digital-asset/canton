#!/bin/bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# this script starts a certain trigger
if [ -z $3 ]; then
  echo "usage: $(basename $0) <dar-file> <party> <trigger-name>"
  exit 1
fi

if [ ! -e shared/parties.txt ]; then
  echo "missing shared/parties.txt, is the system running?"
  exit 1
fi

function find_package_id {
  local dar=$1
  daml damlc inspect $dar | grep "^package" | tail -n 1 | awk '{print $2}'
}

function find_party {
  local party=$1
  grep -E "^${party}" shared/parties.txt | tail -n 1 | awk '{print $2}'
}

if [ ! -e $1 ]; then
    echo "no such dar $1"
    exit 1
fi

package_id=$(find_package_id $1)
party=$(find_party $2)

if [ -z $party ]; then
    echo "no such party: $2"
    exit 1
fi


curl \
   -X POST localhost:4002/v1/triggers \
   -H "Content-type: application/json" \
   -H "Accept: application/json" \
   -d "{\"triggerName\":\"${package_id}:$3\", \"party\": \"${party}\"}"
