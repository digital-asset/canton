#!/bin/bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

RETRIES=180

if [ -z "$1" ]; then
  echo "usage: wait_for_file.sh file"
  exit 1
fi

ret=0
while [ ! -e $1 ]; do
  let RETRIES=$RETRIES-1
  if [ $RETRIES -eq 0 ]; then
    echo "file $1 did not appear after many retries. giving up."
    exit 1
  fi
  sleep 1
done
