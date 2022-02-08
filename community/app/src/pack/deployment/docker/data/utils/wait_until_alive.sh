#!/bin/bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

if [ -z "$2" ]; then
  echo "usage: wait_until_alive.sh host port"
  exit 1
fi

ret=1
while [ $ret -ne 0 ]; do
  nc -z -v $1 $2 > /dev/null 2>&1
  ret=$?
  if [ $ret -ne 0 ]; then
    echo "process on $1 $2 is not yet up, trying again"
    sleep 2
  fi
done
echo "process on $1 $2 appeared"
