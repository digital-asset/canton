#!/bin/bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

if [ -z $1 ]; then
  echo "usage: $(basename $0) <dar>"
  echo "upload dar to trigger service"
  exit 1
fi

if [ ! -e $1 ]; then
  echo "file $1 does not exist!"
  exit 1
fi

curl -F "dar=@$1" localhost:4002/v1/packages
