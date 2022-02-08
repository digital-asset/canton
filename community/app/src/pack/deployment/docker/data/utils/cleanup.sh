#!/bin/bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

if [ ! -e "shared" ]; then
  echo "ERROR. There is no shared directory. Please ensure that all the files this docker deployment needs are accessible"
  pwd
  ls -l
  exit 1
fi

if [ ! -w "shared" ] || [ ! -w "logs" ]; then
  echo "ERROR shared or logs directory are not writable. please change the permissions such that the docker process can write into these"
  exit 1
fi

# if the connect node is running, don't do anything (assuming we are restarting some processes)
nc -z -v connect.node 10011 > /dev/null 2>&1
ret=$?

# remove access tokens from shared file (at startup)
if [ $ret -ne 0 ]; then
  echo "connect node is not running. resetting shared directory"
  rm -f shared/*
  chmod 777 shared 
else
  echo "connect node is running, therefore i won't reset the shared directory"
fi

