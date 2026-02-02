#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Builds canton enterprise.
###############################################################################

set -eu -o pipefail

echo
echo "***** Building Canton enterprise (including performance runner)..."

# "sbt clean" is needed: even though the repo will be cleaned when pulling the latest version,
# the sbt state is not clean in some situations, resulting in `java.lang.NoClassDefFoundError` errors.
(
  cd "$REPOSITORY_ROOT"
  # if here is a problem with sbt build, add a -debug here for debugging purposes, but
  # don't leave it there as it will use too much disk space on the test host
  sbt clean community-base/wartremoverClasspaths Test/compile performance-driver/package performance/package community-app/bundle
)
