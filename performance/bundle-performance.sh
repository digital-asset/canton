#!/bin/bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###########################################################
# Add performance runner to an existing Canton release.
###########################################################

set -eu -o pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"

TARGET="$REPOSITORY_ROOT/community/app/target/release/canton/performance"

mkdir -p "${TARGET}"
cp -rv "$REPOSITORY_ROOT"/performance/src/main/console/* "${TARGET}"
cp -v "$REPOSITORY_ROOT/community/performance-driver/target/scala-2.13/resource_managed/main/PerformanceTest.dar" "${TARGET}"

mkdir -p "${TARGET}/lib"
cp -v "$REPOSITORY_ROOT"/performance/target/scala-2.13/performance_*.jar "${TARGET}/lib"
cp -v "$REPOSITORY_ROOT"/community/performance-driver/target/scala-2.13/performance-driver_*.jar "${TARGET}/lib"

(
  cd "$REPOSITORY_ROOT/community/app/target/release"
  tar -hzcf "$REPOSITORY_ROOT/canton-performance.tar.gz" canton
)
