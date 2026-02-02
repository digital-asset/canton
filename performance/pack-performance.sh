#!/bin/bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###########################################################
# Rebuild Canton enterprise release including performance runner
###########################################################

set -eu -o pipefail

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"

set -e
(
  cd "$REPOSITORY_ROOT"
  sbt "performance-driver/package; performance/package; community-app/bundle"
)

"$REPOSITORY_ROOT"/performance/bundle-performance.sh
