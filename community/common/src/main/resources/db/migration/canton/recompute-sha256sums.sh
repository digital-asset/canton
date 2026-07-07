#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Once we have GA the following holds:
# The Flyway SQL migration scripts and the Scala-based migrations which have corresponding .sha256 files are frozen (should not be functionally changed, else existing deployments will be broken).
# As a rule of thumb, we never regenerate SHA files as part of ordinary development except when we fix a typo in a comment in an existing frozen migration script.
# Run from the repository root.
set -euxo pipefail

recompute() {
  local dir="$1" ext="$2"
  (
    cd "$dir"
    find . -type f,l -name "*.$ext" | xargs sha256sum |
      awk '{print "echo " $1 " > " $2}' | sed "s/\.$ext\$/\.sha256/" | sh
  )
}

# SQL migrations live in the resources tree.
recompute "community/common/src/main/resources/db/migration/canton" sql
# Scala-based migrations live in the source tree, accompanied by their own .sha256 files.
recompute "community/common/src/main/scala/db/migration/canton" scala
