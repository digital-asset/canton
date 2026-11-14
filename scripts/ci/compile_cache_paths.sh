#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# [TEST ONLY] Print the `target` directories of the sbt build, one per line, sorted.
#
# The compile cache used the path `**/target`. To expand it, actions/cache walks the whole
# workspace, including every class file inside every target directory (~10 s before each save).
# This lists the directories instead, from the checkout alone so that the restore (before
# anything is built) and the save produce the same list, which actions/cache requires because
# the paths are part of the cache version:
#   - the build's own target directories (root, project/, project/project/, ...);
#   - <dir>/target for every directory that holds a tracked `src/` tree;
#   - <dir>/target for every project base directory named with file("...") in the build files
#     (projects without sources of their own, such as generated-code-only projects and docs).
# Directories that do not exist at save time are skipped by actions/cache.
set -euo pipefail
export LC_ALL=C # same order on every runner: the list is part of the cache version

cd "$(git rev-parse --show-toplevel)"

{
  printf '%s\n' target project/target project/project/target project/project/project/target docs/target
  git ls-files -- '*/src/*' 'src/*' | sed -nE 's#^(.*/)?src/.*#\1target#p'
  git grep -hoE 'file\("[^"]+"\)' -- 'build.sbt' 'project/*.scala' 'project/*.sbt' |
    sed -E 's#^file\("\.?/?([^"]*)"\)$#\1#' |
    grep -vE '\.\.|^/|\.[a-z]+$' |
    sed -E 's#/$##; s#^$#.#; s#$#/target#; s#^\./##'
} | sort -u
