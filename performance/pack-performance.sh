#!/bin/bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###########################################################
# Rebuild Canton enterprise release including performance runner
###########################################################

set -eu -o pipefail

# if true, we will run the sbt build steps
build=true
runnerOnly=false
function usage {
	echo "Usage: $0 [-h] [ -s ]"
	echo "  -h  display this help"
	echo "  -s  skip sbt build step"
	echo "  -p  only package up performance runner artefacts without canton"
	[ $# -gt 0 ] && echo "Error: $1"
	exit 1
}

while getopts "hsp" OPT
do
  case "${OPT}" in
    s) build=false ;;
    p) runnerOnly=true ;;
    *) usage ;;
  esac
done
shift $((OPTIND-1))

REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"

if $runnerOnly; then
  TARGET="$REPOSITORY_ROOT/performance/target/packaging/performance"
  if [[ -e $TARGET ]]; then
    rm -rf ${TARGET}
  fi
else
  TARGET="$REPOSITORY_ROOT/community/app/target/release/canton/performance"
fi

if $build; then
  if $runnerOnly; then
    set -e
    (
      cd "$REPOSITORY_ROOT"
      sbt "performance-driver/package; performance/package"
    )
  else
    set -e
    (
      cd "$REPOSITORY_ROOT"
      sbt "performance-driver/package; performance/package; community-app/bundle"
    )
  fi
else
  echo "skipping build step"
fi

mkdir -p "${TARGET}"
cp -rv "$REPOSITORY_ROOT"/performance/src/main/console/* "${TARGET}"
cp -v "$REPOSITORY_ROOT/community/performance-driver/target/scala-2.13/resource_managed/main/PerformanceTest.dar" "${TARGET}"

mkdir -p "${TARGET}/lib"
cp -v "$REPOSITORY_ROOT"/performance/target/scala-2.13/performance_*.jar "${TARGET}/lib"
cp -v "$REPOSITORY_ROOT"/community/performance-driver/target/scala-2.13/performance-driver_*.jar "${TARGET}/lib"

if $runnerOnly; then
  (
    cd "$REPOSITORY_ROOT/performance/target/packaging"
    tar -hzcf "$REPOSITORY_ROOT/canton-performance-bare.tar.gz" performance
  )
else
  (
    cd "$REPOSITORY_ROOT/community/app/target/release"
    tar -hzcf "$REPOSITORY_ROOT/canton-performance.tar.gz" canton
  )
fi
