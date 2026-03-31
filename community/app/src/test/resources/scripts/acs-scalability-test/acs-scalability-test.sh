#!/usr/bin/env bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# ---- The current directory ----
ORIG_DIR="$(pwd)"

# ---- Always return to original dir regardless of the exit signal ----
trap 'cd "$ORIG_DIR"' EXIT INT TERM HUP QUIT PIPE

# ---- Resolve script directory ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- Find sbt project root (in case the script runs in a sub dir) ----
find_project_root() {
  DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  while [[ "$DIR" != "/" ]]; do
    if [[ -f "$DIR/build.sbt" ]]; then
      echo "$DIR"
      return 0
    fi
    DIR="$(dirname "$DIR")"
  done
  echo "build.sbt not found in any parent directory" >&2
  return 1
}

PROJECT_ROOT="$(find_project_root)"
cd "$PROJECT_ROOT" || exit 1

# ---- Defaults ----
FULL_CLASS="com.digitalasset.canton.integration.tests.manual.acs.commitment.AcsCommitmentScalabilityTestOpenCommitment"
JFR_FILE="${PROJECT_ROOT}/tmp/test.jfr"
PROFILE_JFC="${SCRIPT_DIR}/profile.jfc"
XMX="8G"

# ---- Parse flags ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--testClass)
      FULL_CLASS="$2"
      shift 2
      ;;
    -o|--jfr)
      JFR_FILE="$2"
      shift 2
      ;;
    -c|--jfc)
      PROFILE_JFC="$2"
      shift 2
      ;;
    -x|--xmx)
      XMX="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage:"
      echo "  $0 [options]"
      echo
      echo "Options:"
      echo "  -t, --testClass   Fully qualified test class (default: com.digitalasset.canton.integration.tests.manual.acs.commitment.AcsCommitmentScalabilityTestOpenCommitment)"
      echo "  -o, --jfr         JFR output file (default: ${PROJECT_ROOT}/tmp/test.jfr)"
      echo "  -c, --jfc         JFC config file (default: ${SCRIPT_DIR}/profile.jfc)"
      echo "  -x, --xmx         Max heap size (default: 8GB)"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# ---- JVM options ----
JVM_OPTS="-Xmx${XMX} -Xms4G \
-XX:+UnlockDiagnosticVMOptions \
-XX:-DoEscapeAnalysis \
-XX:+DebugNonSafepoints \
-XX:StartFlightRecording=filename=${JFR_FILE},dumponexit=true,maxsize=4g,stackdepth=512,settings=${PROFILE_JFC}"

echo "Running test: $FULL_CLASS"
echo "with max Heap: -Xmx=$XMX"

# ---- Run sbt ----
SBT_OPTS="$JVM_OPTS" /usr/bin/time sbt "project community-app" "testOnly $FULL_CLASS"

echo "Run completed with max heap size $XMX!"
echo "JFR output: $JFR_FILE"
echo "JFC config: $PROFILE_JFC"
