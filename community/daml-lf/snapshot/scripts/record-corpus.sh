#!/bin/bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Records the Daml Script tests of a source on the IDE ledger and adds them to a corpus directory
# (see Corpus.scala). Run it inside the canton nix-shell (for dpm and sbt).
#
# A source is a script sources/<name>.sh taking REF and a work directory. It builds the tests and
# leaves in the work directory:
#   dars/<suite>.dar  one DAR per suite, whose scripts are the suite's tests
#   source.env        SOURCE_REPO and SOURCE_COMMIT

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

CLEAN=false
ARGS=()
for ARG in "$@"; do
  if [ "$ARG" = --clean ]; then CLEAN=true; else ARGS+=("$ARG"); fi
done

if [ "${#ARGS[@]}" -ne 3 ]; then
  echo "Usage: $(basename "$0") [--clean] SOURCE REF CORPUS_DIR" >&2
  echo "SOURCE: one of: $(cd "$SCRIPT_DIR/sources" && ls ./*.sh | xargs -n1 basename | sed 's/\.sh$//' | xargs)" >&2
  echo "REF: what to record, passed to the source; e.g. a git ref or a checkout" >&2
  echo "CORPUS_DIR: corpus to add the suites to; created if it doesn't exist" >&2
  echo "--clean: empty WORK_DIR before recording" >&2
  echo "Environment:" >&2
  echo "  WORK_DIR: directory for the build and the raw recordings (default: a temporary one)" >&2
  echo "  DAML_VERSION: daml-script version to record with (default: canton's pinned Daml)" >&2
  exit 1
fi

SOURCE=${ARGS[0]}
REF=${ARGS[1]}
CORPUS_DIR=${ARGS[2]}
REPO=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
DAML_VERSION=${DAML_VERSION:-$(grep 'val version' "$REPO/project/project/DamlVersions.scala" | cut -d\" -f2)}
WORK_DIR=${WORK_DIR:-$(mktemp -d)}
mkdir -p "$WORK_DIR"
WORK_DIR=$(realpath "$WORK_DIR")
# outputs of an earlier run would be recorded again (recordings are appended to)
if [ -n "$(ls -A "$WORK_DIR")" ]; then
  if [ "$CLEAN" = true ]; then
    find "$WORK_DIR" -mindepth 1 -delete
  else
    echo "WORK_DIR $WORK_DIR is not empty. To empty it first, run:" >&2
    CMD=$(printf ' %q' "$0" --clean "$@")
    echo "  WORK_DIR=$(printf '%q' "$WORK_DIR")$CMD" >&2
    exit 1
  fi
fi
mkdir -p "$CORPUS_DIR"
CORPUS_DIR=$(realpath "$CORPUS_DIR")
echo "Working in $WORK_DIR"

"$SCRIPT_DIR/sources/$SOURCE.sh" "$REF" "$WORK_DIR"
# shellcheck disable=SC1091
source "$WORK_DIR/source.env"

# dpm picks the daml-script version from the daml.yaml of the current directory
mkdir -p "$WORK_DIR/project" "$WORK_DIR/recordings" "$CORPUS_DIR/logs/$SOURCE"
cat > "$WORK_DIR/project/daml.yaml" <<EOF
override-components:
  daml-script:
    version: $DAML_VERSION
EOF

DAR_ARGS=()
FAILED=0
for DAR in "$WORK_DIR"/dars/*.dar; do
  SUITE=$(basename "$DAR" .dar)
  LOG=$CORPUS_DIR/logs/$SOURCE/$SUITE.log
  echo "Recording $SUITE"
  # a failing script doesn't stop the others; its transactions up to the failure are kept
  STATUS=0
  (cd "$WORK_DIR/project" &&
    dpm script --dar "$DAR" --all --ide-ledger --static-time --snapshot-dir "$WORK_DIR/recordings/$SUITE") > "$LOG" 2>&1 || STATUS=$?
  FAILURES=$(grep -c ' FAILURE' "$LOG" || true)
  # dpm script prints the results once all scripts ran, and exits with 1 if any failed
  if [ "$STATUS" -ne 0 ] && { [ "$STATUS" -ne 1 ] || [ "$FAILURES" -eq 0 ]; }; then
    echo "Recording $SUITE failed with exit status $STATUS (see $LOG)" >&2
    exit 1
  fi
  echo "  $(grep -c ' SUCCESS' "$LOG" || true) scripts succeeded, $FAILURES failed (see $LOG)"
  FAILED=$((FAILED + FAILURES))
  DAR_ARGS+=(--dar "$DAR")
done

(cd "$REPO" && sbt -batch "daml-lf-snapshot/runMain com.digitalasset.daml.lf.testing.snapshot.AddToCorpus \
  --corpus $CORPUS_DIR --recordings $WORK_DIR/recordings ${DAR_ARGS[*]} \
  --source $SOURCE --repo $SOURCE_REPO --commit $SOURCE_COMMIT --recorder daml-script-$DAML_VERSION")

echo "Added $SOURCE to $CORPUS_DIR"
if [ "$FAILED" -ne 0 ]; then
  echo "$FAILED scripts failed; see $CORPUS_DIR/logs/$SOURCE" >&2
fi
