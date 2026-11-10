#!/bin/bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Source for record-corpus.sh: canton's own corpus tests (community/daml-lf/snapshot/corpus-tests),
# small scripts that exist to cover what the recording and replay must handle, such as contract
# keys, upgrades, interfaces and rollbacks. Each package under suites/ is one suite.
#
# Usage: canton-tests.sh REF WORK_DIR
# REF: a canton checkout

set -euo pipefail

REF=$1
WORK_DIR=$2
CANTON_DIR=$(realpath "$REF")
PROJECT=$CANTON_DIR/community/daml-lf/snapshot/corpus-tests
DAML_VERSION=${DAML_VERSION:-$(grep 'val version' "$CANTON_DIR/project/project/DamlVersions.scala" | cut -d\" -f2)}

(cd "$PROJECT" && TARGET_ROOT=$WORK_DIR/build DAML_VERSION=$DAML_VERSION dpm build --all)

mkdir -p "$WORK_DIR/dars"
cp "$WORK_DIR"/build/suites/*.dar "$WORK_DIR/dars"

cat > "$WORK_DIR/source.env" <<ENV
SOURCE_REPO=https://github.com/DACH-NY/canton
SOURCE_COMMIT=$(git -C "$CANTON_DIR" rev-parse HEAD)
ENV
