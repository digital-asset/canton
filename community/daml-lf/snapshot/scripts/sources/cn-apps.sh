#!/bin/bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Source for record-corpus.sh: the Daml Script tests of canton-network-apps, i.e. the DARs its
# own scripts/daml-test-all.sh runs.
#
# Usage: cn-apps.sh REF WORK_DIR
# REF: a canton-network-apps checkout, or a branch or tag of APPS_REPO to clone
# Environment:
#   APPS_REPO: repo to clone (default https://github.com/DACH-NY/canton-network-apps.git)
#   CN_BUNDLE_PATH: unpacked splice-node bundle; downloaded if unset

set -euo pipefail

REF=$1
WORK_DIR=$2
APPS_REPO=${APPS_REPO:-https://github.com/DACH-NY/canton-network-apps.git}

if [ -d "$REF" ]; then
  APPS_DIR=$(realpath "$REF")
else
  APPS_DIR=$WORK_DIR/cn-apps
  git clone --depth 1 --branch "$REF" "$APPS_REPO" "$APPS_DIR"
fi

if [ -z "${CN_BUNDLE_PATH:-}" ]; then
  SPLICE_VERSION=$(jq -r .version "$APPS_DIR/nix/canton-network-sources.json")
  export CN_BUNDLE_PATH=$WORK_DIR/splice
  mkdir -p "$CN_BUNDLE_PATH"
  curl -sSfL "https://github.com/digital-asset/decentralized-canton-sync/releases/download/v$SPLICE_VERSION/${SPLICE_VERSION}_splice-node.tar.gz" |
    tar --strip-components=1 -C "$CN_BUNDLE_PATH" -xz
fi

SDK_VERSION=$(grep -h -m1 '^sdk-version:' "$(find "$APPS_DIR/app" -name daml.yaml | head -1)" | awk '{print $2}')
dpm install "$SDK_VERSION"
(cd "$APPS_DIR" && dpm build --all)

mkdir -p "$WORK_DIR/dars"
find "$APPS_DIR/app/daml" -name "*-tests-v[0-9]-[0-9.]*.dar" -exec cp {} "$WORK_DIR/dars" \;
# bootstrap tests live in the main bootstrap DAR
cp "$APPS_DIR/.dars/digitalasset-bootstrap-v0-current.dar" "$WORK_DIR/dars"

cat > "$WORK_DIR/source.env" <<EOF
SOURCE_REPO=$APPS_REPO
SOURCE_COMMIT=$(git -C "$APPS_DIR" rev-parse HEAD)
EOF
