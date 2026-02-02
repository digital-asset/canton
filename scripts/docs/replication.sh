#!/usr/bin/env bash

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

source $THIS_DIR/../../release/versions.sh

set -eu -o pipefail

CANTON_LOCAL_VERSION=$(extract_local_version)
CANTON_FULL_VERSION=$(snapshot_version $(git rev-parse HEAD) $CANTON_LOCAL_VERSION)
CANTON_SHORT_VERSION=$(extract_major_minor $CANTON_LOCAL_VERSION)

amm $THIS_DIR/replication.sc $@ --shortVersion $CANTON_SHORT_VERSION --fullVersion $CANTON_FULL_VERSION --docsOpenDir $(realpath $THIS_DIR/../../docs-open)
