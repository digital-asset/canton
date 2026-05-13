#!/usr/bin/env bash
set -eo pipefail

. scripts/daml.sh

DAML_VERSION=$(fetch_daml_version "$PWD")
GIT_SHA=$(git rev-parse HEAD)
echo "daml_version=$DAML_VERSION sha=$GIT_SHA" | tee info.properties

