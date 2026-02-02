#!/usr/bin/env bash
# Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

### TEMPLATED MANAGED FILE, DO NOT EDIT
## If you need to modify this file, please edit relevant file in https://github.com/DACH-NY/grafana-tools

set -eu -o pipefail

DASHBOARDS_DIR="${1:-dashboards}"

declare -a versions=( "v11.0.0" )

mkdir -p generated
rm -rf generated/*

for version in "${versions[@]}"; do
  echo "Generating dashboards for Grafana $version in ${DASHBOARDS_DIR}"
  mkdir -p generated/"${version}"
  find -L "${DASHBOARDS_DIR}" -name '*.jsonnet' -exec sh -c '
      echo "Processing ${1}"
      make generate GRAFONNET_VERSION='"${version}"' DASHBOARD_SOURCE="${1}" DASHBOARD_GENERATED="generated/'"${version}"'/$(basename "${1}" .jsonnet).json"
    ' sh {} \;
  echo
done
