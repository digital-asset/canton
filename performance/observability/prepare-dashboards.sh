#!/usr/bin/env bash

set -ueo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SPLICE_VERSION=0.7.0

cd $SCRIPT_DIR

mkdir -p grafana/dashboards/splice
curl -L https://github.com/hyperledger-labs/splice/archive/refs/tags/${SPLICE_VERSION}.tar.gz | tar -xz -C grafana/dashboards/splice --strip-components=5 splice-${SPLICE_VERSION}/cluster/pulumi/observability/grafana-dashboards/

