#!/usr/bin/env bash

set -ueo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CANTON_DIR=$(dirname $(dirname $SCRIPT_DIR))
SPLICE_VERSION=0.5.6

echo $SCRIPT_DIR
echo $CANTON_DIR

cd $CANTON_DIR/dashboards

./generate-all.sh

cd generated/v11.0.0

mkdir -p $SCRIPT_DIR/grafana/dashboards/canton-internal
cp *.json $SCRIPT_DIR/grafana/dashboards/canton-internal

cd $SCRIPT_DIR

mkdir -p grafana/dashboards/splice
curl -L https://github.com/hyperledger-labs/splice/archive/refs/tags/${SPLICE_VERSION}.tar.gz | tar -xz -C grafana/dashboards/splice --strip-components=5 splice-${SPLICE_VERSION}/cluster/pulumi/infra/grafana-dashboards/
