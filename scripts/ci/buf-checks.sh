#!/usr/bin/env bash

# The buf command will return a non-zero exist code if differences are found so do not exit on failure
set -o pipefail

SNAPSHOT_TO_COMPARE=$1

mkdir -p tmp
buf build -o tmp/.proto-snapshot.bin.gz

buf breaking tmp/.proto-snapshot.bin.gz --against $SNAPSHOT_TO_COMPARE --config protobuf-continuity-check/forwards-buf.yaml
buf breaking $SNAPSHOT_TO_COMPARE --against tmp/.proto-snapshot.bin.gz --config protobuf-continuity-check/backwards-buf.yaml

# checking the ledger api protos independently since they require stricter rules
buf breaking tmp/.proto-snapshot.bin.gz --against $SNAPSHOT_TO_COMPARE --config protobuf-continuity-check/ledger-api-buf.yaml
