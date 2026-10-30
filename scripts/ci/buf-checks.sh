#!/usr/bin/env bash

# The buf command will return a non-zero exit code if differences are found.
# Do not use `set -e` here. ProtobufCompatibilityReaderTest parses the output of all the `buf breaking` calls below, so each of them has to run and print its report.
# Instead, EVERY `buf breaking` call must end with `|| status=1`, and the script exits with `$status` at the end.
# A call without the suffix cannot fail the script, so its breaking changes would be reported but CI would stay green.
set -o pipefail

SNAPSHOT_TO_COMPARE=$1
status=0

mkdir -p tmp
buf build -o tmp/.proto-snapshot.bin.gz || exit 1

buf breaking tmp/.proto-snapshot.bin.gz --against $SNAPSHOT_TO_COMPARE --config protobuf-continuity-check/forwards-buf.yaml || status=1
buf breaking $SNAPSHOT_TO_COMPARE --against tmp/.proto-snapshot.bin.gz --config protobuf-continuity-check/backwards-buf.yaml || status=1

# checking the ledger api protos independently since they require stricter rules
buf breaking tmp/.proto-snapshot.bin.gz --against $SNAPSHOT_TO_COMPARE --config protobuf-continuity-check/ledger-api-buf.yaml || status=1

exit $status
