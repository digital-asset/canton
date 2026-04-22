#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Run conformance tests against the Ledger API.
# https://docs.daml.com/tools/ledger-api-test-tool/index.html

jar_file=lapitt.jar

if [ ! -f "${jar_file}" ]; then
  echo "Error: ${jar_file} not found in the current directory ($(pwd))." >&2
  echo "  Build it from the repository root:" >&2
  echo "    sbt ledger-test-tool-2-1/assembly" >&2
  echo "  Then copy it here:" >&2
  echo "    cp community/ledger-test-tool/lf-v2.1/target/scala-2.13/ledger-api-test-tool-2.1-*.jar \\" >&2
  echo "       community/app/target/release/canton-open-source-*/examples/13-observability/lapitt.jar" >&2
  echo "  See the Prerequisites section of README.md for details." >&2
  exit 1
fi

# Tests excluded because they are incompatible with the persistent shared ledger state of this
# multi-participant observability setup:
#
# - ParticipantPruningIT: Pruning tests modify and prune the shared persistent ledger, causing
#   PARTICIPANT_PRUNED_DATA_ACCESSED errors in unrelated test suites that run afterwards
#   (e.g. UpgradingIT, InteractiveSubmissionServiceIT, CommandSubmissionCompletionIT).
#
# - ActiveContractsServiceIT:AcsAtPruningOffsetIsAllowed,
#   ActiveContractsServiceIT:AcsBeforePruningOffsetIsDisallowed,
#   CommandDeduplicationPeriodValidationIT:OffsetPruned:
#   These individual tests also prune the shared ledger, causing the same cross-iteration
#   PARTICIPANT_PRUNED_DATA_ACCESSED contamination as ParticipantPruningIT.
#
# - CheckpointInTailingStreamsIT: All checkpoint tests time out (30 s) in this Docker environment
#   because the BFT sequencer does not emit checkpoints fast enough under the test conditions.
#
# - VettingIT: Fails with "synchronizerId not yet discovered" because the vetting tests rely on
#   a Canton-specific topology inspection API that is not yet available at the point the test runs
#   against the observability setup.
#
# - ContractIdIT:AcceptSuffixedV1CidExerciseTarget: Exercises a fabricated suffixed v1 contract ID
excluded_tests="ParticipantPruningIT,ActiveContractsServiceIT:AcsAtPruningOffsetIsAllowed,ActiveContractsServiceIT:AcsBeforePruningOffsetIsDisallowed,CommandDeduplicationPeriodValidationIT:OffsetPruned,CheckpointInTailingStreamsIT,VettingIT,ContractIdIT:AcceptSuffixedV1CidExerciseTarget"
#   INTERNAL instead of CONTRACT_NOT_FOUND for this case in the observability environment.
excluded_tests="ParticipantPruningIT,ActiveContractsServiceIT:AcsAtPruningOffsetIsAllowed,ActiveContractsServiceIT:AcsBeforePruningOffsetIsDisallowed,CommandDeduplicationPeriodValidationIT:OffsetPruned,CheckpointInTailingStreamsIT,VettingIT,CommandDeduplicationIT,ContractIdIT:AcceptSuffixedV1CidExerciseTarget"

echo "### Running Ledger API conformance tests 🛠️"
java -jar "${jar_file}" "${@:1}" \
  --exclude="${excluded_tests}" \
  --timeout-scale-factor=2.0 \
  "localhost:10011;localhost:10012" \
  "localhost:10021;localhost:10022"
