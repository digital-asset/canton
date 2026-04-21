#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Script to generate load by running conformance tests multiple times (defaults to 10)

# Check required host tools are available before doing anything else
missing=0
if ! command -v grpcurl > /dev/null 2>&1; then
  echo "Error: grpcurl not found." >&2
  echo "  Install: https://github.com/fullstorydev/grpcurl#installation" >&2
  echo "  macOS:   brew install grpcurl" >&2
  echo "  Linux:   https://github.com/fullstorydev/grpcurl/releases" >&2
  missing=1
fi
if ! command -v jq > /dev/null 2>&1; then
  echo "Error: jq not found." >&2
  echo "  Install: https://jqlang.github.io/jq/download/" >&2
  echo "  macOS:   brew install jq" >&2
  echo "  Linux:   apt-get install jq  /  yum install jq" >&2
  missing=1
fi
if [ "${missing}" -eq 1 ]; then
  exit 1
fi

# Full path to this script
current_dir=$(cd "$(dirname "${0}")" && pwd)

wait_for_participant() {
  local name="${1}"
  local port="${2}"
  local retries=60
  local delay=5
  echo "### Waiting for ${name} (admin-api :${port}) to become active..."
  for attempt in $(seq 1 "${retries}"); do
    if grpcurl --plaintext "localhost:${port}" \
        com.digitalasset.canton.admin.participant.v30.ParticipantStatusService/ParticipantStatus 2>/dev/null \
        | jq -e '.status.commonStatus.active == true' > /dev/null 2>&1; then
      echo "### ✅ ${name} is active"
      return 0
    fi
    echo "### ⏳ Attempt ${attempt}/${retries}: ${name} not ready yet, retrying in ${delay}s..."
    sleep "${delay}"
  done
  echo "### ❌ ${name} did not become active after $((retries * delay))s"
  exit 1
}

# Verify all participants are up before generating any load
wait_for_participant "participant1" 10012
wait_for_participant "participant2" 10022

loops="${1:-10}"
case "${loops}" in
  ''|*[!0-9]*) echo 'Error: first argument must be an integer'; exit 1;;
  *) echo "### Running conformance tests ${loops} times";;
esac

for i in $(seq 1 "${loops}")
do
  echo '################################################################################'
  echo "### Iteration ${i} (out of ${loops})"
  "${current_dir}/test-ledger-api.sh" "${@:2}"
done

echo '### Done'
