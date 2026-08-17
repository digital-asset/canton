#!/usr/bin/env bash
set -euo pipefail
# Cleans compiled class outputs before each job so cache restoration is
# deterministic on reused runners. When a clean rebuild is requested, the job
# skips the network cache restore and recompiles from scratch. Otherwise it
# restores precompiled outputs from the shared cache.

RESET_REQUESTED="${RESET_REQUESTED:-false}"
BRANCH_NAME="${BRANCH_NAME:-main}"

BRANCH_FORCE_RESET="false"
FORCE_RECOMPILE_FILE=".circleci/branches_to_be_fully_recompiled_in_ci.txt"

if [ -f "$FORCE_RECOMPILE_FILE" ]; then
  if echo "$BRANCH_NAME" | grep -qEf <(grep -Ev '^\s*($|#)' "$FORCE_RECOMPILE_FILE" | grep .); then
    BRANCH_FORCE_RESET="true"
    echo "Branch $BRANCH_NAME detected in force-recompile list."
  fi
fi

echo "Cleaning local target directories before restore/build."
find . -type d -name target -prune -exec rm -rf {} +

if [[ "$RESET_REQUESTED" == "true" || "$BRANCH_FORCE_RESET" == "true" ]]; then
  echo "Clean rebuild requested, skipping network restore."
  echo "needs_network_restore=false" >> "$GITHUB_OUTPUT"
else
  echo "Target directories cleaned, network restore will be used."
  echo "needs_network_restore=true" >> "$GITHUB_OUTPUT"
fi
