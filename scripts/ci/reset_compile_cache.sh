#!/usr/bin/env bash
set -euo pipefail
# Cleans compiled class outputs before each job so cache restoration is
# deterministic on reused runners. A cold rebuild (skip the network restore and
# recompile from scratch) happens only when the job asks for it AND the branch is
# on the force-recompile list, so the compile job rebuilds cold on those branches
# while every other job restores the freshly repopulated shared cache. Any other
# combination restores precompiled outputs from the shared cache.

RESET_IF_REQUESTED="${RESET_IF_REQUESTED:-false}"
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

if [[ "$RESET_IF_REQUESTED" == "true" && "$BRANCH_FORCE_RESET" == "true" ]]; then
  echo "Cold rebuild requested for a force-recompile branch, skipping network restore."
  echo "needs_network_restore=false" >> "$GITHUB_OUTPUT"
else
  echo "Target directories cleaned, network restore will be used."
  echo "needs_network_restore=true" >> "$GITHUB_OUTPUT"
fi
