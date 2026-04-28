#!/usr/bin/env bash
set -euo pipefail
# Resets compiled classes when requested or when the
# branch is marked for full recompilation, otherwise
# decides whether a network restore is needed. Assigns
# var needs_network_restore either true or false.

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

if [[ "$RESET_REQUESTED" == "true" || "$BRANCH_FORCE_RESET" == "true" ]]; then
  echo "Resetting classes... wiping target folders."
  find . -type d -name target -exec rm -rf {} \;
  echo "needs_network_restore=false" >> "$GITHUB_OUTPUT"
else
  if [ -d "community/app/target" ] || [ -d "target" ] || [ -d "community/common/target" ]; then
    echo "Local target folders found on ARC runner. Using incremental build."
    echo "needs_network_restore=false" >> "$GITHUB_OUTPUT"
  else
    echo "No local target folders found. Network restore will be needed."
    echo "needs_network_restore=true" >> "$GITHUB_OUTPUT"
  fi
fi
