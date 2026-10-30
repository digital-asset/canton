#!/usr/bin/env bash
set -eo pipefail

# The size-config classes are generated from the Ledger API protos and exclusions.txt, and checked in.
# sbt ledger-api-size-config/protocGenerate regenerates them in place, so afterwards any difference to the
# checked-in files means they are out of date.
GENERATED_DIR="community/ledger-api-size-config/src/main/scala"

changes="$(git status --porcelain -- "$GENERATED_DIR")"

if [ -n "$changes" ]; then
  echo "The generated size-config classes are out of date with the Ledger API protos or exclusions.txt:"
  echo "$changes"
  echo "Run 'sbt ledger-api-size-config/protocGenerate' and commit the changes in $GENERATED_DIR."
  exit 1
fi

echo "The generated size-config classes are up to date."
