#!/usr/bin/env bash
set -eo pipefail

ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

if [ -x "$(command -v aws)" ]; then
  aws --version
else
  echo "Missing tool required: aws"
  exit 1
fi

# For each DAR in this list, check if it is staged in Git. If it is, upload it
# to S3 with its hash.
declare -a test_dars
test_dars=(
  "community/daml-lf/archive/src/test/resources/DarReaderTest-v115.dar" # Build this DAR by running `daml build --target=1.15` in `daml-lf/archive/src/.../DarReaderTest` with the most recent 2.x compiler and placing the output on this path
  "community/daml-lf/transaction-tests/src/test/resources/InterfaceTestPackage-v1.dar" # Build this DAR by running `daml build --target=1.15` in `daml-lf/transaction-tests/src/.../InterfaceTestPackage` with the most recent 2.x compiler and placing the output on this path
)

for test_dar in "${test_dars[@]}"; do
  if [[ -e "$test_dar" ]]; then
    hash=$(sha1sum "$test_dar" | cut -f1 -d' ')
    dar_name="${test_dar##*/}"
    dar_name="${dar_name%.dar}"

    run " copy a test dar to s3" aws s3 cp "$test_dar" "s3://canton-public-releases/test-artifacts/$dar_name-$hash.dar"
  else
    echo "Can't find '$test_dar', not updating."
  fi
done
