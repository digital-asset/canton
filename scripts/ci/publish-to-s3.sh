#!/usr/bin/env bash
set -eo pipefail

ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

if [[ -z "$RELEASE_SUFFIX" ]]; then
    # the suffix gets defined in a previous job. we just reuse it here.
    err "ERROR, NO RELEASE_SUFFIX DEFINED! You need to run this job after the obtain_release_suffix job."
    exit 1
fi

# We don't publish "SNAPSHOT" releases, only properly dated snapshots or full releases.
if [[ "$RELEASE_SUFFIX" == *"-SNAPSHOT" ]]; then
  info "Skip publishing of snapshot release"
  exit 0
fi

filename="canton-open-source-${RELEASE_SUFFIX}"

if [ -x "$(command -v aws)" ]; then
  aws --version
else
  echo "Missing tool required: aws"
  exit 1
fi

echo "Release is ${RELEASE_SUFFIX}"
echo "Files in the release directory:"
ls -all /tmp/workspace/community/app/target/release/

run " copy tarball to s3" aws s3 cp "/tmp/workspace/community/app/target/release/canton-open-source-${RELEASE_SUFFIX}.tar.gz" "s3://canton-public-releases/releases/$filename.tar.gz"
run " copy zip to s3" aws s3 cp "/tmp/workspace/community/app/target/release/canton-open-source-${RELEASE_SUFFIX}.zip" "s3://canton-public-releases/releases/$filename.zip"

run " copy tarball to s3" aws s3 cp "/tmp/workspace/community/app/target/release/canton-open-source-${RELEASE_SUFFIX}-protobuf.tar.gz" "s3://canton-public-releases/releases/$filename-protobuf.tar.gz"
run " copy zip to s3" aws s3 cp "/tmp/workspace/community/app/target/release/canton-open-source-${RELEASE_SUFFIX}-protobuf.zip" "s3://canton-public-releases/releases/$filename-protobuf.zip"

run " copy scaladoc to s3" aws s3 cp /tmp/workspace/scaladoc.tar.gz "s3://canton-public-releases/releases/canton-scaladoc-${RELEASE_SUFFIX}.tar.gz"
# TODO(i29845): Uncomment the two following lines when `build_systematic_testing_inventory` is fixed
# run " copy test evidence to s3" aws s3 cp /tmp/workspace/security-tests.csv "s3://canton-public-releases/releases/canton-security-tests-${RELEASE_SUFFIX}.csv"
# run " copy test evidence to s3" aws s3 cp /tmp/workspace/reliability-tests.csv "s3://canton-public-releases/releases/canton-reliability-tests-${RELEASE_SUFFIX}.csv"

# this invalidation is probably unneeded as it's unlikely any prior file has been cached with the same path
# however it's easy and cheap, so can't hurt
# Cloudfront invalidation is known to be fragile, so bump the max number of attempts
# See https://github.com/aws/aws-cdk/issues/15891#issuecomment-966456154
# https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-retries.html#cli-usage-retries-configure
AWS_MAX_ATTEMPTS=10 aws cloudfront create-invalidation \
    --distribution-id "$AWS_CF_CANTON_IO_DISTRIBUTION_ID" \
    --paths "/releases/*"
info "Published to s3 and should be openly accessible at:"
info "  - https://www.canton.io/releases/$filename.tar.gz"
info "  - https://www.canton.io/releases/$filename.zip"
