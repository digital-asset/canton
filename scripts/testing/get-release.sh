#!/usr/bin/env bash
# Downloads a Canton release from the public S3 bucket
# Usage: get-release.sh <release-to-download>

set -e -o pipefail

RELEASE=$1
TARGET_DIR="tmp/canton-open-source-$RELEASE"
TGZ_FILE="$TARGET_DIR.tar.gz"
if [[ -f $TGZ_FILE ]]; then
  echo "Release $RELEASE has already been downloaded to file $TGZ_FILE. Skipping download"
else
  echo "Downloading release $RELEASE"
  mkdir -p tmp
  curl -sSL --fail -o "$TGZ_FILE" \
    "https://canton-public-releases.s3.amazonaws.com/releases/canton-open-source-$RELEASE.tar.gz"
fi

echo "Unpacking to directory $TARGET_DIR"
mkdir -p "$TARGET_DIR"
ls "$TARGET_DIR"
tar xzf "$TGZ_FILE" -C "$TARGET_DIR/" --strip-components 1
