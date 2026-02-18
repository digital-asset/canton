#!/usr/bin/env bash
# Downloads a Canton enterprise release from artifactory
# Assumes that environment variables ARTIFACTORY_USER and ARTIFACTORY_PASSWORD are defined
# Usage: get-release.sh <release-to-download>

set -e -o pipefail

ARTIFACTORY_PASSWORD="${ARTIFACTORY_PASSWORD:-${ARTIFACTORY_TOKEN}}"
RELEASE=$1
TARGET_DIR="tmp/canton-enterprise-$RELEASE"
TGZ_FILE="$TARGET_DIR.tar.gz"
if [[ -f $TGZ_FILE ]]; then
  echo "Release $RELEASE has already been downloaded to file $TGZ_FILE. Skipping download"
else
  echo "Downloading release $RELEASE"
  mkdir -p tmp
  if [[ -z $ARTIFACTORY_PASSWORD ]]; then
    AUTH_ARGS=( --netrc )
  else
    AUTH_ARGS=( --user "$ARTIFACTORY_USER:$ARTIFACTORY_PASSWORD" )
  fi
  curl "${AUTH_ARGS[@]}" -sSL --fail -o "$TGZ_FILE" https://digitalasset.jfrog.io/artifactory/canton-enterprise/canton-enterprise-"$RELEASE".tar.gz
fi

echo "Unpacking to directory $TARGET_DIR"
mkdir -p "$TARGET_DIR"
ls "$TARGET_DIR"
tar xzf "$TGZ_FILE" -C "$TARGET_DIR/" --strip-components 1
