#!/usr/bin/env bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Build a Canton Docker image and optionally publish it
#   Usage: ./build_canton_image.sh is_nightly_release

set -euo pipefail
cd "$(dirname "$0")"

get_major_minor() {
  version="$1"
  echo "$version" | awk -F. '{print $1 "." $2}'
}

release_suffix="${RELEASE_SUFFIX:-local}"

echo "... Preparing files for docker builds"
./prepare_dependencies.sh
./prepare_canton_tar.sh
oci_snapshot_path="${OCI_REGISTRY:-"local"}/da-images/public-unstable/docker/"
oci_release_path="${OCI_REGISTRY:-"local"}/da-images/public/docker/"
image_types=(base participant sequencer mediator)
nightly_release="${IS_NIGHTLY_RELEASE:-$1}" # CircleCI parameter nightly_release

if [[ "$release_suffix" == *"-SNAPSHOT" || ( "$nightly_release" == "true" && $(date +"%u") -ne 2 ) ]]; then
    echo "Skip publishing of unnamed snapshot release or a nightly release not on Tuesdays"
    exit 0
fi

if [[ "$release_suffix" == *"snapshot"* || "$release_suffix" == *"ad-hoc"* ]]; then
    oci_path="${oci_snapshot_path}"
    echo "Publishing named snapshot release to ${oci_path}"

else
    oci_path="${oci_release_path}"
    echo "Publishing full release to ${oci_path}"
fi

for image_type in "${image_types[@]}"; do
  short_tag="canton-${image_type}:${release_suffix}"

  echo "⏳ Building ${image_type} → (alias ${short_tag})"

  dockerfile="images/canton-${image_type}/Dockerfile"
  context="images/canton-${image_type}"

  release_target="--tag ${oci_path}${short_tag} --tag ${oci_path}canton-${image_type}:$(get_major_minor ${release_suffix})"

  if [ "${CIRCLECI:-}" = "true" ]; then
    build_flags="--platform linux/amd64,linux/arm64 --push"
  else
    build_flags="--load"
  fi

  docker buildx build \
    --progress=plain \
    --build-arg "base_version=${release_suffix}" \
    --build-arg "oci_path=${oci_path}" \
    ${release_target} \
    -f "${dockerfile}" \
    ${build_flags} \
    "${context}"


  echo "✅ Finished ${image_type} image"
done

echo "▶️ Running ping test for images (from ./tests/ping)..."
(
  cd ./tests/ping && OCI_PATH=$oci_path ./run_test.sh $release_suffix
)
exit_code=$?
if [[ $exit_code -ne 0 ]]; then
  echo "❌ Ping test failed for images (exit code $exit_code). Aborting."
  exit $exit_code
fi
echo "✅ Ping test passed for images!"

