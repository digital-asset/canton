#!/usr/bin/env bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Build a Canton Docker image and optionally publish it
#   Usage: ./build_canton_image.sh is_nightly_release

set -euo pipefail
cd "$(dirname "$0")"

version_tag=$(date +"%Y%m%d")
release_suffix="local"

echo "... Preparing files for docker builds"
../pack-performance.sh -p -s
if [[ ! -e ../../canton-performance-bare.tar.gz ]]; then
  echo "where is my canton-performance-bare?"
  exit 1
fi
# unpack perf runner artefacts to docker build directory
(
  cd images/canton-perf-base
  rm -rf target
  mkdir target
  cd target
  tar zxvf ../../../../../canton-performance-bare.tar.gz
)

image_types=(perf-base perf-master perf-participants perf-synchronizer)
oci_snapshot_path="${OCI_REGISTRY:-"local"}/da-images/public-unstable/docker/"
oci_release_path="${OCI_REGISTRY:-"local"}/da-images/public/docker/"

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

  release_target="--tag ${oci_path}${short_tag} --tag ${oci_path}canton-${image_type}:$version_tag"

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
