#!/usr/bin/env bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
cd "$(dirname "$0")"

version_tag=$(date +"%Y%m%d")
release_suffix="3.7.0-snapshot-$(date -u +%F).$(git rev-parse --short HEAD)"

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
oci_snapshot_path="${OCI_REGISTRY:-"local"}/da-images/playground/docker/"
oci_release_path="${OCI_REGISTRY:-"local"}/da-images/public/docker/"

if [[ "$release_suffix" == *"snapshot"* || "$release_suffix" == *"ad-hoc"* ]]; then
    oci_path="${oci_snapshot_path}"
    echo "Publishing named snapshot release to ${oci_path}"
else
    oci_path="${oci_release_path}"
    echo "Publishing full release to ${oci_path}"
fi

staging_suffix="${release_suffix}-pre"

delete_staging_tags() {
  echo "🧹 Deleting staging tags..."
  for image_type in "${image_types[@]}"; do
    gcloud artifacts docker tags delete "${oci_path}canton-${image_type}:${staging_suffix}" --quiet || true
    echo "✅ Deleted staging tag canton-${image_type}:${staging_suffix}"
  done
}

for image_type in "${image_types[@]}"; do
  short_tag="canton-${image_type}:${staging_suffix}"

  echo "⏳ Building ${image_type} → (staging alias ${short_tag})"

  dockerfile="images/canton-${image_type}/Dockerfile"
  context="images/canton-${image_type}"

  stage_target="--tag ${oci_path}${short_tag}"

  if [ "${CIRCLECI:-}" = "true" ]; then
    build_flags="--platform linux/amd64,linux/arm64 --push"
  else
    build_flags="--load"
  fi

  docker buildx build \
    --progress=plain \
    --build-arg "base_version=${staging_suffix}" \
    --build-arg "oci_path=${oci_path}" \
    ${stage_target} \
    -f "${dockerfile}" \
    ${build_flags} \
    "${context}"

  echo "✅ Finished ${image_type} image"
done

if [ "${CIRCLECI:-}" = "true" ]; then
  echo "▶️ Verifying staging images are multi-arch..."
  staging_refs=()
  for image_type in "${image_types[@]}"; do
    staging_refs+=("${oci_path}canton-${image_type}:${staging_suffix}")
  done
  set +e
  ../../scripts/ci/check-multiarch-images.py "${staging_refs[@]}"
  exit_code=$?
  set -e
  if [[ $exit_code -ne 0 ]]; then
    echo "❌ Multi-arch verification failed for staging images (exit code $exit_code). Aborting."
    delete_staging_tags
    exit $exit_code
  fi
  echo "✅ All staging images are multi-arch!"
fi

if [ "${CIRCLECI:-}" = "true" ]; then
  echo "▶️ Promoting staging images to release tags..."
  for image_type in "${image_types[@]}"; do
    docker buildx imagetools create \
      --tag "${oci_path}canton-${image_type}:${release_suffix}" \
      --tag "${oci_path}canton-${image_type}:${version_tag}" \
      "${oci_path}canton-${image_type}:${staging_suffix}"
    echo "✅ Published canton-${image_type}:${release_suffix}"
  done
  delete_staging_tags
else
  echo "▶️ Tagging staging images with release tags (local)..."
  for image_type in "${image_types[@]}"; do
    docker tag "${oci_path}canton-${image_type}:${staging_suffix}" "${oci_path}canton-${image_type}:${release_suffix}"
    docker tag "${oci_path}canton-${image_type}:${staging_suffix}" "${oci_path}canton-${image_type}:${version_tag}"
    # Removes the local -pre staging tag only; the image layers remain in the
    # local daemon under the release tags assigned above. Nothing is deleted from the registry.
    docker rmi "${oci_path}canton-${image_type}:${staging_suffix}"
    echo "✅ Tagged canton-${image_type}:${release_suffix}"
  done
fi
