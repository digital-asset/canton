#!/usr/bin/env bash
set -eu -o pipefail
# get the full path to the deployment directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
# shellcheck source=scripts/ci/common.sh
source "$ABSDIR/../scripts/ci/common.sh"
# shellcheck source=scripts/daml.sh
source "$ABSDIR/../scripts/daml.sh"
# Set params for multi-arch build
BUILD_PLATFORM_ARGS="--platform linux/amd64"
if [[  "$(uname -m)" != "x86_64" ]]; then
    BUILD_PLATFORM_ARGS="${BUILD_PLATFORM_ARGS} --platform linux/$(uname -m | sed 's/aarch64/arm64/;s/armv[0-9]\+l/armhf/') "
fi
# Function cleanup staff on exit
on_exit() {
  # Cleaning nix files
  info "Cleanup... Exit"
  rm -rf "$ABSDIR/shell.nix" "$ABSDIR/nix" "$ABSDIR/nix.tar"
}
# Trap - cleanup on exit
trap on_exit EXIT
# calculate files hash
hash_files() {
  local hash
  # sha256 the files
  # awk to remove whitespace (shasum is annoying)
  # cut as we can get away with just a shortened version of the sha256
  if ! hash=$(cat "$@" | sha256sum | awk '{print $1}' | cut -c -12) || [[ -z "$hash" ]]; then
    err "Failed to hash files: $@"
    exit 1
  fi
  echo "$hash"
}
# update docker image record in circleci configuration
update_circleci_config() {
  local -r image=$1
  local -r tag=$2
  # `files`- an array of files containing content matched by regex
  mapfile -t files < <(grep -RE  " digitalasset/${image}:[-_.[:alnum:]]*" $ABSDIR/../.circleci/config | awk -F":" '{print $1}')
  for file in "${files[@]}"
  do
    # The -i.bak incantation that works for both BSD and GNU seds, must delete the .bak file afterwards, though
    if ! sed -i.bak "s/digitalasset\/${image}:[-_.[:alnum:]]*/digitalasset\/${image}:${tag}/g" "${file}"; then
      err "Failed to update build image in ${file}"
      rm "${file}.bak"
      exit 1
    fi
      rm "${file}.bak"
  done
}
# calculate setup files hash
setupfile_hash=$(hash_files "$ABSDIR/setup-docker-build-container.sh") || exit 1
# ensure all nix files are available in the build context
cp -r "$ABSDIR/../shell.nix" "$ABSDIR/../nix" -t "$ABSDIR"
# calculate nix files hash
nix_hash=$(hash_files  "$ABSDIR/shell.nix" $(find "$ABSDIR/nix" -type f)) || exit 1
# prepare archive for inject info docker container
cd $ABSDIR && tar cf nix.tar shell.nix nix setup-docker-build-container.sh || exit 1
#
# canton build
#
org="digitalasset"

build_and_push_image() {
  local -r dockerfile="$1"
  local -r img="$2"
  local tag="$3"
  name="${org}/${img}:${tag}"
  info "Build Docker image: ${name}"
  # build container:
  docker buildx \
     build ${BUILD_PLATFORM_ARGS} \
    --file "${ABSDIR}/${dockerfile}" \
    "${ABSDIR}" \
    --pull \
    --compress \
    --progress plain \
    --tag "${name}"
  # calculate image size
  image_size="$(docker inspect ${name} --format='{{.Size}}' | numfmt --to=si)"
  # update circleci configuration and push image on CI
  set +euo pipefail
  if test "${CI}"; then
    info "Updating CircleCI configuration to use docker image"
    update_circleci_config "${img}" "${tag}"
    info "Pushing image to dockerhub"
    docker push "${name}"
  else
    info "Variable CI is not defined, set \"local\" tag on image"
    tag="local"
    if [ -n "$(docker images -q "${org}/${image}:${tag}" 2>/dev/null || true)" ]; then
      docker rmi ${org}/${img}:${tag}
    fi
    docker tag ${name} ${org}/${img}:${tag}
    name="${org}/${img}:${tag}"
  fi

  info "Image: \"$name\" Size: $image_size"
}

_print_header "Update docker build image"
dockerfile_hash=$(hash_files "$ABSDIR/Dockerfile.canton-build") || exit 1
tag="${dockerfile_hash}-${setupfile_hash}-${nix_hash}"
build_and_push_image "Dockerfile.canton-build" "canton-build" "$tag"

_print_header "Update query stats postgres image"
postgres_dockerfile_hash=$(hash_files "$ABSDIR/Dockerfile.query-stats-postgres") || exit 1
postgres_tag="17-${postgres_dockerfile_hash}"
build_and_push_image "Dockerfile.query-stats-postgres" "query-stats-postgres" "$postgres_tag"
