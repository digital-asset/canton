#!/usr/bin/env bash

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

# shellcheck source=./scripts/ci/common.sh
source "$ABSDIR/../scripts/ci/common.sh"

REMOTE=origin

# get the name of the tag for our HEAD
tag=$(git tag --points-at HEAD)
info "Found tag [$tag]"

[[ -z "$tag" ]] && {
  err "No tag name reported for HEAD"
  exit 1
}

# extract version
version=$([[ "$tag" =~ v(.*) ]] && echo "${BASH_REMATCH[1]}")
if [[ -z "$version" ]] ; then
  err "Could not extract version from tag [$tag]"
  exit 1
else
  info "Extracted version [$version] from tag [$tag]"
fi

# extract version from version.sbt
version_from_sbt=$(cut -d\" -f4 < version.sbt)
if [[ -z "$version_from_sbt" ]]; then
  err "Could not extract version from 'version.sbt'"
  exit 1
else
  info "Extracted version [$version] from 'version.sbt'"
fi

# sanity check versions from the tag and sbt match
if [[ "$version" != "$version_from_sbt" ]]; then
  err "Versions do not match: tag version [$version], sbt version [$version_from_sbt]"
  exit 1
else
  debug "Tag and sbt versions match"
fi

info "Release [$version] is valid"
echo "$version"
