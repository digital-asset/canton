#!/usr/bin/env bash
set -eo pipefail

ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

info "Using tag '$CIRCLE_TAG'"
hub --version

# we only publish formal releases to github
# so if we're not running on a release tag, drop out
if [[  "$CIRCLE_TAG" != v* ]]; then
  warn "Skipping Github releasing publishing as not a formal release"
  exit 0
fi

if ! [[ "$CIRCLE_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9a-zA-Z]+)?$ ]]; then
  warn "Skipping Github releasing on non-RC snapshot release"
else
  # verify we can release this build and extract its version
  if ! version=$(release/verify-authorized-release.sh); then
    exit 1
  fi

  # release candidates don't come with release notes
  get_release_notes() {
    if [[ "$version" =~ -[0-9a-zA-Z]+$ ]]; then
      warn "Release candidates such as $version don't come with release notes"
    else
      cat "release-notes/$version.md"
    fi
  }

  release_notes="$(get_release_notes)"

  info "Creating release in the private repository"

  # push to internal repo
  # note: wildcard can be used but at most one asset is pushed per --attach. Therefore, we use explicit names.
  hub release create \
      "v$version" \
      --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version".tar.gz \
      --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version".zip \
      --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version"-protobuf.tar.gz \
      --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version"-protobuf.zip \
      --message "canton v$version" \
      --message "$release_notes" || true # we don't want to fail: let's do as much as we can

  # clone public repository (hub requires the git repo we're targetting in the working directory - even if only calling api endpoints like release)
  # can clone the repo bare as we don't actually need any of its content locally
  git clone --bare \
      "https://${GITHUB_TOKEN}@github.com/digital-asset/canton.git" \
      ~/canton-public

  info "Creating release in the OSS repository"

  # hub uses the repo in the current working directory
  cd ~/canton-public && \
  # push new release to public repo
  # note: wildcard can be used but at most one asset is pushed per --attach. Therefore, we use explicit names.
  # commitish flag will cause the HEAD of `main` to be tagged with this release
  # will pick up the GITHUB_TOKEN environment variable that should be a token for our canton-machine account
  hub release create \
      "v$version" \
      --commitish main \
      --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version".tar.gz \
      --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version".zip \
      --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version"-protobuf.tar.gz \
      --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version"-protobuf.zip \
      --message "canton v${version}" \
      --message "$release_notes" || true # we don't want to fail: let's do as much as we can
fi
