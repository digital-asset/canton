#!/usr/bin/env bash
set -eo pipefail

ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

info "Using tag '$CIRCLE_TAG'"
hub --version

private_github_token="${PRIVATE_GITHUB_TOKEN:-${GITHUB_TOKEN:-}}"
public_github_token="${PUBLIC_GITHUB_TOKEN:-${GITHUB_TOKEN:-}}"

release_asset_names() {
  local repo="$1"
  local token="$2"

  GH_TOKEN="$token" gh release view "v$version" -R "$repo" \
    --json assets \
    -q '.assets[] | select(.state == "uploaded") | .name'
}

release_expected_asset_names() {
  printf '%s\n' \
    "canton-open-source-$version.tar.gz" \
    "canton-open-source-$version.zip" \
    "canton-open-source-$version-api.tar.gz" \
    "canton-open-source-$version-api.zip"
}

release_missing_asset_names() {
  local repo="$1"
  local token="$2"
  local names

  names=$(release_asset_names "$repo" "$token") || return 1

  local asset
  while IFS= read -r asset; do
    [[ -n "$asset" ]] || continue
    if ! grep -Fqx "$asset" <<<"$names"; then
      printf '%s\n' "$asset"
    fi
  done < <(release_expected_asset_names)
}

release_upload_missing_assets() {
  local repo="$1"
  local token="$2"
  local missing="$3"
  local -a paths=()

  if [[ -z "$missing" ]]; then
    return 0
  fi

  local asset
  while IFS= read -r asset; do
    [[ -n "$asset" ]] || continue
    paths+=("/tmp/workspace/community/app/target/release/$asset")
  done <<<"$missing"

  GH_TOKEN="$token" gh release upload "v$version" "${paths[@]}" -R "$repo" --clobber
}

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

  release_failed=0

  if [[ -z "$private_github_token" ]]; then
    warn "Missing private GitHub token for release publishing"
    release_failed=1
  else
    info "Creating release in the private repository"
    export GITHUB_TOKEN="$private_github_token"

    if hub release show "v$version" >/dev/null 2>&1; then
      if ! private_missing=$(release_missing_asset_names "DACH-NY/canton" "$private_github_token" 2>/dev/null); then
        warn "Failed to inspect the private release asset list; failing to avoid a false green"
        release_failed=1
      elif [[ -n "$private_missing" ]]; then
        warn "Private release v$version exists but is incomplete; uploading missing assets to repair the release"
        if ! release_upload_missing_assets "DACH-NY/canton" "$private_github_token" "$private_missing"; then
          warn "Private GitHub release repair failed"
          release_failed=1
        fi
      else
        info "Release v$version already exists in the private repository with all assets, skipping"
      fi
    else
      # push to internal repo
      # note: wildcard can be used but at most one asset is pushed per --attach. Therefore, we use explicit names.
      if ! hub release create \
          "v$version" \
          --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version".tar.gz \
          --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version".zip \
          --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version"-api.tar.gz \
          --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version"-api.zip \
          --message "canton v$version" \
          --message "$release_notes"; then
        warn "Private GitHub release publish failed"
        release_failed=1
      fi
    fi
  fi

  if [[ -z "$public_github_token" ]]; then
    warn "Missing public GitHub token for release publishing"
    release_failed=1
  else
    # clone public repository (hub requires the git repo we're targetting in the working directory - even if only calling api endpoints like release)
    # can clone the repo bare as we don't actually need any of its content locally
    public_repo_dir="${RUNNER_TEMP:-/tmp}/canton-public"
    rm -rf "$public_repo_dir"
    git clone --bare \
      "https://${public_github_token}@github.com/digital-asset/canton.git" \
      "$public_repo_dir"

    info "Creating release in the OSS repository"

    # hub uses the repo in the current working directory
    cd "$public_repo_dir"
    export GITHUB_TOKEN="$public_github_token"

    if hub release show "v$version" >/dev/null 2>&1; then
      if ! public_missing=$(release_missing_asset_names "digital-asset/canton" "$public_github_token" 2>/dev/null); then
        warn "Failed to inspect the public release asset list; failing to avoid a false green"
        release_failed=1
      elif [[ -n "$public_missing" ]]; then
        warn "Public release v$version exists but is incomplete; uploading missing assets to repair the release"
        if ! release_upload_missing_assets "digital-asset/canton" "$public_github_token" "$public_missing"; then
          warn "Public GitHub release repair failed"
          release_failed=1
        fi
      else
        info "Release v$version already exists in the public repository with all assets, skipping"
      fi
    else
      # push new release to public repo
      # note: wildcard can be used but at most one asset is pushed per --attach. Therefore, we use explicit names.
      # commitish flag will cause the HEAD of `main` to be tagged with this release
      if ! hub release create \
          "v$version" \
          --commitish main \
          --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version".tar.gz \
          --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version".zip \
          --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version"-api.tar.gz \
          --attach /tmp/workspace/community/app/target/release/canton-open-source-"$version"-api.zip \
          --message "canton v${version}" \
          --message "$release_notes"; then
        warn "Public GitHub release publish failed"
        release_failed=1
      fi
    fi
  fi

  if [[ "$release_failed" -ne 0 ]]; then
    err "One or more GitHub release publishes failed"
    exit 1
  fi
fi
