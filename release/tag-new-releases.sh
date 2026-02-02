#!/usr/bin/env bash

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

# shellcheck source=./scripts/ci/common.sh
source "$ABSDIR/../scripts/ci/common.sh"

# configs
CHECK_COMMIT_COUNT=100 # how far back in history should we look for release commits
REMOTE=${REMOTE:-origin}

extract_releases_from_recent_commits() {
  git log -"$CHECK_COMMIT_COUNT" --format=tformat:%H,%s | grep -E "release: [0-9]+\.[0-9]+\.[0-9]+"
}

potential_commits=$(extract_releases_from_recent_commits)

[[ -z "$potential_commits" ]] && {
  info "No potential release commits were found"
  exit 0
}

potential_commits_count=$(wc -l <<< "$potential_commits" | tr -d ' ')

info "Found $potential_commits_count potential commits"

# we read from HEAD back so we'll see any reverted releases before we see the release commit
# keep track of what releases were reverted so we can just immediately exclude them from tagging
declare -a reverted_releases

extract_releases_from_recent_commits | while read -r sha_and_msg; do
  sha=$(cut -d',' -f1 <<< "$sha_and_msg")
  msg=$(cut -d',' -f2 <<< "$sha_and_msg")

  debug "SHA=$sha, Message=[$msg]"

  # pull out the version from the commit message
  if ! version=$([[ "$msg" =~ ([0-9]+\.[0-9]+\.[0-9]+(-[^\s]+)?) ]] && echo "${BASH_REMATCH[1]}"); then
    info "Ignoring [$sha] as version could not be successfully extracted from commit message [$msg]"
    continue
  fi

  # are we dealing with a revert of a release?
  if [[ $msg == Revert* ]]; then
      # record that the release has been reverted
      reverted_releases+=("$version")
      info "Will exclude reverted release [$version]"
      continue
  fi


  # check this release hasn't been reverted (i'm assuming versions will never include spaces to cheat when looking if it's in the array)
  if [[ " ${reverted_releases[@]} " =~ " ${version} "  ]]; then
    # this version has had a later revert commit so just ignore
    info "Ignoring [$version] as it was later reverted"
    continue
  fi

  tag="v${version}"

  # see if the tag already exists
  if git ls-remote --exit-code --tags "$REMOTE" "$tag"; then
    info "Ignoring [$sha] as the tag [$tag] already exists"
    continue
  fi

  # tag the release
  info "Creating tag [$tag] for release [$version] with sha [$sha]"
  run "Tag release locally" git tag -a -m "canton release $version" "$tag" "$sha"
  run "Push tag to remote" git push "$REMOTE" "$tag"
done
