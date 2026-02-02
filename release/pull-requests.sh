#!/usr/bin/env bash

# Helpers to create pull requests

hub_installed() {
  command -v "${_HUB}" >/dev/null 2>&1
}

discover_open_command() {
  # attempt xdg-open first as a open command likely exists on linux but does something very different from what we'd like
  if command -v xdg-open >/dev/null 2>&1; then
    echo "xdg-open" # probably on Linux
  elif command -v open >/dev/null 2>&1; then
    echo "open" # probably on MacOS
  else
    exit 1 # you're on your own
  fi
}

try_open_url() {
  local -r url=$1
  local open_command

  if open_command=$(discover_open_command); then
    if ! "$open_command" "$url"; then
      warn "Opening url with [$open_command] failed. Please open the following url in a browser [$url]."
    fi
  else
    info "Please open [$url]"
  fi
}

create_pull_request() {
  local -r branch="$1"
  local -r labels="$2"        # comma separated list of labels to apply to the PR
  local -r description="$3"   # description to use for the PR in Github. The first line will automatically become the title.
  local -r base=$4
  local url

  echo "$1 / $2 / $3"
  echo "$4"

  if hub_installed ; then
    debug "Creating PR for [$branch] with labels [$labels]"

    if ! url=$("$_HUB" pull-request -b "$base" -h "$branch" -r "DACH-NY/canton-release-owners" -l "$labels" --message="$description"); then
      # if hub fails to create the pull request (typically due to being misconfigured) fall back on just suggesting the pull request
      warn "Failed to create pull request with 'hub' so falling back on the github UI"
      suggest_pull_request "$branch" "$base"
    else
      info "Created pull request [$url]"
      try_open_url "$url"
    fi
  else
    suggest_pull_request "$branch" "$base"
  fi
}

create_release_pull_request() {
  local -r release_version=$1
  local -r next_version=$2
  local -r branch=$3
  local -r description=$(cat <<-EOM
release: canton $release_version

This change:
 - Creates a release commit for \`$release_version\`
 - Then the following commit prepares \`$BASE\` for development on \`$next_version\`.

This pull request **MUST** be merged with the _Rebase and Merge_ strategy in GitHub so the commits are not combined.

Release Checklist:
 - [ ] Code freeze is announced in #team-canton.
 - [ ] The release notes are clean and properly written.
 - [ ] The database migrations for this version have been merged into VXX_upgrade_schema_to_X.Y
 - [ ] Steps in https://github.com/DACH-NY/canton/blob/main/contributing/release-process.md#reviewing-and-merging-the-pr is followed.
EOM
  )

  create_pull_request "$branch" "release,Standard-Change" "$description" "$BASE"
}

create_snapshot_pull_request() {
  local -r release_version=$1
  local -r branch=$2
  local -r description=$(cat <<-EOM
snapshot release: canton $release_version

This change:
 - Creates a release commit for \`$release_version\`
 - Then the following commit returns \`$BASE\` to continue on the snapshot version

This pull request **MUST** be merged with the _Rebase and Merge_ strategy in GitHub so the commits are not combined.

_Pull request automatically created by [release/propose.sh](https://github.com/DACH-NY/canton/blob/$BASE/release/propose.sh)_.

Release Checklist:
 - [ ] Yes, I understand that this release will only appear in artifactory as $release_version
EOM
  )

  create_pull_request "$branch" "release,Standard-Change" "$description" "$BASE"
}

create_deployment_pull_request() {
  local -r release_version=$1
  local -r branch=$2

  local -r description=$(cat <<-EOM
deployment: approve deploy of $release_version to canton.global

This change:
  - Adds a production deployment approval file allowing \`deployment/google-cloud.sh\` to deploy to production.

_Pull request automatically created by [release/propose.sh](https://github.com/DACH-NY/canton/blob/$BASE/release/propose.sh)_.
EOM
  )

  create_pull_request "$branch" "Standard-Change" "$description" "$BASE"
}

suggest_pull_request() {
  local -r branch=$1
  local -r base=$2

  warn "Install 'hub' to automatically create the pull request: https://hub.github.com/"

  info "Please create the pull request yourself and include the release owner group as a reviewer (canton-release-owners)"
  try_open_url "https://github.com/DACH-NY/canton/compare/$base...$branch?expand=1"
}

create_update_pr_on_main() {
  local -r release_version=$1
  local -r branch=$2
  push_to_remote "$branch"
  # create a new PR
  local -r description=$(cat <<-EOM
Release wrap-up: adding $release_version notes and checksums for DB migrations

- Add the notes of the release line release $release_version to main such that
it is included in the documentation and download page.

_Pull request automatically created by [release/propose.sh](https://github.com/DACH-NY/canton/blob/$BASE/release/propose.sh)_.

EOM
    )
    create_pull_request "$branch" "" "$description" "main"
}
