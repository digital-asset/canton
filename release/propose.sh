#!/usr/bin/env bash

export ALLOW_DAML_SNAPSHOT=1

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

# shellcheck source=./../scripts/ci/common.sh
source "$ABSDIR/../scripts/ci/common.sh"

# shellcheck source=./update-version.sh
source "$ABSDIR/update-version.sh"

# shellcheck source=./pull-requests.sh
source "$ABSDIR/pull-requests.sh"

# shellcheck source=./pull-requests.sh
source "$ABSDIR/versions.sh"

# shellcheck source=./propose-common.sh
source "$ABSDIR/propose-common.sh"

# shellcheck source=./scripts/daml.sh
source "$ABSDIR/../scripts/daml.sh"

ensure_workspace_clean() {
  # allow short circuiting this test for dev locally on propose.sh
  if [[ -n "$SKIP_CLEAN_WORKSPACE_CHECK" ]]; then
    debug "SKIP_CLEAN_WORKSPACE_CHECK is set so skipping clean workspace check"
    return
  fi

  local -r status=$("$_GIT" status --porcelain --untracked)

  [[ -z "$status" ]] || {
    err "The release process requires a clean git workspace"
    err "Please ensure 'git status' shows no modified or untracked files"
    err "Alternatively, at your own risks, set the env var SKIP_CLEAN_WORKSPACE_CHECK"
    exit 1
  }
}

check_remote() {
  remote_url=$("$_GIT" config --get "remote.$REMOTE.url")

  # check were in the correct remote with the correct origin (both http and ssh configuration uris supported)
  if [[ -z "$CI" &&  ! ("$remote_url" == "$REPO_HTTP_URL" || "$remote_url" == "$REPO_SSH_URL") ]]; then
    err "expected remote repository origin to have url [$REPO_HTTP_URL] or [$REPO_SSH_URL]. Found [$remote_url]."
    err "please ensure you are releasing from the canton repository and 'git config --get remote.$REMOTE.url' is set correctly."
    exit 1
  fi
}

next_snapshot_version_from_release_version() {
  local -r release_version=$1
  # calculate new version
  local -r major_and_minor_version=$(cut -d'.' -f1-2 <<< "$release_version")
  local -ir patch_version=$(cut -d'.' -f 3 <<< "$release_version")
  local -ir next_patch_version=$(( patch_version + 1 ))
  echo "${major_and_minor_version}.${next_patch_version}-SNAPSHOT"
}

update_remote_ref() {
  run "Updating local reference" "$_GIT" fetch --quiet "$REMOTE" "$BASE"
}

get_release_branch_name() {
  echo "${RELEASE_BRANCH_PREFIX}release-$release_version"
}

checkout_release_branch() {
  local -r release_version=$1
  local -r branch=$(get_release_branch_name "$release_version")

  info "Pulling latest changes from $REMOTE/$BASE"
  run "Checking out release line" "$_GIT" checkout "$BASE"
  run "Pulling changes" "$_GIT" pull --ff-only

  info "Checking out $BASE to local branch [$branch]"
  run "Checking out release branch" "$_GIT" checkout -b "$branch"

  if [[ -n $("$_GIT" ls-remote --heads origin "$branch") ]]; then
    err "Release branch $branch already exists remotely"
    exit 1
  fi

  echo "$branch"
}

stage_version_updates() {
  run "Stage version.sbt update" "$_GIT" add "$REPO_ROOT/version.sbt"
  run "Stage VERSION update" "$_GIT" add "$REPO_ROOT/VERSION"
  run "Stage community/ledger-api/VERSION" "$_GIT" add "$REPO_ROOT/community/ledger-api/VERSION"

  # stage our daml.yaml changes
  run "Staging daml project updates" "$_GIT" add "**/daml.yaml"
}

update_release_notes() {
  local -r release_version=$1
  truncated_release_version=$(cut -d'.' -f1,2 <<< "$release_version")

  # By default a stable Daml version is used for the release
  if [[ -z "$ALLOW_DAML_SNAPSHOT" ]]; then
    daml_version=$(get_daml_stable_version)
  else
    daml_version=$(get_daml_version)
  fi

  ci_config=${REPO_ROOT}/.circleci/config.yml
  # find postgres versions used in CI
  postgres_test_default=$(sed -e '/postgres_docker_executor:/,/image: digitalasset\/query-stats-postgres:/!d' "${ci_config}" | grep "digitalasset/query-stats-postgres" | awk '{print $2}')
  postgres_test_default_version=$(docker run "${postgres_test_default}" postgres --version)

  readarray -t postgres_conformance_tests <<< "$(sed -e '/postgres_conformance_test_/,/postgres_version:/!d' "${ci_config}" | grep "postgres_version:" | sed -e 's/postgres_version: /postgres:/g' | sort)"
  postgres_conformance_test_versions=$(printf "%s\n" "${postgres_conformance_tests[@]}" | xargs -I{} docker run {} postgres --version | sort | uniq | paste -sd, | sed -e 's/,/, /g')

  postgres_version=$(printf 'Recommended: %s – Also tested: %s' "${postgres_test_default_version}" "${postgres_conformance_test_versions}" | sed -e 's/postgres (PostgreSQL)/PostgreSQL/g')

  # Supported protocol versions
  run "Exporting release to protocol versions JSON file" sbt "community-common/runMain com.digitalasset.canton.version.ReleaseVersionToProtocolVersionsExporter"

  embed_reference_json="embed_reference.json"
  protocol_versions=$(jq -r ".[] | select(.[0] | contains (\"$truncated_release_version\")) | .[1] | join(\", \")" "$embed_reference_json")

  # determine java version and daml version of our build
  java_version=$(java -version 2>&1 | sed "3q;d")

  today=$(LC_ALL=en_US.utf8 date +"%B %d, %Y")
  SEDARGS="s/CANTON_VERSION/$release_version/g"
  SEDARGS="${SEDARGS}; s/RELEASE_DATE/$today/g"
  SEDARGS="${SEDARGS}; s/POSTGRES_VERSION/$postgres_version/g"
  SEDARGS="${SEDARGS}; s/JAVA_VERSION/$java_version/g"
  SEDARGS="${SEDARGS}; s/DAML_VERSION/$daml_version/g"
  SEDARGS="${SEDARGS}; s/PROTOCOL_VERSIONS/$protocol_versions/g"
  # long live posix compliance!
  if [ "$(uname)" == "Darwin" ]; then
    sed -i'' "$SEDARGS" "$REPO_ROOT/UNRELEASED.md"
  else
    sed -i "$SEDARGS" "$REPO_ROOT/UNRELEASED.md"
  fi
  run "Append amended release notes" "$_GIT" add "$REPO_ROOT/UNRELEASED.md"
}

create_release_commit() {
  local -r release_version=$1

  update_version "$release_version"
  stage_version_updates

  # JSON API golden files
  run "Generate Ledger JSON Api documentation" sbt packageJsonApiDocsArtifacts
  git add "community/ledger/ledger-json-api/src/test/resources/json-api-docs/openapi.yaml"
  git add "community/ledger/ledger-json-api/src/test/resources/json-api-docs/asyncapi.yaml"

  run "Commit release commit" "$_GIT" commit -m "release: $release_version"
  local -r sha=$("$_GIT" log -1 --format=format:%H)
  info "Committed [$release_version] release with SHA [$sha]"
}

create_checksums_commit() {
  local -r release_version=$1
  run "Add SHA256 checksums for new Flyway migration scripts" "$REPO_ROOT/$DB_MIGRATION_PATH/recompute-sha256sums.sh" "$REPO_ROOT/$DB_MIGRATION_PATH"
  run "Stage SHA256 checksum updates" "$_GIT" add "$REPO_ROOT/$DB_MIGRATION_PATH/*.sha256"

  # if the commit message is changed, please adapt it also in `finish_release_process`
  run "Commit checksums" "$_GIT" commit --allow-empty -m "Updated checksums for: $release_version"
  local -r sha=$("$_GIT" log -1 --format=format:%H)
  info "Committed checksums for [$release_version] release with SHA [$sha]"
}

create_next_snapshot_commit() {
  local -r next_version=$1

  run "Create new UNRELEASED.md" cp "$REPO_ROOT/release/RELEASE-NOTES-TEMPLATE.md" "$REPO_ROOT/UNRELEASED.md"
  run "Stage new unreleased notes" "$_GIT" add "$REPO_ROOT/UNRELEASED.md"

  debug "Next version is [$next_version]"
  update_version "$next_version"
  stage_version_updates

  # JSON API golden files
  run "Generate Ledger JSON Api documentation" sbt packageJsonApiDocsArtifacts
  git add "community/ledger/ledger-json-api/src/test/resources/json-api-docs/openapi.yaml"
  git add "community/ledger/ledger-json-api/src/test/resources/json-api-docs/asyncapi.yaml"

  run "Commit next version" "$_GIT" commit -m "chore: prepare for work on $next_version"

  info "Committed bump to begin work on [$next_version]"
}

update_main_after_release_branch() {
  local -r release_version=$1
  local -r branch=update-main-after-release-line-$release_version
  local -r description="Update main after creation of release-line $release_version"

  info "Creating update branch $branch from main"
  run "Checking out remote main branch" "$_GIT" checkout -b "$branch" "$REMOTE/main"

  info "Resetting UNRELEASED.md"
  run "Create new UNRELEASED.md" cp "$REPO_ROOT/release/RELEASE-NOTES-TEMPLATE.md" "$REPO_ROOT/UNRELEASED.md"
  run "Stage new unreleased notes" "$_GIT" add "$REPO_ROOT/UNRELEASED.md"

  # Calculate new version
  local -r major_version=$(cut -d'.' -f1 <<< "$release_version")
  local -ir minor_version=$(cut -d'.' -f2 <<< "$release_version")
  local -ir next_minor_version=$(( minor_version + 1 ))
  local -r next_version="${major_version}.${next_minor_version}.0-SNAPSHOT"

  # Update version.sbt
  info "Updating version.sbt and VERSION to $next_version"
  update_version_sbt_and_VERSION "$next_version"
  run "Stage version.sbt" "$_GIT" add "$REPO_ROOT/version.sbt"
  run "Stage VERSION" "$_GIT" add "$REPO_ROOT/VERSION"
  run "Stage community/ledger-api/VERSION" "$_GIT" add "$REPO_ROOT/community/ledger-api/VERSION"

  # Add the new version to the list of ReleaseVersion
  info "Adding $next_version to the versions list"
  run "Add $next_version to the versions list" sbt --batch "fixReleaseVersions $next_version \"$RELEASE_SCRIPT_VERSION_FILE\" \"$RELEASE_SCRIPT_VERSION_MAPPING_FILE\""
  run "Stage $RELEASE_SCRIPT_VERSION_FILE" "$_GIT" add "$RELEASE_SCRIPT_VERSION_FILE"
  run "Stage $RELEASE_SCRIPT_VERSION_MAPPING_FILE" "$_GIT" add "$RELEASE_SCRIPT_VERSION_MAPPING_FILE"

  run "Commit updates after release-line is created" "$_GIT" commit -m "$description"

  info "Push update branch"
  push_to_remote "$branch"

  info "Create update PR"
  create_pull_request "$branch" "post-release" "$description" "main"
}

push_to_remote() {
  local -r branch=$1
  info "Pushing branch to [$REMOTE/$branch]"
  run "Pushing branch" "$_GIT" push -q --set-upstream "$REMOTE" "$branch"
}

# actions required by all commands to ensure we're locally in a good place
preflight() {
  ensure_workspace_clean
  check_remote
  update_remote_ref
}

# Creates the PR for the release with the following commits:
#  - release commit
#  - DB checksums
#  - next snapshot version
create_release_pr() {
  preflight
  local -r current_canton_version=$(extract_version)
  local -r release_version=$(next_release_version "$current_canton_version") || exit 1
  local -r release_branch=$(get_release_branch_name "$release_version")
  local -r next_version=$(next_snapshot_version_from_release_version "$current_canton_version")

  # By default a stable Daml version is used for the release
  if [[ -z "$ALLOW_DAML_SNAPSHOT" ]]; then
    current_daml_version=$(get_daml_stable_version)
  else
    current_daml_version=$(get_daml_version)
  fi

  info "Daml version $current_daml_version is used for this release"
  info "Preparing release for version [$release_version]"
  info "Next version will be $next_version"

  run "git checkout -B $release_branch" "$_GIT" checkout -B "$release_branch"

  # Check that the release version was added to the list of ReleaseVersions
  fix_release_versions "$release_version"

  # Release notes
  update_release_notes "$release_version"
  run "Create release notes from UNRELEASED.md" "$_GIT" mv "$REPO_ROOT/UNRELEASED.md" "$REPO_ROOT/release-notes/$release_version.md"
  run "Commit release notes" "$_GIT" commit --allow-empty -m "Updated release notes for: $release_version"
  local -r sha=$("$_GIT" log -1 --format=format:%H)
  info "Committed release notes for [$release_version] release with SHA [$sha]"

  create_checksums_commit "$release_version"
  create_release_commit "$release_version"

  echo "Running a simple ping as a smoke test to test release"
  run "Running simple ping as a smoke test" sbt pingTest
  echo "Ping was successful"

  create_next_snapshot_commit "$next_version"

  push_to_remote "$release_branch"

  if [[ ! -z "$CI" ]]; then
   create_release_pull_request "$release_version" "$next_version" "$release_branch"
  fi
}

# Syncs DB migrations from the release tag to the currently checked-out branch.
#  - checks out .sql and .sha256 files from the tag located in postgres/stable ONLY
#  - commits new or modified files
sync_db_migrations_from_tag() {
  local -r release_version=$1
  local -r tag="v${release_version}"
  # Do NOT prepend REPO_ROOT here. Keep path relative for `git checkout`
  local -r sync_path="$DB_MIGRATION_PATH/postgres/stable"

  # Check if the tag exists locally; fully qualified 'refs/tags/$tag' to avoid ambiguity
  if ! "$_GIT" rev-parse --verify --quiet "refs/tags/$tag" >/dev/null; then
     info "Tag [$tag] not found locally. Attempting to fetch from remote..."

     # Allow fetch to fail (|| true) so local testing works without remote tags
     run "Fetching tag $tag" "$_GIT" fetch "$REMOTE" "refs/tags/$tag:refs/tags/$tag" || true

     # Verify again - abort if the tag is still missing
     if ! "$_GIT" rev-parse --verify --quiet "refs/tags/$tag" >/dev/null; then
        err "Tag [$tag] could not be found locally or fetched from remote."
        exit 1
     fi
  else
     debug "Tag [$tag] found locally."
  fi

  info "Checking for SQL migrations and Checksums in $sync_path from tag $tag to sync to main..."

  # Checkout only .sql and .sha256 files in the postgres/stable directory from the release tag
  run "Checkout DB files from tag" "$_GIT" checkout "$tag" -- "$sync_path/*.sql" "$sync_path/*.sha256"

  # Check if anything changed in that specific directory
  if [[ -n $("$_GIT" status --porcelain "$sync_path/") ]]; then
      info "New or modified SQL migrations/checksums found in postgres/stable. Committing..."
      run "Stage DB files" "$_GIT" add "$sync_path/*.sql" "$sync_path/*.sha256"
      run "Commit DB files" "$_GIT" commit -m "release-line-db: synced SQL migrations and checksums from $release_version (postgres/stable only)"
  else
      info "No new SQL migrations or checksums found to sync."
  fi
}

# Port relevant changes to main
#  - release notes
#  - DB migrations (SQL + checksums from the release tag)
finish_release_process() {
  if [[ -z "$CIRCLE_TAG" ]]; then
    err "Unable to read tag from env var CIRCLE_TAG"
    exit 1
  fi

  local -r release_version=$(echo "$CIRCLE_TAG" | sed -E "s/^v//")
  info "Release version is ${release_version}"

  # Update main
  update_branch="update-main-for-${release_version}"
  run "Creating update branch for main" "$_GIT" branch "$update_branch" "$REMOTE/main"
  # By side effect this checks out the update branch - so we're already on it when we cherry pick checksums
  copy_release_notes_to_main_update_branch "$release_version" "$update_branch"

  # Sync both SQL and checksums directly from the tag
  sync_db_migrations_from_tag "$release_version"

  push_to_remote "$update_branch"

  # Create final update PR targeting main
  if [[ ! -z "$CI" ]]; then
    create_update_pr_on_main "$release_version" "$update_branch"
  fi
}

snapshot_release() {
  local -r suffix=$1
  if [ -z "$suffix" ];then
    echo "A snapshot release requires a suffix!"
  fi
  preflight
  current_version=$(extract_version)
  release_version=$(next_release_version "$current_version") || exit 1
  release_version="${release_version}-${suffix}"
  daml_version=$(get_daml_version)

  info "Release version is ${release_version} and daml version is ${daml_version}."

  release_branch=$(checkout_release_branch "$release_version") || exit 1

  # Check that the version was added to the list of ReleaseVersion
  run "Fix release versions" sbt --batch "fixReleaseVersions $release_version \"$RELEASE_SCRIPT_VERSION_FILE\" \"$RELEASE_SCRIPT_VERSION_MAPPING_FILE\""

  git add "${RELEASE_SCRIPT_VERSION_FILE}"
  git add "${RELEASE_SCRIPT_VERSION_MAPPING_FILE}"
  # Only commit if something was added
  "$_GIT" diff-index --quiet HEAD || "$_GIT" commit -m 'Updated release version files'

  # create a new commit with the new version
  # don't create release notes
  create_release_commit "$release_version"
  create_checksums_commit "$release_version"

  echo "Running a simple ping as a smoke test"
  run "Running simple ping as a smoke test" sbt pingTest
  echo "Ping was successful"

  # but revert back to the old version
  update_version "$current_version"
  stage_version_updates
  run "Commit next version" "$_GIT" commit -m "chore: continue work on $current_version"

  push_to_remote "$release_branch"

  create_snapshot_pull_request "$release_version"  "$release_branch"
}

copy_release_notes_to_main_update_branch() {
  local -r release_version=$1
  local -r update_branch=$2
  echo "$release_version"
  notes="${REPO_ROOT}/release-notes/${release_version}.md"
  info "Copying release notes $notes to main via branch $update_branch"
  # copy current release notes into a separate file
  cp $notes tmp.md
  run "Checking out the update branch" "$_GIT" checkout "$update_branch"
  # Move back release notes to their original file
  mv tmp.md $notes
  "$_GIT" add $notes
  "$_GIT" commit -m "release-line-notes: added $release_version notes to main"
}

create_release_line() {
  preflight
  local -r release_line=$1

  # check that the release line version is major.minor and extract the numbers
  if [[ ! $release_line =~ ^[0-9]+\.[0-9]+$ ]]; then
    err "Can not match release-line ${release_line} to major.minor"
    exit 1
  fi
  local -r major=$(echo "$release_line" | sed -E 's/^(.*)\..*$/\1/')
  local -r minor=$(echo "$release_line" | sed -E 's/^.*\.(.*)$/\1/')

  # check if the release line already exists
  local -r branch="${RELEASE_LINE_BRANCH_PREFIX}-${release_line}"
  if [[ -n $("$_GIT" branch --list $branch) ]]; then
    err "Release branch $branch already exists"
    exit 1
  fi

  # check codeowners of proto snapshots
  if grep -q -E -e "^/\.proto_snapshot_image\.bin\.gz\s+@DACH-NY/canton-change-owners" CODEOWNERS ; then
    info "Codeowners for proto snapshots configured correctly"
  else
    err "Codeowners not configured for proto snapshots. Please add the following line to CODEOWNERS:"
    err "/.proto_snapshot_image.bin.gz    @DACH-NY/canton-change-owners"
    exit 2
  fi

  info "Checks passed. Creating release-line branch for $release_line: $branch"

  info "Creating release-branch $branch on head"
  run "Creating release-branch $branch based on head" "$_GIT" checkout -b $branch

  # push release-line branch to github
  run "Pushing release-branch $branch to main" "$_GIT" push --set-upstream origin $branch

  # open a pull request to update main
  update_main_after_release_branch "$major.$minor"
}

usage() {
  >&2 cat << EOF
Usage: ./release/propose.sh release

  create_release_pr: create a PR for a stable release
  release-line <major.minor>: create a new release line branch (needs to be a lower version than current main)
  snapshot <suffix>: create a pre-release / snapshot release
EOF
}

case "$1" in
  create_release_pr)
    create_release_pr
    ;;
  finish_release_process)
    finish_release_process
    ;;
  release-line)
    create_release_line $2
    ;;
  snapshot)
    snapshot_release $2
    ;;
  help|--help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
