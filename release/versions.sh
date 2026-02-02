#!/usr/bin/env bash

# Helpers to read and update versions

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

source "$ABSDIR/../scripts/ci/common.sh" # debug, info, err
source "$ABSDIR/propose-common.sh"
source "$ABSDIR/../scripts/daml_version_util.sh"

bump_daml_version() {
  local -r current_version=$1
  local -r target_version=$2

  if [[ "$current_version" == "$target_version" ]]; then
    info "Current Daml version is target version; nothing to do"
  else
    # update daml project
    update_daml_project_versions $current_version $target_version

    # commit
    $_GIT --no-pager diff
    $_GIT add -v "$ABSDIR/.."
    $_GIT commit -m "Update Daml to $target_version"
    push_to_remote "$release_branch"
  fi
}

extract_version() {
  # assumes version.sbt is in the form `version in ThisBuild :="CURRENT_VERSION-SNAPSHOT"`
  version_file=$("$_GIT" show "$REMOTE/$BASE":version.sbt)
  debug "version.sbt content: [$version_file]"

  version=$(cut -d\" -f4 <<< "$version_file")

  if [[ -z "$version" ]]; then
    err "Failed to extract version from 'version.sbt' content: [$version_file]"
    exit 1
  fi

  echo "$version"
}

extract_local_version() {
  version_file=$(cat version.sbt)
  echo $(cut -d\" -f4 <<< "$version_file")
}

next_release_version() {
  local -r snapshot_version=$1

  # check we're running against a snapshot
  if [[ "$snapshot_version" != *"-SNAPSHOT" ]]; then
    err "Current version of $REMOTE/$BASE is not a snapshot [$snapshot_version]"
    exit 1
  fi

  # remove the snapshot modifier
  echo "${snapshot_version/-SNAPSHOT/}"
}

# Unlike get_daml_version above, fails if the version is not a stable version
get_daml_stable_version() {
  daml_version_stable_regex='val version[^=]+ = "([0-9\.]+)"'
  daml_version_snapshot_regex='val version[^=]+ = "([[:alnum:]\.\-]+)"'

  result_stable=$(grep -Eo "${daml_version_stable_regex}" project/project/DamlVersions.scala)
  result_snapshot=$(grep -Eo "${daml_version_snapshot_regex}" project/project/DamlVersions.scala)

  if [[ -z "$result_stable" && -z "$result_snapshot" ]]; then
    err "Unable to extract Daml version from DamlVersions.scala"
    exit 1
  elif [[ -z "$result_stable" ]]; then
    res=$(echo "$result_snapshot" | sed -E "s/${daml_version_snapshot_regex}/\1/")
    err "Releases should be done using stable Daml versions. Found: $res".
    info "Snapshot version can be allowed by setting ALLOW_DAML_SNAPSHOT"
    exit 1
  fi

  echo "$result_stable" | sed -E "s/${daml_version_stable_regex}/\1/"
}

fix_release_versions() {
  version=$1
  run "Fix release versions" sbt --batch "fixReleaseVersions $version \"$RELEASE_SCRIPT_VERSION_FILE\" \"$RELEASE_SCRIPT_VERSION_MAPPING_FILE\""

  git add "${RELEASE_SCRIPT_VERSION_FILE}"
  git add "${RELEASE_SCRIPT_VERSION_MAPPING_FILE}"
  # Only commit if something was added
  "$_GIT" diff-index --quiet HEAD || "$_GIT" commit -m "Updated release version files: $version"
}

get_latest_release_version() {
  # extract the latest release from release-notes in case the latest dev version has already moved on
  latest_version=$(find "$REPO_ROOT/release-notes" -name '*.md' -exec basename {} .md \; | sort --version-sort --reverse | head -1)

  if [[ -z "$latest_version" ]]; then
    err "Failed to extract latest version"
    exit 1
  else
    debug "Found latest version of [$latest_version]"
  fi

  echo "$latest_version"
}

# Gets the snapshot version
# Follows the Daml naming scheme (2.7.0-snapshot.major.20230522.10367.0.v55e81000)
# Parameters
#   $1: sha of the commit
#   $2: prefix (Canton version in format x.y.z or x.y.z-snapshot.major)

snapshot_version() (
  sha=$1
  prefix=$2
  commit_date=$(git log -n1 --format=%cd --date=format:%Y%m%d $sha)
  number_of_commits=$(git rev-list --count $sha)
  commit_sha_8=$(git log -n1 --format=%h --abbrev=8 $sha)

  # ensure prefix contains 'snapshot'
  grepped_snapshot=$(echo $prefix | grep -i "SNAPSHOT")
  if [[ -z "$grepped_snapshot" ]]; then
    err "The raw version should contain SNAPSHOT"
    exit 1
  fi

  branches_main=$(git branch -a --contains "$sha" | grep "* main")
  branches_release_line=$(git branch -a --contains "$sha" | grep -E "^\* release-line-[0-9]+\.[0-9]+")

  if [[ -z "$branches_main" && -z "$branches_release_line" ]]; then # if commit is not in main nor in any release branch
    updated_prefix=$(echo "$prefix" | tr '[:upper:]' '[:lower:]' | sed "s/snapshot/ad-hoc/g")
  else
    updated_prefix=$(echo "$prefix" | tr '[:upper:]' '[:lower:]')
  fi

  echo "$updated_prefix.$commit_date.$number_of_commits.0.v$commit_sha_8"
)

extract_major_minor() {
    version="$1"
    echo "$version" | sed -E 's/^([0-9]+)\.([0-9]+).*/\1.\2/'
}
