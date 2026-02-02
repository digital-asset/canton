#!/usr/bin/env bash

DEBUG=1
_GIT=git
_SBT=sbt
_HUB=hub
_CURL=curl

fetch_daml_version() {
  local -r root=$1

  # lookup up the daml version from the sbt build configuration
  local version

  if ! version=$(grep 'val version' "${root}/project/project/DamlVersions.scala" | cut -d\" -f2) ||
    [[ -z "$version" ]]; then
    err "Failed to lookup daml version from ${root}/project/project/DamlVersions.scala"
    exit 1
  fi

  echo "$version"
}

fetch_dpm_registry() {
  local -r root=$1

  # lookup up the DPM registry from the sbt build configuration
  local dpm_registry

  if ! dpm_registry=$(grep 'val dpm_registry' "${root}/project/project/DamlVersions.scala" | cut -d\" -f2) ||
    [[ -z "$dpm_registry" ]]; then
    err "Failed to lookup DPM registry from ${root}/project/project/DamlVersions.scala"
    exit 1
  fi

  echo "$dpm_registry"
}

# Determine newer versions by day
day_from_daml_snapshot_version() {
  DAML_VERSION=$1
  echo $DAML_VERSION | grep -e "-snapshot\." | sed -e "s/.*-snapshot\.\([0-9]*\).*/\1/"
}

fetch_newest_daml_version() {
  # Extract major-minor version prefix, e.g. 3.5, from version.sbt
  version_prefix=$(grep -oE '[0-9]+\.[0-9]+\.[0-9]+' version.sbt | cut -d'.' -f1,2)

  # Use dpm to resolve latest version with that prefix
  NEWEST_DAML_VERSION=$(dpm repo resolve-tags damlc:$version_prefix --registry $DPM_REGISTRY)
  echo $NEWEST_DAML_VERSION
}

daml_version_to_upgrade_to() {
  NEWEST_DAML_VERSION=$1
  CANTON_DAML_VERSION=$2

  comparison_result=$(amm scripts/daml/compareDamlVersions.sc compare "$NEWEST_DAML_VERSION" "$CANTON_DAML_VERSION")

  if [[ "$comparison_result" -ge 1 ]]; then
    echo "Newer Daml version exists: $NEWEST_DAML_VERSION is newer than $CANTON_DAML_VERSION"
  else
    echo "Canton Daml repo version \"$CANTON_DAML_VERSION\" is not older than newest Daml version \"$NEWEST_DAML_VERSION\"" 2>&1
    echo "Already on the latest Daml version"
    exit 0
  fi
}

update_daml_project_versions() {
  CANTON_DAML_VERSION=$1
  NEWEST_DAML_VERSION=$2

  info "Current Daml version: \"$CANTON_DAML_VERSION\""
  info "Target Daml version: \"$NEWEST_DAML_VERSION\""

  daml_version_to_upgrade_to "$NEWEST_DAML_VERSION" "$CANTON_DAML_VERSION"
  DAML_VERSION_TO_UPGRADE_TO=$NEWEST_DAML_VERSION

  echo "Attempting upgrading to Daml $NEWEST_DAML_VERSION"
  echo "Updating DamlVersions.version"
  sed -i project/project/DamlVersions.scala -e "s/\"${CANTON_DAML_VERSION}\"/\"${DAML_VERSION_TO_UPGRADE_TO}\"/"

  echo "Updating daml project versions"
  $_SBT updateDamlProjectVersions
}

# Only takes x.y.z into account for comparison (not snapshot suffix)
check_canton_version_is_not_smaller_than_daml_version() {
  DAML_VERSION=$1

  # assumes version.sbt is in the form `version in ThisBuild :="CURRENT_VERSION-SNAPSHOT"`
  version_file=$(cat version.sbt)

  canton_version=$(cut -d\" -f4 <<< "$version_file")
  if [[ -z "$canton_version" ]]; then
    err "Failed to extract version from 'version.sbt' content: [$version_file]"
    exit 1
  fi

  # We only care about major.minor.patch for the comparison
  semver_canton_version=$(echo $canton_version | grep -Eo "^([0-9]+)\.([0-9]+)\.([0-9]+)")
  semver_daml_version=$(echo $DAML_VERSION | grep -Eo "^([0-9]+)\.([0-9]+)\.([0-9]+)")

  # Use sort -V to sort the 2 versions, but add a dummy underscore at the end if there's no "-" to force snapshot versions
  # to be sorted lower than release versions https://stackoverflow.com/a/40391207
  smallest_version=$(printf "%s\n%s" "$semver_daml_version" "semver_daml_version" | sort -V | head -n1)

  if [ "$smallest_version" = "$semver_daml_version" ]; then
    echo "All good. Canton version $canton_version >= $DAML_VERSION"
  else
    echo "The canton version ($canton_version) appears to be lower than the daml version ($DAML_VERSION). Did we forget to bump version.sbt?"
    exit 1;
  fi
}

# Writes the URL of the pull request to stdout
#
# All other progress and diagnostic output must go to stderr
create_daml_upgrade_pull_request() {
  DAML_VERSION_TO_UPGRADE_TO=$1
  echo "Pushing changes of daml version and in .circleci/config.yml to origin" >&2
  $_GIT --no-pager diff >&2
  $_GIT add -v . >&2

  # Only commit if something has changed
  if $_GIT diff-index --quiet HEAD; then
    echo "No difference applied so not bothering with commit/push/pr" >&2
    exit 0
  fi
  $_GIT commit -m "Upstream upgrade to Daml $DAML_VERSION_TO_UPGRADE_TO" >&2

  MAIN_BRANCH="main"
  MAX_NUMBER_DAML_CHANGES=200
  DAML_REPO="https://github.com/digital-asset/daml"
  PR_DESCRIPTION_FILE="/tmp/pr_description.txt"
  echo -e "Upstream upgrade to Daml ${DAML_VERSION_TO_UPGRADE_TO}\n" | tee $PR_DESCRIPTION_FILE >&2

  # Embed daml-repo change list in PR:
  echo -e "[${MAIN_BRANCH}] Daml changes (listing up to $MAX_NUMBER_DAML_CHANGES):\n" | tee -a $PR_DESCRIPTION_FILE >&2
  rm -rf /tmp/daml
  $_GIT clone --depth="$MAX_NUMBER_DAML_CHANGES" --no-single-branch "${DAML_REPO}.git" /tmp/daml
  CANTON_DIR=$PWD
  cd /tmp/daml

  commit_from_daml_version() {
    local VERSION=$1
    COMMIT_WITH_PREFIX=$(echo $VERSION | sed -e "s|.*\.||")
    STRIPPED_VERSION=${COMMIT_WITH_PREFIX:1}

    echo $STRIPPED_VERSION
  }

  FIRST_COMMIT_EXCLUSIVE=$(commit_from_daml_version "$CANTON_DAML_VERSION")
  LAST_COMMIT_INCLUSIVE=$(commit_from_daml_version "$DAML_VERSION_TO_UPGRADE_TO")
  # TODO(i27546) Remove adhoc handling once adhoc versions are no longer needed / used
  if ! git merge-base --is-ancestor "$LAST_COMMIT_INCLUSIVE" main && [[ "$DAML_VERSION_TO_UPGRADE_TO" == *adhoc* ]]; then
    echo "Daml version  $CANTON_DAML_VERSION commit $LAST_COMMIT_INCLUSIVE is not in main. Exiting." >&2
    exit 1
  fi

  cd $CANTON_DIR

  echo "Pushing to origin" >&2
  $_GIT push origin ${BRANCH_NAME}

  cd /tmp/daml
  echo "Producing daml change log from $FIRST_COMMIT_EXCLUSIVE (exclusive) to $LAST_COMMIT_INCLUSIVE (inclusive)" >&2

  # Produce git log with commit number, author, and title
  # 1st sed: adds link to daml commit to first, lowercase hexadecimal hash, producing [text](url) format
  # 2nd sed: adds link to daml PR to suffix (#12345)
  $_GIT --no-pager log --pretty=format:"%h %an: %s" "^${FIRST_COMMIT_EXCLUSIVE}" "$LAST_COMMIT_INCLUSIVE" |
    sed -e "s|^\([a-f0-9]*\) |[\`\1\`](${DAML_REPO}/commit/\1) |" \
      -e "s|(#\([0-9]*\))|([#\1](${DAML_REPO}/pull/\1))|g" | tee -a $PR_DESCRIPTION_FILE >&2

  cd $CANTON_DIR

  echo "Preparing pull request (Standard-Change as .circleci config is being updated)" >&2
  URL=$("$_HUB" pull-request --base "main" --head "$BRANCH_NAME" --file "$PR_DESCRIPTION_FILE")
  echo -n "Done sending pull request " >&2
  echo $URL
}

release_or_snapshot_daml_git_ref_from_canton () {
  daml_version="$(fetch_daml_version .)"
  if [[ "$daml_version" == *"snapshot"* ]]; then
    echo "$daml_version" | grep -o -E '[a-f0-9]+$'
  else
    echo "v$daml_version"
  fi
}
