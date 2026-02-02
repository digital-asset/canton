#!/usr/bin/env bash

ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

# shellcheck source=./scripts/ci/common.sh
source "$ABSDIR/../scripts/ci/common.sh"


update_VERSION_file() {
  local -r version=$1
  local -r VERSION_file=$2
  run "Update ${VERSION_file}" sed -i.old -E "1s/.*/${version}/"  "$ABSDIR/../${VERSION_file}"
  rm "$ABSDIR/../${VERSION_file}.old"
}

update_version_sbt_and_VERSION() {
  local -r version=$1

  # using sed in place isn't possible in a compatible way between gnu and bsd sed
  # instead use `-i.old` to move old version to another file and then just remove it
  run "Update version.sbt" sed -i.old -E "s/\"[0-9].+\"/\"$version\"/" "$ABSDIR/../version.sbt"
  rm "$ABSDIR/../version.sbt.old"
  update_VERSION_file "${version}" "VERSION"
  update_VERSION_file "${version}" "community/ledger-api/VERSION"
}

update_version() {
  update_version_sbt_and_VERSION "$1"

  # we use a sbt task to update the daml.yaml files to match the sbt project version
  # therefore `update_version_sbt_and_VERSION` MUST be run first
  info "Updating daml project versions to match sbt project (this uses sbt so can take a while)"
  run "Updating daml.yaml versions" sbt updateDamlProjectVersions
}

update_version_command() {
  local -r new_version="$1"

  if [[ -n "$new_version" ]]; then
    info "Updating canton to version [$new_version]"
    update_version "$new_version"
    info "Version successfully updated"
    echo "HINT: Run 'reload' in any active sbt sessions to pick up the new version"
    echo "Now smoke testing that the release works by running a Ping (this takes ~1-2min)"
    run "Running SimplestPingIntegrationTest" sbt pingTest
    echo "Ping completed successfully"
  else
    err "Usage: release/update-version.sh new-version"
    exit 1
  fi
}

# determine if this script is being sourced by another script
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  # we're being executed directly so run the update version command expecting the version to have been supplied
  update_version_command "$1"
fi
