#!/usr/bin/env bash
set -eo pipefail
get_daml_version() {
  # lookup up the sdk version from the scala dependencies and cut out the version from parenthesis
  local version root_path
  root_path="$(git rev-parse --show-toplevel)"
  if ! version=$(grep 'val version' "${root_path}/project/project/DamlVersions.scala" | \
                 awk -F"=" '{print $2}' | sed 's/"//g') || \
    [[ -z "$version" ]]; then
      >&2 err "Failed to lookup sdk version from DamlVersions.scala"
      exit 1
  fi
  echo "$version"
}

get_daml_language_versions() {
  # lookup up the daml language versions from the scala dependencies
  local versions root_path
  root_path="$(git rev-parse --show-toplevel)"
  if ! versions=$(grep 'val daml_language_versions' "${root_path}/project/project/DamlVersions.scala" | \
                  awk -F"=" '{print $2}'| sed -e "s/.*(//" -e "s/).*//" -e "s/[\",]//g" ) || \
    [[ -z "$versions" ]]; then
      >&2 err "Failed to lookup daml language versions from DamlVersions.scala"
      exit 1
  fi
  echo "$versions"
}

get_daml_commit() {
  echo $(get_daml_version) | sed -e 's/.*\.//' -e 's/^.//'
}
