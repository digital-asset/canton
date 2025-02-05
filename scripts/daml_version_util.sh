#!/usr/bin/env bash

get_daml_version() {
  daml_version_snapshot_regex='val version[^=]+ = "([[:alnum:]\.\-]+)"'

  result_snapshot=$(grep -Eo "${daml_version_snapshot_regex}" project/project/DamlVersions.scala)

  if [[ -z "$result_snapshot" ]]; then
    err "Unable to extract Daml version from DamlVersions.scala"
    exit 1
  fi

  echo "$result_snapshot" | sed -E "s/${daml_version_snapshot_regex}/\1/"
}

get_daml_commit() {
  echo $(get_daml_version) | sed -e 's/.*\.//' -e 's/^.//'
}
