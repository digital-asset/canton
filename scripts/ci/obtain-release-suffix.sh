#!/usr/bin/env bash
set -eo pipefail

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

# shellcheck source=./../../release/versions.sh
source "$ABSDIR/../../release/versions.sh"

# release verification check has already been done
is_nightly_workflow="${IS_NIGHTLY_WORKFLOW:-$1}"
is_manual="${IS_MANUAL:-$2}"

suffix=""
release_tag="${CIRCLE_TAG:-}"

if [ -z "$release_tag" ] && [[ "${GITHUB_REF_TYPE:-}" == "tag" && -n "${GITHUB_REF_NAME:-}" ]]; then
  release_tag="${GITHUB_REF_NAME}"
fi

if [ -n "$release_tag" ]; then
  validate_version_tag "$release_tag"

  suffix="${release_tag:1}" # strips the initial v
else
  # determine suffix (either as date for nightly/manual builds or keep current version for main)
  if [[ "$is_nightly_workflow" == "true" || "$is_manual" == "true" ]]; then
      sha=$(git rev-parse HEAD)
      canton_version=$(extract_local_version)
      suffix=$(snapshot_version $sha $canton_version)
  else
      suffix=$(extract_local_version)
  fi
fi

echo "$suffix"

