#!/usr/bin/env bash
set -eo pipefail

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"

# shellcheck source=./../../release/versions.sh
source "$ABSDIR/../../release/versions.sh"

# release verification check has already been done
is_nightly_workflow="${IS_NIGHTLY_WORKFLOW:-$1}"
is_manual="${IS_MANUAL:-$2}"
ref="${CIRCLE_TAG:-$CIRCLE_BRANCH}"
suffix=""

# determine the filename based on whether it's a formal release or just a main build
case "$ref" in
v*)
    # strip the v prefix
    suffix="${ref:1}"
    ;;
*)
    # determine suffix (either as date for nightly/manual builds or keep current version for main)
    if [[ "$is_nightly_workflow" == "true" || "$is_manual" == "true" ]]; then
        sha=$(git rev-parse HEAD)
        canton_version=$(extract_local_version)
        suffix=$(snapshot_version $sha $canton_version)
    else
        suffix=$(extract_local_version)
    fi
    ;;
esac

echo $suffix

