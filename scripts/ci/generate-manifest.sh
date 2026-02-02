#!/bin/bash
set -eo pipefail

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

generate_manifest() {

  if [[ -z "${RELEASE_SUFFIX}" ]]; then
    # the suffix gets defined in a previous job. we just reuse it here.
    err "ERROR, NO RELEASE_SUFFIX DEFINED! You need to run this job after the obtain_release_suffix job."
    exit 1
  fi
  VERSION="${RELEASE_SUFFIX}"

  cat << EOF
apiVersion: digitalasset.com/v1
kind: Component
spec:
  jar-commands:
    - path: lib/canton-${1}-${VERSION}.jar
      name: sandbox
      desc: Run full Canton installation in a single process
      jar-args: [sandbox]
    - path: lib/canton-${1}-${VERSION}.jar
      name: canton-console
      desc: Run a canton console connecting to a remote participant and synchronizer (defaulting to sandbox)
      jar-args: [sandbox-console]
EOF
}

usage() {
  >&2 cat << EOF
Usage: ./scripts/ci/generate-manifest.sh component-line

  component-line: enterprise / open-source - type of the component-line to be produced.
EOF
}

case "${1}" in
  enterprise|open-source|private)
    generate_manifest ${1}
    ;;
  *)
    usage
    exit 1
    ;;
esac
