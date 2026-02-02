#!/usr/bin/env bash
set -euo pipefail

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"
source "$ABSDIR/../daml.sh"

# Variables with default values
oci_snapshot_path="${OCI_REGISTRY}/da-images/public-unstable"
oci_release_path="${OCI_REGISTRY}/da-images/public"
DPM_REGISTRY_AUTH="${DPM_REGISTRY_AUTH:-~/.docker/config.json}"
nightly_release="${IS_NIGHTLY_RELEASE:-$1}" # CircleCI parameter nightly_release
current_version="${RELEASE_SUFFIX}"
workspace="/tmp/workspace"
workspace_oci="/tmp/workspace_oci"
archive_type="zip"

if [[ -z "$RELEASE_SUFFIX" ]]; then
  # the suffix gets defined in a previous job. we just reuse it here.
  err "ERROR, NO RELEASE_SUFFIX DEFINED! You need to run this job after the obtain_release_suffix job."
  exit 1
fi

# We don't publish "SNAPSHOT" releases, only properly dated snapshots or full releases.
if [[ "$current_version" == *"-SNAPSHOT" || ( "$nightly_release" == "true" && $(date +"%u") -ne 2 ) ]]; then
  echo "Skip publishing of unnamed snapshot release or a nightly release not on Tuesdays"
  exit 0
fi

if [[ "$current_version" == *"snapshot"* || "$current_version" == *"ad-hoc"* ]]; then
  oci_path="${oci_snapshot_path}"
  echo "Publishing named snapshot release to ${oci_path}"
else
  oci_path="${oci_release_path}"
  echo "Publishing full release to ${oci_path}"
fi

get_major_minor() {
  version="$1"
  echo "$version" | awk -F. '{print $1 "." $2}'
}


on_exit() {
  exit_code=$?
  info_exit "Cleanup..."
  rm -rf "${workspace_oci}"
  # Showing the cursor.
  printf '\e[?25h'
  exit $exit_code
}

trap on_exit EXIT
# Array of type published artifacts
declare -a component_line=( "open-source" )
# Associative array with artifacts location by publish type
declare -A artifact_location
artifact_location[open-source]="community/app/target/release/canton-open-source-${current_version}"
# Associative array with artifacts path from location by publish type
declare -A artifact_path
artifact_path[open-source]="lib/canton-open-source-${current_version}.jar"

_print_header "Publish ${c_yellow}canton${c_reset} to OCI registry"
# Hiding the cursor.
printf '\e[?25l'
if [[ "x$CI" != "x" ]]; then
# Authorize in GCP
info_step "Login ${c_lyellow}GCP ${c_lwhite}${OCI_REGISTRY}${c_reset}"
# Hide login output, print only login failed
if ! output=$(nix-shell -I nixpkgs=./nix/nixpkgs.nix shell.nix --run ".circleci/gcp-login.sh \"${OCI_REGISTRY}\"" 2>&1)
 then
    err "login failed: $output"
    exit 1
fi
info_done "Login ${c_lyellow}GCP ${c_lgreen}successful${c_reset}."
fi


# Subroutine for proccessing by component line
for i in ${component_line[@]}; do
  info_step "Prepare ${c_white}canton ${c_lgreen}${i}${c_reset} for upload..."
  run "Create the same directory structure" mkdir -vp "${workspace_oci}/${artifact_location[${i}]}/$(dirname ${artifact_path[${i}]})"
  # Extract only one file by artifact_path
  run "Extract artifact_path" unzip -o -j "${workspace}/${artifact_location[${i}]}.${archive_type}" \
    "$(basename ${artifact_location[${i}]})/${artifact_path[${i}]}" \
    -d "${workspace_oci}/${artifact_location[${i}]}/$(dirname ${artifact_path[${i}]})"
  # Get linked daml path
  linked_daml_version=$(fetch_daml_version $ABSDIR/../..)
  # Go to extracted path
  cd "${workspace_oci}/${artifact_location[${i}]}" && \
  # Check component exists on place
  if [ -f "${artifact_path[${i}]}" ]; then
      info "Generate ${i} component manifest"
      ${ABSDIR}/generate-manifest.sh ${i} | tee "${workspace_oci}/${artifact_location[${i}]}/component.yaml"; echo "";
      echo "$linked_daml_version" > "${workspace_oci}/${artifact_location[${i}]}/linked-daml-version"
      cat ${ABSDIR}/../../LICENSE.txt > "${workspace_oci}/${artifact_location[${i}]}/LICENSE" 
      info "Uploading..."
      dpm repo publish-component "canton-${i}" "${current_version}" --extra-tags "$(get_major_minor ${current_version})" --platform generic=. --registry "${oci_path}"
      info_done "Component ${c_white}canton ${c_lgreen}${i}${c_reset} published.\n"
      unset DPM_EDITION
    else
      err "${artifact_location[${i}]}/${artifact_path[${i}]} - not found! Skip upload to oci"
  fi
done

exit 0
