#!/usr/bin/env bash
set -eo pipefail

# Publishes the Canton API definitions (protobuf and JSON API specs) as a Maven artifact
# to Maven Central (via the Sonatype Central Portal).
#
# Usage: publish-api-to-maven-central.sh
#
# Required environment variables:
#   RELEASE_SUFFIX    - version string (e.g. "3.5.0")
#   MAVEN_USERNAME    - Sonatype Central Portal token username
#   MAVEN_PASSWORD    - Sonatype Central Portal token password
#   gpg_code_signing  - base64-encoded GPG private key for signing artifacts

ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

if [[ -z "$RELEASE_SUFFIX" ]]; then
  err "ERROR, NO RELEASE_SUFFIX DEFINED! You need to run this job after the obtain_release_suffix job."
  exit 1
fi

# Only publish stable releases to Maven Central
if [[ "$RELEASE_SUFFIX" == *"-SNAPSHOT" ]]; then
  info "Skip publishing of snapshot release to Maven Central"
  exit 0
fi

for var in MAVEN_USERNAME MAVEN_PASSWORD gpg_code_signing; do
  if [[ -z "${!var}" ]]; then
    err "ERROR, ${var} is not set."
    exit 1
  fi
done

current_version="$RELEASE_SUFFIX"
group_id="com.digitalasset.canton"
group_path="com/digitalasset/canton"
artifact_id="canton-api"

central_api_url="https://central.sonatype.com/api/v1/publisher"

release_dir="/tmp/workspace/community/app/target/release"

gpg_sign() {
  local file="$1"
  gpg --batch --yes --pinentry-mode loopback --armor --detach-sign "$file"
  # produces "${file}.asc"
}

generate_pom() {
  local pom_file="$1"
  cat > "${pom_file}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd"
         xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <modelVersion>4.0.0</modelVersion>
  <groupId>${group_id}</groupId>
  <artifactId>${artifact_id}</artifactId>
  <version>${current_version}</version>
  <packaging>zip</packaging>
  <name>Canton API</name>
  <description>Canton API definitions (protobuf and JSON API specs)</description>
  <url>https://github.com/digital-asset/canton</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>https://www.apache.org/licenses/LICENSE-2.0</url>
    </license>
  </licenses>
  <developers>
    <developer>
      <organization>Digital Asset</organization>
      <organizationUrl>https://www.digitalasset.com</organizationUrl>
    </developer>
  </developers>
  <scm>
    <connection>scm:git:git://github.com/digital-asset/canton.git</connection>
    <developerConnection>scm:git:ssh://github.com:digital-asset/canton.git</developerConnection>
    <url>https://github.com/digital-asset/canton</url>
  </scm>
</project>
EOF
}

central_curl() {
  curl -s -u "${MAVEN_USERNAME}:${MAVEN_PASSWORD}" "$@"
}

# Prepare a single artifact file with its checksums and GPG signature,
# placing the results into the bundle staging directory.
prepare_artifact() {
  local bundle_dir="$1"
  local source="$2"
  local extension="$3"

  local base_dir="${bundle_dir}/${group_path}/${artifact_id}/${current_version}"
  mkdir -p "${base_dir}"

  local filename="${artifact_id}-${current_version}.${extension}"
  cp "${source}" "${base_dir}/${filename}"

  gpg_sign "${base_dir}/${filename}"
  sha1sum "${base_dir}/${filename}" | awk '{print $1}' > "${base_dir}/${filename}.sha1"
  md5sum  "${base_dir}/${filename}" | awk '{print $1}' > "${base_dir}/${filename}.md5"
}

prepare_pom() {
  local bundle_dir="$1"

  local base_dir="${bundle_dir}/${group_path}/${artifact_id}/${current_version}"
  mkdir -p "${base_dir}"

  local filename="${artifact_id}-${current_version}.pom"
  generate_pom "${base_dir}/${filename}"

  gpg_sign "${base_dir}/${filename}"
  sha1sum "${base_dir}/${filename}" | awk '{print $1}' > "${base_dir}/${filename}.sha1"
  md5sum  "${base_dir}/${filename}" | awk '{print $1}' > "${base_dir}/${filename}.md5"
}

upload_bundle() {
  local bundle_zip="$1"

  info "Uploading deployment bundle to Central Portal"
  local response
  response=$(central_curl \
    -X POST \
    -F "bundle=@${bundle_zip}" \
    "${central_api_url}/upload")

  # The response body is the deployment ID (plain text)
  local deployment_id
  deployment_id=$(echo "$response" | tr -d '[:space:]')

  if [[ -z "$deployment_id" ]]; then
    err "Failed to upload bundle. Response: ${response}"
    exit 1
  fi

  echo "$deployment_id"
}

wait_for_deployment() {
  local deployment_id="$1"
  local max_attempts=60
  local attempt=0

  while (( attempt < max_attempts )); do
    local response
    response=$(central_curl "${central_api_url}/status?id=${deployment_id}")
    local state
    state=$(echo "$response" | grep -oP '"deploymentState"\s*:\s*"\K[^"]+' || true)

    if [[ "$state" == "PUBLISHED" ]]; then
      info "Deployment ${deployment_id} has been published successfully"
      return 0
    elif [[ "$state" == "FAILED" ]]; then
      err "Deployment ${deployment_id} failed. Response: ${response}"
      exit 1
    fi

    attempt=$((attempt + 1))
    info "Waiting for deployment to complete (attempt ${attempt}/${max_attempts}, current state: ${state})"
    sleep 10
  done

  err "Timed out waiting for deployment ${deployment_id} to be published"
  exit 1
}

info "Publishing API artifacts to Maven Central via Central Portal"

# 1. Prepare bundle directory with all artifacts
bundle_dir="$(mktemp -d)"
trap 'rm -rf "${bundle_dir}"' EXIT

prepare_pom "${bundle_dir}"
prepare_artifact "${bundle_dir}" "${release_dir}/canton-open-source-${current_version}-api.zip" "zip"
prepare_artifact "${bundle_dir}" "${release_dir}/canton-open-source-${current_version}-api.tar.gz" "tar.gz"

# 2. Create the bundle zip
bundle_zip="$(mktemp --suffix=.zip)"
(cd "${bundle_dir}" && zip -r "${bundle_zip}" .)

# 3. Upload the bundle
deployment_id=$(upload_bundle "${bundle_zip}")
info "Created deployment: ${deployment_id}"
rm -f "${bundle_zip}"

# 4. Wait for validation and publishing
wait_for_deployment "${deployment_id}"

info "Done publishing API artifacts to Maven Central"

