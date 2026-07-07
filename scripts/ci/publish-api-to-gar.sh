#!/usr/bin/env bash
set -eo pipefail

# Publishes the Canton API definitions (protobuf and JSON API specs) as a Maven artifact
# to Google Artifact Registry.
#
# Usage: publish-api-to-gar.sh
#
# Required environment variables:
#   RELEASE_SUFFIX       - version string (e.g. "3.5.0-SNAPSHOT" or "3.5.0")
#   GOOGLE_CREDENTIALS_FILE - path to GCP service account key file
#   GAR_MAVEN_REPO_URL   - GAR Maven repository URL
#                          e.g. "https://europe-maven.pkg.dev/da-images/public-maven-unstable"
#                          or   "https://europe-maven.pkg.dev/da-images/public-maven"

ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

if [[ -z "$RELEASE_SUFFIX" ]]; then
  err "ERROR, NO RELEASE_SUFFIX DEFINED! You need to run this job after the obtain_release_suffix job."
  exit 1
fi

# We don't publish "SNAPSHOT" releases, only properly dated snapshots or full releases.
if [[ "$RELEASE_SUFFIX" == *"-SNAPSHOT" ]]; then
  info "Skip publishing of snapshot release"
  exit 0
fi

if [[ -z "$GOOGLE_CREDENTIALS_FILE" ]]; then
  err "ERROR, GOOGLE_CREDENTIALS_FILE is not set."
  exit 1
fi

if [[ -z "$GAR_MAVEN_REPO_URL" ]]; then
  err "ERROR, GAR_MAVEN_REPO_URL is not set."
  exit 1
fi

current_version="$RELEASE_SUFFIX"
group_id="com.digitalasset.canton"
group_path="com/digitalasset/canton"
artifact_id="canton-api"

# Authenticate with GCP and get an access token
ACCESS_TOKEN="$(gcloud auth activate-service-account --key-file="$GOOGLE_CREDENTIALS_FILE" 2>/dev/null && gcloud auth print-access-token)"

put_to_gar() {
  local source="$1"
  local remote_url="$2"

  info "Publishing ${source} to ${remote_url}"

  if [ ! -e "$source" ]; then
    err "ERROR, file does not exist: ${source}."
    exit 1
  fi

  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X PUT \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -T "${source}" \
    "${remote_url}")

  if [[ "$http_code" -ge 200 && "$http_code" -lt 300 ]]; then
    info "Successfully published $(basename "${source}") (HTTP ${http_code})"
  elif [[ "$http_code" == "409" ]]; then
    warn "Artifact already exists (HTTP 409), skipping: $(basename "${source}")"
  else
    err "Failed to publish $(basename "${source}") (HTTP ${http_code})"
    exit 1
  fi
}

# Upload an artifact file together with its SHA1 and MD5 checksums
publish_to_gar() {
  local source="$1"
  local extension="$2"
  local base_url="${GAR_MAVEN_REPO_URL}/${group_path}/${artifact_id}/${current_version}"
  local remote_url="${base_url}/${artifact_id}-${current_version}.${extension}"

  put_to_gar "${source}" "${remote_url}"

  # Upload checksums so Maven clients can verify downloads
  sha1sum "${source}" | awk '{print $1}' > "${source}.sha1"
  md5sum  "${source}" | awk '{print $1}' > "${source}.md5"
  put_to_gar "${source}.sha1" "${remote_url}.sha1"
  put_to_gar "${source}.md5"  "${remote_url}.md5"
}

# Generate and upload a minimal POM so GAR indexes this as a proper Maven artifact
publish_pom_to_gar() {
  local pom_file
  pom_file="$(mktemp)"
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
</project>
EOF

  local base_url="${GAR_MAVEN_REPO_URL}/${group_path}/${artifact_id}/${current_version}"
  local remote_url="${base_url}/${artifact_id}-${current_version}.pom"

  put_to_gar "${pom_file}" "${remote_url}"

  # Upload checksums for the POM
  sha1sum "${pom_file}" | awk '{print $1}' > "${pom_file}.sha1"
  md5sum  "${pom_file}" | awk '{print $1}' > "${pom_file}.md5"
  put_to_gar "${pom_file}.sha1" "${remote_url}.sha1"
  put_to_gar "${pom_file}.md5"  "${remote_url}.md5"

  rm -f "${pom_file}" "${pom_file}.sha1" "${pom_file}.md5"
}

# Generate and upload maven-metadata.xml so GAR can discover available versions
publish_maven_metadata_to_gar() {
  local metadata_file
  metadata_file="$(mktemp)"
  cat > "${metadata_file}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>${group_id}</groupId>
  <artifactId>${artifact_id}</artifactId>
  <versioning>
    <latest>${current_version}</latest>
    <release>${current_version}</release>
    <versions>
      <version>${current_version}</version>
    </versions>
  </versioning>
</metadata>
EOF

  local remote_url="${GAR_MAVEN_REPO_URL}/${group_path}/${artifact_id}/maven-metadata.xml"

  put_to_gar "${metadata_file}" "${remote_url}"

  sha1sum "${metadata_file}" | awk '{print $1}' > "${metadata_file}.sha1"
  md5sum  "${metadata_file}" | awk '{print $1}' > "${metadata_file}.md5"
  put_to_gar "${metadata_file}.sha1" "${remote_url}.sha1"
  put_to_gar "${metadata_file}.md5"  "${remote_url}.md5"

  rm -f "${metadata_file}" "${metadata_file}.sha1" "${metadata_file}.md5"
}

release_dir="/tmp/workspace/community/app/target/release"

info "Publishing API artifacts to Google Artifact Registry (${GAR_MAVEN_REPO_URL})"

# 1. Upload the POM (required for GAR to recognize the artifact)
publish_pom_to_gar

# 2. Upload the actual artifact files
publish_to_gar "${release_dir}/canton-open-source-${current_version}-api.zip" "zip"
publish_to_gar "${release_dir}/canton-open-source-${current_version}-api.tar.gz" "tar.gz"

# 3. Upload maven-metadata.xml (for version discovery)
publish_maven_metadata_to_gar

info "Done publishing API artifacts to GAR"

