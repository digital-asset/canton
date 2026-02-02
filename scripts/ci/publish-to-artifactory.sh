#!/usr/bin/env bash
set -eo pipefail

# get the full path to this directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

if [[ -z "$RELEASE_SUFFIX" ]]; then
  # the suffix gets defined in a previous job. we just reuse it here.
  err "ERROR, NO RELEASE_SUFFIX DEFINED! You need to run this job after the obtain_release_suffix job."
  exit 1
fi

# We don't publish "SNAPSHOT" releases, only properly dated snapshots or full releases.
if [[ "$RELEASE_SUFFIX" == *"-SNAPSHOT" ]]; then
  info "Skip publishing of snapshot release"
  exit 0
fi

if [[ -z "${ARTIFACTORY_USER}" || -z "${ARTIFACTORY_PASSWORD}" ]]; then
   err "artifactory user:key is empty: ${ARTIFACTORY_USER} / pw. maybe add context:artifactory?"
   exit 1
fi

publish () {
  source=$1
  full_path=$2
  current_version=$3
  info "Publishing ${source} to ${full_path}"
  if [ ! -e "$source" ]; then
    err "ERROR, file does not exist: ${source}."
    exit 1
  fi

  if [[ "${current_version}" != "dev" ]] && curl -u "${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}" -o /dev/null --silent --head --fail "${full_path}"; then
    warn "Skipping the publication as the artifact already exists"
    return
  fi

  exitcode=0
  curl -u "${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}" -sSf -X PUT -T "${source}" "${full_path}" 2> curl.stderr || exitcode=$?
  if [ $exitcode -ne 0 ]; then
    echo "Curl returned exit code $exitcode"
    curl_stderr=$(cat curl.stderr)
    echo "$curl_stderr"
    rm -f curl.stderr
    exit $exitcode
  fi
  rm -f curl.stderr
  echo "" # curl output does not add the new line
}


# publish canton community and lib releases to artifactory (nightly only on tuesdays)
artifactory_url="https://digitalasset.jfrog.io/artifactory"
current_version=$RELEASE_SUFFIX
nightly_release="${IS_NIGHTLY_RELEASE:-$1}" # CircleCI parameter nightly_release

# Publish non-nightly releases or nightly releases once a week (on Tuesdays)
if [[ "$nightly_release" == "false" || $(date +"%u") -eq 2 ]]; then

  # Publish community edition as enterprise for backwards compatibility
  echo "Publishing canton release artifact"
  for ending in "zip" "tar.gz"
  do
    publish "$(ls /tmp/workspace/community/app/target/release/canton-open-source-${current_version}.${ending} | tail -n 1)" \
            "${artifactory_url}/canton-enterprise/canton-enterprise-${current_version}.${ending}" \
            "${current_version}"
  done
  echo "Done publishing canton-standalone artifacts"

  # publish internal jar artifacts
  list=("sequencer-driver-lib sequencer-driver-lib" "community/sequencer-driver-api-conformance-tests sequencer-driver-api-conformance-tests" "community-integration-testing-lib community-integration-testing-lib")
  for i in "${list[@]}"
  do
    set -- $i # convert the "tuple" into the param args $1 $2...
    local_path=$1
    module=$2
    echo "Publishing canton ${module} lib release (once per week, on canton-only release and with dev suffix on every push to main, or manually)"
    repo="canton-internal"
    package="com/digitalasset/canton"
    for ending in ".jar" "-javadoc.jar" "-sources.jar" ".pom"
    do
      ending_with_version="${current_version}${ending}"

      # the workspace path is based on the `sbt package` task (and is therefore guaranteed to be stable as long as it's not changed in following sbt versions or reconfigured in sbt)
      # the artifactory path is tweaked to be properly picked up by build tools
      # for instance, in sbt, the artifacts can be accessed by "com.digitalasset.canton" %% "${module}_2.13" % "${current_version}"
      publish "$(ls /tmp/workspace/${local_path}/target/scala-2.13/${module}*${ending_with_version} | tail -n 1)" \
              "${artifactory_url}/${repo}/${package}/${module}_2.13/${current_version}/${module}_2.13-${ending_with_version}" \
              "${current_version}"
    done
  done
  echo "Done publishing internal jar lib artifacts"

  # publish external jar artifacts
  list=("community/kms-driver-api kms-driver-api kms-driver-api" "kms-driver-testing-lib kms-driver-testing kms-driver-testing-lib" "community/mock-kms-driver mock-kms-driver mock-kms-driver")
  for i in "${list[@]}"
  do
    set -- $i # convert the "tuple" into the param args $1 $2...
    local_path=$1
    remote_name=$2
    module=$3
    echo "Publishing ${module} lib release"
    repo="canton-${remote_name}"
    package="com/digitalasset/canton"
    for ending in ".jar" "-javadoc.jar" "-sources.jar" ".pom"
    do
      ending_with_version="${current_version}${ending}"

      # the workspace path is based on the `sbt package` task (and is therefore guaranteed to be stable as long as it's not changed in following sbt versions or reconfigured in sbt)
      # the artifactory path is tweaked to be properly picked up by build tools
      # for instance, in sbt, the artifacts can be accessed by "com.digitalasset.canton" %% "${module}_2.13" % "${current_version}"
      publish "$(ls /tmp/workspace/${local_path}/target/scala-2.13/${module}*${ending_with_version} | tail -n 1)" \
               "${artifactory_url}/${repo}/${package}/${module}_2.13/${current_version}/${module}_2.13-${ending_with_version}" \
               "${current_version}"
    done
  done
  echo "Done publishing external jar lib artifacts"

  # publish jsonapi zip artifact
  module="canton-json-apidocs"
  echo "Publishing  ${module} tar.gz release"
  repo="canton-internal"
  package="com/digitalasset/canton"
  ending=".tar.gz"

  ending_with_version="${current_version}${ending}"

  # the workspace path is based on the `sbt package` task (and is therefore guaranteed to be stable as long as it's not changed in following sbt versions or reconfigured in sbt)
  # the artifactory path is tweaked to be properly picked up by build tools
  # for instance, in sbt, the artifacts can be accessed by "com.digitalasset.canton" %% "${module}_2.13" % "${current_version}"
  publish "$(ls /tmp/workspace/target/${module}*${ending_with_version} | tail -n 1)" \
            "${artifactory_url}/${repo}/${package}/${module}_2.13/${current_version}/${module}_2.13-${ending_with_version}" \
            "${current_version}"

  echo "Done publishing internal tgz apidocs artifact"

  list=("2.1" "2.dev")
  for lf_version in "${list[@]}"
  do
    module="ledger-api-test-tool"
    local_path="community/ledger-test-tool/tool/lf-v${lf_version}"
    echo "Publishing canton ${module} jar release"
    repo="canton-internal"
    package="com/digitalasset/canton"
    ending=".jar"
    ending_with_version="${current_version}${ending}"

    publish "$(ls /tmp/workspace/${local_path}/target/scala-2.13/${module}*${ending_with_version} | tail -n 1)" \
            "${artifactory_url}/${repo}/${package}/${module}_2.13/${current_version}/${module}-${lf_version}_2.13-${ending_with_version}" \
            "${current_version}"
  done

  # publish all releases to our assembly repo
  echo "Publishing community .tar.gz to artifactory/assembly/canton/..."
  list=("open-source community")
  for i in "${list[@]}"
  do
    set -- $i # convert the "tuple" into the param args $1 $2...
    edition_external=$1
    edition_internal=$2

    source=$(ls /tmp/workspace/${edition_internal}/app/target/release/canton-${edition_external}-*.tar.gz | tail -n 1)
    publish "$source" \
            "${artifactory_url}/assembly/canton/${current_version}/canton-${edition_external}-${current_version}.tar.gz" \
            "${current_version}"
  done

  echo "Publishing canton scaladoc to artifactory/assembly/canton/${current_version}/..."
  publish "/tmp/workspace/scaladoc.tar.gz" \
          "${artifactory_url}/assembly/canton/${current_version}/canton-scaladoc-${current_version}.tar.gz" \
          "${current_version}"

  # TODO(i29845): Uncomment the following lines when `build_systematic_testing_inventory` is fixed
  #echo "Publishing canton test evidence to artifactory/assembly/canton/${current_version}/..."
  #publish "/tmp/workspace/security-tests.csv" \
  #        "${artifactory_url}/assembly/canton/${current_version}/canton-security-tests-${current_version}.csv" \
  #        "${current_version}"
  #publish "/tmp/workspace/reliability-tests.csv" \
  #        "${artifactory_url}/assembly/canton/${current_version}/canton-reliability-tests-${current_version}.csv" \
  #        "${current_version}"

  echo "Publishing protos to artifactory/assembly/canton/${current_version}/..."
  publish "/tmp/workspace/community/app/target/release/canton-open-source-${current_version}-protobuf.tar.gz" \
          "${artifactory_url}/assembly/canton/${current_version}/canton-open-source-${current_version}-protobuf.tar.gz" \
          "${current_version}"
  publish "/tmp/workspace/community/app/target/release/canton-open-source-${current_version}-protobuf.zip" \
            "${artifactory_url}/assembly/canton/${current_version}/canton-open-source-${current_version}-protobuf.zip" \
            "${current_version}"

  echo "Publishing canton versions to info.json"
  jo -p canton_version=$current_version $(cat /tmp/workspace/info.properties) | tee /tmp/workspace/info.json
  publish "/tmp/workspace/info.json" \
          "${artifactory_url}/assembly/canton/${current_version}/info.json" \
          "${current_version}"

  echo "Done publishing canton open-source, documentation/rst, and info.json to artifactory/assembly/canton/${current_version}/..."
else
  echo "Skipped publishing artifacts: only once a week on Tuesdays or for non-nightly releases"
fi
