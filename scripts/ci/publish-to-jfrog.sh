#!/usr/bin/env bash
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
source "$ABSDIR/common.sh" # debug, info, err

  _CURL="$(which curl)"
  _GIT="$(which git)"
  HEADER='ETX project publisher of artifacts to jfrog'
  ARTIFACTORY_REPO="${ARTIFACTORY_REPO:-generic-sandbox}"
  CIRCLE_PROJECT_REPONAME="${CIRCLE_PROJECT_REPONAME:-canton}"
  CIRCLE_BUILD_NUM="${CIRCLE_BUILD_NUM:-0}"
  CIRCLE_PIPELINE_NUMBER="${CIRCLE_PIPELINE_NUMBER:-0}"
  DELETE_ARTIFACT='false'
  repo_name="${ARTIFACTORY_REPO}"
  sanctioned_build='false'
  target_file=""
  additional_parameters=("")

function render_help() {
  echo "  Publish artifacts to jfrog from CI/CD pipelines:"
  echo ""

  print_help_item "-r|repo" \
    "repository name [optional]"

  print_help_item "-s|sanctioned_build <version_number>" \
    "publish an artifact that has been promoted [optional]"

  print_help_item "-d|dryrun" \
    "execute script in dryrun mode, will not upload any files [optional]"

  print_help_item "-D|delete" \
    "delete an artifact"

  print_help_item "-p|property <key=value>" \
    "add property key value pair for artifacts [optional]"

  print_help_item "-h|help" \
    "you are reading it"
}

  while [[ $# -gt 0 ]]; do
    case $1 in
    -r|repo)
      repo_name="${2}"
      shift 2
      ;;
    -s|sanctioned_build)
      sanctioned_build='true'
      version_number="${2}"
      shift 2
      ;;
    -d|dryrun)
      DRYRUN='true'
      shift 1
      ;;
    -D|delete)
      DELETE_ARTIFACT='true'
      shift 1
      ;;
    -p|parameter)
      additional_parameter+=("${2}")
      shift 2
      ;;
    -h|help|--help)
      render_help
      exit 0
      ;;
    *)
      target_file="${1}"
      shift 1
      ;;
    esac
  done

  _print_header "${HEADER}"
  if [[ "${DRYRUN}" == 'true' ]]; then
    info "$(_tab)$(_tab)$(_tab)\e[92m<<< \e[93mdry run mode enabled! \e[92m>>>\e[0m"
    info ""
  fi

  _check_if_variables_empty ARTIFACTORY_USERNAME ARTIFACTORY_PASSWORD ARTIFACTORY_REPO
  if [[ "${?}" != '0' ]]; then
   warn ""
   warn "\e[91m>>\e[0m ARTIFACTORY_USERNAME ARTIFACTORY_PASSWORD ARTIFACTORY_REPO - env vars not set!"
  fi

  _check_if_variables_empty CIRCLE_PROJECT_REPONAME CIRCLE_BUILD_NUM CIRCLE_PIPELINE_NUMBER
  if [[ "${?}" != '0' ]]; then
   warn ""
   warn "\e[91m>>\e[0m CIRCLE_PROJECT_REPONAME CIRCLE_BUILD_NUM or CIRCLE_PIPELINE_NUMBER - env vars not set!"
  fi

  if [[ "${DELETE_ARTIFACT}" == 'false' ]]; then
    _check_files_exist "${target_file}" || exit ${?}
    ## generate md5 and sha1 checksums of file to publish:
    artifact_md5_checksum=$(_md5_checksum_file "${target_file}")
    artifact_sha1_checksum=$(_sha1_checksum_file "${target_file}")

    info ""
    info "\e[33;1m\e[0;32m${target_file}\e[0m will be published with the following checksums\e[0m:"
    info_item "\e[97martifact_md5_checksum: \e[93m${artifact_md5_checksum}\e[0m"
    info_item "\e[97martifact_sha1_checksum: \e[93m${artifact_sha1_checksum}\e[0m"

    url_args=("${target_file}")

    ## Add git commit sha to properties:
    ## - gitsha is ETX legacy key
    ## - vcs.revision is what jfrog tooling publishes
    url_args+=("gitsha=$(git rev-parse HEAD)")
    url_args+=("vcs.revision=$(git rev-parse HEAD)")

    ## Add build information:
    url_args+=("build.name=${CIRCLE_PROJECT_REPONAME}")
    url_args+=("build.system=circleci")
    url_args+=("build.timestamp=$(date '+%s')")
    url_args+=("build.job.number=${CIRCLE_BUILD_NUM}")

    ## add pipeline number if present:
    if [[ "${CIRCLE_PIPELINE_NUMBER:-null}" != 'null' ]]; then
      url_args+=("build.number=${CIRCLE_PIPELINE_NUMBER}")
    else
      url_args+=("build.number=${CIRCLE_BUILD_NUM}")
    fi

    if [[ ${sanctioned_build} == 'true' ]]; then
      url_args+=("released=true")
      url_args+=("version_number=${version_number}")
    else
      url_args+=("released=false")
      url_args+=("branch_name=$(git rev-parse --abbrev-ref HEAD)")
      url_args+=("vcs.branch=$(git rev-parse --abbrev-ref HEAD)")
    fi

    ## append additional parameters:
    url_args+=(${additional_parameter[@]})
    info ""
    info "\e[33;1m\e[0;32m${target_file}\e[0m will be published with the following properties:"

    ## iterate over all url parameters and concatenate them in to
    ## a single variable to use as part of the url
    count='0'
    url_vars=""
    for url_arg in ${url_args[@]}; do
      url_vars="${url_vars}${url_arg}"
      let "count=count+1"
      ## append an ; apart from the last url variable
      if [[ "${count}" -lt "${#url_args[@]}" ]]; then
        url_vars="${url_vars};"
      fi
    info_item "$(echo "\e[97m${url_arg}\e[0m" | sed 's/=/: \\e[93m/g')"
    done
  fi
  _setup_jfrog_curl_auth || exit "${?}"
  cmd+="${_CURL} "
  cmd+=${jfrog_curl_auth[@]}
  if [[ "${DELETE_ARTIFACT}" == 'true' ]]; then
      cmd+=" -X DELETE "
      cmd+="\"https://digitalasset.jfrog.io/artifactory/${repo_name}/${target_file}\""
   else
      cmd+=" --header \"X-Checksum-MD5:${artifact_md5_checksum}\" "
      cmd+=" --header \"X-Checksum-Sha1:${artifact_sha1_checksum}\" "
      cmd+=" --upload-file \"${target_file}\" "
      cmd+=" \"https://digitalasset.jfrog.io/digitalasset/${repo_name}/${url_vars}\""
  fi
  ## attempt to retry publishing if something does wrong in the internets:
  for i in 1 2 3 4 5 6; do \
    info "";
    if [[ "${DELETE_ARTIFACT}" == 'true' ]]; then
      info "\e[91m>>\e[0m Deleting \e[33;1m\e[0;32m${target_file}\e[m from \"\e[93m${repo_name}\e[m\" jfrog repository attempt [${i}/6]:"
    else
      info "\e[91m>>\e[0m Publishing \e[33;1m\e[0;32m${target_file}\e[m to \"\e[93m${repo_name}\e[m\" jfrog repository attempt [${i}/6]:"
    fi
    if [[ "${DRYRUN}" == 'true' ]]; then
      echo "${cmd[*]}"
    else
      echo "${cmd[*]}"
      eval $(echo "${cmd[*]}")
    fi
    ret_code="${?}"
    info "$(_tab)\e[91m>>\e[0m curl returned exit code: ${ret_code}"
    if [[ "${ret_code}" == '0' ]];
      then
        exit "${ret_code}";
    fi
    if [[ "${i}" == '6' ]];
      then
        err "\e[91m>>\e[0m Artifact push/delete failed 6 times, please investigate issue";
        exit "${ret_code}";
    fi
    info "\e[91m>>\e[0m Will attempt to publish/delete again in 30 seconds..."
    sleep 30
  done
