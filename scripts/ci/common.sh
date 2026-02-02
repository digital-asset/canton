#!/usr/bin/env bash
# Set TERM-dumb if not set
[[ -z "$TERM" ]] && TERM=dumb

# colors
c_white="\e[1;97m"
c_lwhite="\e[0;97m"
c_red="\e[0;31m"
c_lred="\e[1;31m"
c_green="\e[0;32m"
c_lgreen="\e[1;32m"
c_grey="\e[90m"
c_magenta="\e[95m"
c_yellow="\e[0;93m"
c_lyellow="\e[1;93m"
c_blue="\e[1;34m"
c_cyan="\e[0;36m"
c_reset="\e[0m"

# Dump environment variables to log folder
dump_vars() {
  local dump_log="dump_vars.log"
  local absdir="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
  if [[ -d "$absdir/../../log" ]]; then
    dump_log="$absdir/../../log/dump_vars.log"
  fi
  for((i=0;i<30;i++)); do echo -n "*" >> "${dump_log}"; done
  echo "" >> "${dump_log}"
    env >> "${dump_log}"
  for((i=0;i<30;i++)); do echo -n "*" >> "${dump_log}"; done
  echo "" >> "${dump_log}"
    cat "${BASH_ENV}" >> "${dump_log}"
  for((i=0;i<30;i++)); do echo -n "*" >> "${dump_log}"; done
    echo "" >> "${dump_log}"
  info "Environment variables dumped to $dump_log"
}
###############################################################################
### printing & formatting #####################################################
###############################################################################

err() {
  (>&2 echo -e "${c_grey}$(basename $0)${c_white}: [\e[1;31mERROR${c_white}]:${c_reset} $1")
}
warn() {
  (>&2 echo -e "${c_grey}$(basename $0)${c_white}: [\e[0;33mWARN${c_white}]:${c_reset} $1")
}
info() {
  (>&2 echo -e "${c_grey}$(basename $0)${c_white}: [\e[0;36mINFO${c_white}]:${c_reset} $1")
}
info_n() {
  (>&2 echo -e -n "${c_grey}$(basename $0)${c_white}: [\e[0;36mINFO${c_white}]:${c_reset} $1")
}
info_done() {
  (>&2 echo -e "${c_grey}$(basename $0)${c_white}:${c_lred}<${c_white}[${c_lgreen}DONE${c_white}]:${c_reset} $1")
}
info_fail() {
  (>&2 echo -e "${c_grey}$(basename $0)${c_white}:${c_lred}<${c_white}[${c_lred}FAIL${c_white}]:${c_reset} $1")
}
info_item() {
  (>&2 echo -e "${c_grey}$(basename $0)${c_white}: [\e[0;36mINFO${c_white}]:${c_reset}$(_tab)$(_tab)${c_white}-${c_reset} ${1}")
}
info_step() {
  info ""
  (>&2 echo -e "${c_grey}$(basename $0)${c_white}:${c_lred}>${c_white}[${c_lgreen}STEP${c_white}]:${c_reset} $1")
  info ""
}
info_exit() {
  (>&2 echo -e "${c_grey}$(basename $0)${c_white}: [${c_lyellow}EXIT${c_white}]:${c_reset} $1")
}
debug() {
  [ -z "$DEBUG" ] || (>&2 echo -e "${c_grey}$(basename $0)${c_white}: [\e[0;90mDEBUG${c_white}]:${c_reset} $1")
}
# Getting size of current terminal
# '\e7':           Save the current cursor position.
# '\e[9999;9999H': Move the cursor to the bottom right corner.
# '\e[6n':         Get the cursor position (window size).
# '\e8':           Restore the cursor to its previous position.
get_term_size() {
    IFS='[;' read -sp $'\e7\e[9999;9999H\e[6n\e8' -d R -rs _ LINES COLUMNS
}
# Helper function to run commands with debug and error logging
# Failed commands will exit the process with a 1 exit code
# First parameter should be message to use for logging
# Remaining arguments are passed to the subshell to run the command
# Usage: run "Listing some directory" ls dir-name
run() {
  local message="$1"
  shift
  local -ra cmd=( "$@" )
  local output
  (>&2 echo -e "${c_grey}$(basename $0)${c_white}: [${c_lred}#\e[0;32mRUN${c_white}]:${c_reset} $message \e[0;90m[${c_reset}${cmd[*]}\e[0;90m]${c_reset}")
  if ! output=$("$@" 2>&1); then
    err "$message failed: $output"
    exit 1
  fi
}
_tab()           { printf "\t"; }
info_failed()    { info "$1 [\e[33;1m\e[0;31mFAILED${c_reset}]"; }
progress_start() { info_n "${c_white}[\e[1;34mStarting${c_white}]:${c_reset} ${1} "; }
progress_step()  { printf '.'; }
progress_done()  { echo -e "${c_white} [ \e[33;1m\e[0;32mDONE${c_white} ]${c_reset}"; }
_print_header()  { info ""; info "$(_tab)$(_tab)${c_white}${1}${c_reset}"; info ""; }
_print_line()    {
  local term_size=$COLUMNS
  local pre_size="$(echo $(basename $0) | wc -c)"
  local symbols_count="$(($term_size - ($pre_size + 1)))"
   printf "${c_grey}$(basename $0)${c_white}:%*s\n" "${symbols_count}" "" | tr " " -;
}

## Formatted output for print arguments as part of help out
function print_help_item() {
  echo "    ${1}"
  echo "       - ${2}"
  if [[ "${3}" != '' ]]; then
    echo "       - ${3}"
  fi
  echo ""
}

###############################################################################
### dryrun command execution ##################################################
###############################################################################
## Wrapper for either executing a command or printing out what would be
## executed if DRYRUN is set to true.
DRYRUN='false'
function _cmd() {
  if [[ "${DRYRUN}" == 'true' ]]; then
    info "$(_tab)\e[91m>>${c_reset} would run the following command:"
    info_item "${@}"
  else
    "${@}"
  fi
}

###############################################################################
### retry command on fail #####################################################
###############################################################################
## Function that will attempt to retry a given command n number of times
## - default is to run 6 retries
## - there is a 5 second sleep between retries
## - if first arugment is a number, this will be used as the number of retry attempts
## - all following arguments are the command to execute
function retry() {
  local retries
  local sleep
  retries='6'
  sleep='5'

  if [[ ! "${1}" =~ [a-zA-Z] ]]; then
      info "overriding retries to ${1}"
      retries="${1}"
      shift 1
  fi

  for i in $(seq 1 ${retries}); do
    if "${@}"; then
      return 0
    fi
    info "$(_tab)\e[91m>>${c_reset} Command failed (attempt ${i}/${retries}): ${@}"
    sleep "${sleep}"
  done
    info "$(_tab)\e[91m>>${c_reset} Command failed after ${retries} attempts: ${@}"
  return 1
}

###############################################################################
### _check_files_exist ########################################################
###############################################################################
## Checks that all files provided exist and are not empty
function _check_files_exist() {
  if [[ "${#@}" -lt '1' ]]; then
    err "no files provided for validation..."
    return 1
  else
    info "\e[91m>>${c_reset} Checking that all files provided exist and are not empty"
    for FILE in "${@}"; do
      info_n "$(progress_step)"
      if [[ -s "${FILE}" ]]; then
        progress_step
      else
        # echo " fail"
        info_failed "\e[97${FILE}${c_reset} is does not exist or is empty, failing..."
        return 1
      fi
      printf "\n"
    done
  fi
  info_done "\e[91m>>${c_reset} Checking files complete "
  return 0
}

###############################################################################
### _check_if_variables_empty #################################################
###############################################################################
## Checks n number of variables to see if they are empty:
## - exits 1 of any of the variables are empty
## - exits 2 if no variables are provided as arguments
function _check_if_variables_empty(){
  local ret_code
  ret_code=0

  ## exit 2 if no arguments are proide
  if [[ ${#@} -lt 1 ]]; then return 2; fi

  ## itterate over args and use variable indirect
  for i in ${@}; do
    if [ -z "${!i}" ]; then
      ret_code=1
    fi
  done

  return ${ret_code}
}

###############################################################################
### _jfrog_auth_netrc_file ####################################################
###############################################################################

## Checks if we have ~/.netrc that has what appears to be a machine entry...
function _jfrog_auth_netrc_file(){
  local ret_code
  ret_code='0'
  if [[ ! -s "${HOME}/.netrc" ]]; then ret_code='1'; fi
  grep -q 'machine digitalasset.jfrog.io' ${HOME}/.netrc || ret_code='1'
  return "${ret_code}"
}

###############################################################################
### _setup_jfrog_curl_auth ####################################################
###############################################################################
## Function populates jfrog_curl_auth based on the following:
## - use vars ARTIFACTORY_READONLY_USER and ARTIFACTORY_READONLY_PASSWORD if they are not empty
## - use vars ARTIFACTORY_USERNAME and ARTIFACTORY_PASSWORD if they are not empty
## - use ~/.netrc if it has an entry for digitalasset.jfrog.io
declare -a jfrog_curl_auth='()'
function _setup_jfrog_curl_auth(){

  _check_if_variables_empty ARTIFACTORY_READONLY_USER ARTIFACTORY_READONLY_PASSWORD
  if [[ "${?}" == '0' ]]; then
    jfrog_curl_auth=('-u' "${ARTIFACTORY_READONLY_USER}:${ARTIFACTORY_READONLY_PASSWORD}")
    info ""
    info "\e[91m>>${c_reset} Using env vars for jfrog credentials"
    return 0
  fi

  _check_if_variables_empty ARTIFACTORY_USERNAME ARTIFACTORY_PASSWORD
  if [[ "${?}" == '0' ]]; then
    jfrog_curl_auth=('-u' "${ARTIFACTORY_USERNAME}:${ARTIFACTORY_PASSWORD}")
    info ""
    info "\e[91m>>${c_reset} Using env vars for jfrog credentials"
    return 0
  fi

  _jfrog_auth_netrc_file
  if [[ "${?}" == '0' ]]; then
    jfrog_curl_auth=('-n')
    info ""
    info "\e[91m>>${c_reset} defaulting to use ${c_white}${HOME}/.netrc${c_reset} for jfrog credentials"
     return 0
  fi

  err "unable to find suitable authentication details for jfrog";
  return 1
}

###############################################################################
### file checksums ############################################################
###############################################################################

## prints md5 checksum of file
function _md5_checksum_file() {
  md5sum "${1}" | awk '{print $1}'
}

## prints sha checksum of file
function _sha1_checksum_file() {
  shasum -a 1 "${1}" | awk '{print $1}'
}


###############################################################################
### tell slack ################################################################
###############################################################################
function tell_slack() {
  local message channel pr_url from_version to_version
  message="${1:?message required}"
  channel="${2:?channel required}"
  pr_url="${3:-}"
  from_version="${4:-}"
  to_version="${5:-}"

  if [ -n "$pr_url" ]; then
    if [ -n "$from_version" ] && [ -n "$to_version" ]; then
      full_message="$message from \`$from_version\` to \`$to_version\`"
    else
      full_message="$message"
    fi

    full_message="$full_message: <$pr_url|PR>"
    jq -n --arg full_message "$full_message" '{"text": $full_message}' |
      curl -XPOST -i -H 'Content-Type: application/json' -d @- $channel
  else
    echo "Skipping Slack notification because PR_URL is not set."
  fi
}
