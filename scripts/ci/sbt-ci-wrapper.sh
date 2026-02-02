#!/usr/bin/env bash
set -o pipefail
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
source "$ABSDIR/common.sh" # debug, info, err, colors

_print_header "Wrapper for ${c_lgreen}CI/CD${c_reset} run SBT"

# if EXECUTOR_NUM_CPUS is not a number/or empty, set it to CPU count
if [ -z "${EXECUTOR_NUM_CPUS##*[!0-9]*}" ]; then
 if [[ "$(uname -s)" == "Darwin" ]]; then
    EXECUTOR_NUM_CPUS="$(sysctl hw.ncpu | awk '{print $2}')"
  elif [[ "$(uname -s)" == "Linux" ]]; then
    EXECUTOR_NUM_CPUS="$(grep processor /proc/cpuinfo | wc -l)"
  fi
fi

# if EXECUTION_CONTEXT_SIZE is not a number/or empty, set it to EXECUTOR_NUM_CPUS
if [ -z "${EXECUTION_CONTEXT_SIZE##*[!0-9]*}" ]; then
  EXECUTION_CONTEXT_SIZE=${EXECUTOR_NUM_CPUS}
fi
TEMPDIR="${TEMPDIR:-/tmp}"
# SBT output mode
DEBUG="${DEBUG:-false}"
# Use Azure DevOps Maven mirror for dependencies
USE_MAVEN_MIRROR="${USE_MAVEN_MIRROR:-false}"
 # Init vars and assign values or default values
EXECUTOR_JVM_HEAP_SIZE="${EXECUTOR_JVM_HEAP_SIZE:-6500M}"
EXECUTOR_JVM_METASPACE_SIZE="${EXECUTOR_JVM_METASPACE_SIZE:-2500M}"
TIMEOUT="${TIMEOUT:-25m}"
SUCCEED_ON_ERROR="${SUCCEED_ON_ERROR:-0}"
RETRY_FETCH="${RETRY_FETCH:-0}"
FAIL_ON_ERROR_IN_OUTPUT="${FAIL_ON_ERROR_IN_OUTPUT:-1}"
if [[ "${DEBUG,,}" == "true" || "${DEBUG,,}" == "1" ]]; then
  FAIL_ON_ERROR_IN_OUTPUT="false"
fi
CODE=0
# Print variable and value
print_var() {
  local value
  if [[ "x${!1}" == "x" ]]; then
      value="x"
    else
      value="${!1}"
  fi
    case "${value,,}" in
      x)
        info " ${c_blue}*  ${c_grey}${1}${c_white} = <<empty>>${c_reset}"
        ;;
      1|true)
        info " ${c_blue}*  ${c_grey}${1}${c_white} = ${c_lgreen}True${c_reset}"
        ;;
      0|false)
        info " ${c_blue}*  ${c_grey}${1}${c_white} = ${c_lred}False${c_reset}"
        ;;
      *)
        info " ${c_blue}*  ${c_grey}${1}${c_white} = ${!1}${c_reset}"
        ;;
    esac
}
# Run on trap EXIT
on_exit() {
    # Export exit code for usage in subsequent steps
    echo "export STATUS=$CODE" >> "${BASH_ENV}"
    # Provide some explanation on exit
    if [ "$CODE" == 0 ]
      then
        info "The script has terminated successfully."
    elif [ "$CODE" == 1 ]
      then
        err "The script has failed with exit code 1 (likely a test failure)"
    elif [ "$CODE" == 2 ]
      then
        # exit code produced by check-sbt-output.sh
        err "The script has failed with exit code 2 (likely an unexpected log message)"
    else
        # Everything else is reported to Datadog. Give some hints in case of known failures.
        local HINT_MSG="no further information"
        if [ "$CODE" == 117 ]
          then
            # exit code produced by FatalError.scala
            err "sbt has been aborted due to an error that was considered fatal."
        HINT_MSG="fatal error"
        elif [ "$CODE" == 124 ]
          then
            err "sbt has been aborted with the TERM signal after $TIMEOUT."
            HINT_MSG="likely timed out"
        elif [ "$CODE" == 137 ]
          then
            err "sbt has been killed because it has allocated too much memory or it has ignored the TERM signal after $TIMEOUT."
            HINT_MSG="likely used too much memory or ignored TERM signal"
        else
            err "The script has failed with exit code $CODE."
        fi
        nix-shell -I nixpkgs=./nix/nixpkgs.nix --run "python3 ./scripts/ci/collect_failing_tests_and_send_to_datadog.py \"SBT exited with code $CODE ($HINT_MSG)\""
    fi
        # ${variable,,} -- convert value to lowercase (Bash ver > 4)
        if [[ "${SUCCEED_ON_ERROR,,}" == "true" || "${SUCCEED_ON_ERROR}" == "1" ]]; then
            warn "Overriding original exit code $CODE with zero."
            CODE=0
        fi
 exit $CODE
  }
          trap on_exit EXIT

# Necessary workaround to prevent sbt from setting default JVM options
export SBT_OPTS="-Xmx$EXECUTOR_JVM_HEAP_SIZE"


_print_header "${c_white}Running parameters:${c_reset}"
# Print variable = value of configuration
for i in EXECUTION_CONTEXT_SIZE \
         MAX_CONCURRENT_SBT_TEST_TASKS \
         EXECUTOR_NUM_CPUS \
         EXECUTOR_JVM_HEAP_SIZE \
         EXECUTOR_JVM_METASPACE_SIZE \
         TIMEOUT \
         SUCCEED_ON_ERROR \
         RETRY_FETCH \
         FAIL_ON_ERROR_IN_OUTPUT \
         CUSTOM_JAVA_HOME \
         EXTRA_PARAMETERS \
         OVERRIDE_JAVA_VERSION_FOR_TESTS \
         USE_MAVEN_MIRROR \
         RELEASE_SUFFIX \
         LOG_IMMEDIATE_FLUSH \
         DEBUG; do
print_var $i
done
info ""
# Define sbt command
SBT_CMD=("sbt")
# if running in CI, set properties
if [[ "${CI}" == "true" || "${CI}" == "!"  ]]; then
  SBT_CMD+=("-Dsbt.ci=true") # tell sbt that it is running in CI
  # Instructs sbt to use Java's native methods for retrieving file timestamps, which typically offer
  # millisecond resolution. Docker container filesystems might truncate file modification times to
  # second-level resolution, leading to incorrect incremental compilation behavior. This options is fixing that.
  SBT_CMD+=("-Dsbt.io.jdktimestamps=true")
fi

# Set log level
if [[ "$DEBUG" == "true" || "$DEBUG" == "1" ]]; then
  SBT_CMD+=("--debug")
  SBT_CMD+=("-Dsbt.supershell=false")
  SBT_CMD+=("-Dsbt.coursier.log=debug")
  SBT_CMD+=("-Dcoursier.verbose=true")
  SBT_CMD+=("-Dsbt.log.noformat=yes")
else
  SBT_CMD+=("--verbose")
fi
# Use maven mirror
# ${variable,,} -- convert value to lowercase (Bash ver > 4)
if [[ "${USE_MAVEN_MIRROR,,}" == "true" || "${USE_MAVEN_MIRROR}" == "1" ]]; then
  # Allow override repositories
  SBT_CMD+=("-Dsbt.override.build.repos=true")
  SBT_CMD+=("-Dsbt.repository.config=${ABSDIR}/repositories")
  #  *** Credentials for Azure maven mirror ***
  #   `realm` - must be `null` or `empty  to work with Azure
  #   `host` - `pkgs.dev.azure.com` for Azure maven mirror
  #.  `username` - used environment variable `MAVEN_USERNAME``
  #   `password`- used environment variable `MAVEN_PASSWORD` (PERSONAL_ACCESS_TOKEN)
  # * Note *
  #    Variables stored in CircleCi context `maven-mirror`
  SBT_CMD+=("-Dsbt.boot.credentials=${ABSDIR}/credentials.sbt")
  # sbt and coursier can have different authorization, so it both need to be authorized dedicated.
  SBT_CMD+=("-Dsbt.coursier.credentials=${ABSDIR}/credentials.sbt")
  SBT_CMD+=("-Dsbt.credentials.file=${ABSDIR}/credentials.sbt")
fi

# Setup heap size
SBT_CMD+=("-J-Xmx$EXECUTOR_JVM_HEAP_SIZE" "-J-Xms$EXECUTOR_JVM_HEAP_SIZE")

# Specify custom java home if supplied
if [[ -n "${CUSTOM_JAVA_HOME}" ]]; then
  SBT_CMD+=("-java-home" "${CUSTOM_JAVA_HOME}")
fi

# Setup metaspace
SBT_CMD+=("-J-XX:MaxMetaspaceSize=$EXECUTOR_JVM_METASPACE_SIZE")

# Setup execution context size
SBT_CMD+=("-J-Dscala.concurrent.context.numThreads=${EXECUTION_CONTEXT_SIZE}")
SBT_CMD+=("-J-Dscala.concurrent.context.maxThreads=${EXECUTION_CONTEXT_SIZE}")

# Print JVM arguments
SBT_CMD+=("-J-XX:+PrintCommandLineFlags")

# Add extra parameters
if [[ -n "${EXTRA_PARAMETERS}" ]]; then
  SBT_CMD+=( ${EXTRA_PARAMETERS} )
fi

# Specify custom java home to run tests without compilation
# Purpose: Run tests with a different (newer) java version that was used for compilation
if [[ ! -z "${OVERRIDE_JAVA_VERSION_FOR_TESTS}" ]]; then
  SBT_CMD+=("set Global / compile / skip := true")
  SBT_CMD+=("-java-home" "$JAVA_HOME_FOR_TESTS")
fi
# Add sbt commands.
# Do not quote this, to allow the caller to pass in several commands.
# The caller needs to take care of quoting, if a command contains spaces.
for i in "$@"; do
  SBT_CMD+=( "$(printf "%s\n" "$i")" );
done
# Run command
# also send a few newline characters to sbt to ensure we keep on downloading dependencies
# PIPESTATUS - array with exit codes piped command.
# Examples:
#   date | grep 2025 | wc -l
#   echo "${PIPESTATUS[0]} ${PIPESTATUS[1]} ${PIPESTATUS[2]}"
#   false | true
#   echo "${PIPESTATUS[0]} ${PIPESTATUS[1]}"
python3 -c "import os; print ('r\n' * int(os.environ.get('RETRY_FETCH', 0)))" | \
  timeout --kill-after=30s "${TIMEOUT}" .ci/nix-exec "${SBT_CMD[@]}" 2>&1 | \
  tee sbt_output

# save sbt command exit code for use on exit
CODE=${PIPESTATUS[1]}
# Use filter 'ansi2txt' to remove control characters starting with '\e' like:
#   reset text formatting: '\e[m'
#   colors: '\e[1;34m' '\e[90m' '\e[97m' '\e[0m'
#   foreground and background colors: '\e[30;41m'
  cat "sbt_output" | ./scripts/ci/ansi2txt.sh > "temp_sbt_output" && \
  mv "temp_sbt_output" "sbt_output"
  info_done "Remove control symbols from \"sbt_output\" logfile"

if [[ "$CODE" != 0 ]]; then
  err "SBT piped command exit code: PIPESTATUS=${CODE}"
else
  # ${variable,,} -- convert value to lowercase (Bash ver > 4)
  if [[ "${FAIL_ON_ERROR_IN_OUTPUT,,}" == "true" || "${FAIL_ON_ERROR_IN_OUTPUT}" == "1" ]]; then
    # Check and report whether sbt has output errors
    # Need to also apply ignore rules for the log, as errors in the log are emitted to stdout by default.
    .ci/nix-exec ./scripts/ci/check-sbt-output.sh "sbt_output" "project/errors-in-sbt-output-to-ignore.txt" "project/errors-in-log-to-ignore.txt"
    CODE=$?
  fi
fi
