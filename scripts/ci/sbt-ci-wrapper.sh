#!/usr/bin/env bash
set -o pipefail

ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"
source "$ABSDIR/common.sh" # debug, info, err, colors

# GHA MIGRATION: Added environment detection
IS_GHA="${GITHUB_ACTIONS:-false}"
IS_CCI="${CIRCLECI:-false}"

if [[ "$IS_GHA" == "true" ]]; then
    SBT_OUTPUT_FILE="${SBT_OUTPUT_FILE:-sbt_output}"
    echo "sbt-log-file=${SBT_OUTPUT_FILE}" >> "${GITHUB_OUTPUT}"
    export BASH_ENV="${GITHUB_ENV:-/dev/null}"
else
    SBT_OUTPUT_FILE="sbt_output"
fi

_print_header "Wrapper for ${c_lgreen}CI/CD${c_reset} run SBT (GHA: $IS_GHA, CCI: $IS_CCI)"

# GHA Migration: Added new condition for GHA
if [ -z "${EXECUTOR_NUM_CPUS##*[!0-9]*}" ]; then
  if [[ "$IS_GHA" == "true" ]]; then
    EXECUTOR_NUM_CPUS=$(nproc 2>/dev/null || grep processor /proc/cpuinfo | wc -l || echo 4)
  else
    if [[ "$(uname -s)" == "Darwin" ]]; then
      EXECUTOR_NUM_CPUS="$(sysctl hw.ncpu | awk '{print $2}')"
    else
      EXECUTOR_NUM_CPUS="$(grep processor /proc/cpuinfo | wc -l)"
    fi
  fi
  info "Detected ${EXECUTOR_NUM_CPUS} CPUs"
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
if [[ "$IS_GHA" == "true" ]]; then
  EXECUTOR_JVM_HEAP_SIZE="${EXECUTOR_JVM_HEAP_SIZE:-14000M}"
  EXECUTOR_JVM_METASPACE_SIZE="${EXECUTOR_JVM_METASPACE_SIZE:-4000M}"
else
  EXECUTOR_JVM_HEAP_SIZE="${EXECUTOR_JVM_HEAP_SIZE:-6500M}"
  EXECUTOR_JVM_METASPACE_SIZE="${EXECUTOR_JVM_METASPACE_SIZE:-2500M}"
fi
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
    # GHA MIGRATION: Added new CODE export for GHA
    if [[ "$IS_GHA" == "true" ]]; then
            echo "STATUS=$CODE" >> "$GITHUB_ENV"
        fi
    if [[ "$IS_CCI" == "true" ]]; then
        echo "export STATUS=$CODE" >> "$BASH_ENV"
    fi
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
        python3 ./scripts/ci/collect_failing_tests_and_send_to_datadog.py "SBT exited with code $CODE ($HINT_MSG)"
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
if [[ "${CI}" == "true" || "${CI}" == "!" || "$IS_GHA" == "true" ]]; then
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

# GHA Migration: Added more secure conditions for CUSTOM_JAVA_HOME
# Specify custom java home if supplied
if [[ -n "${CUSTOM_JAVA_HOME}" && -d "${CUSTOM_JAVA_HOME}" && -x "${CUSTOM_JAVA_HOME}/bin/java" ]]; then
  SBT_CMD+=("-java-home" "${CUSTOM_JAVA_HOME}")
fi

# Setup metaspace
SBT_CMD+=("-J-XX:MaxMetaspaceSize=$EXECUTOR_JVM_METASPACE_SIZE")

# Create a heap dump on OOME
SBT_CMD+=("-J-XX:+HeapDumpOnOutOfMemoryError")

# Setup execution context size
SBT_CMD+=("-J-Dscala.concurrent.context.numThreads=${EXECUTION_CONTEXT_SIZE}")
SBT_CMD+=("-J-Dscala.concurrent.context.maxThreads=${EXECUTION_CONTEXT_SIZE}")

# Print JVM arguments
SBT_CMD+=("-J-XX:+PrintCommandLineFlags")

# Add extra parameters
if [[ -n "${EXTRA_PARAMETERS}" ]]; then
  SBT_CMD+=( ${EXTRA_PARAMETERS} )
fi

# GHA_MIGRATION: Added additional checks for JAVA_HOME_FOR_TESTS
# Specify custom java home to run tests without compilation
# Purpose: Run tests with a different (newer) java version that was used for compilation
if [[ -n "${OVERRIDE_JAVA_VERSION_FOR_TESTS}" ]]; then
  if [[ -n "${JAVA_HOME_FOR_TESTS}" && -d "${JAVA_HOME_FOR_TESTS}" ]]; then
    info "Using OVERRIDE_JAVA_VERSION_FOR_TESTS: ${OVERRIDE_JAVA_VERSION_FOR_TESTS}"
    SBT_CMD+=("set Global / compile / skip := true")
    SBT_CMD+=("-java-home" "${JAVA_HOME_FOR_TESTS}")
  else
    warn "OVERRIDE_JAVA_VERSION_FOR_TESTS is set, but JAVA_HOME_FOR_TESTS is empty or directory does not exist!"
  fi
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
  timeout --kill-after=30s "${TIMEOUT}" "${SBT_CMD[@]}" 2>&1 | \
  tee "${SBT_OUTPUT_FILE}"

# save sbt command exit code for use on exit
CODE=${PIPESTATUS[1]}

# Use filter 'ansi2txt' to remove control characters starting with '\e' like:
#   reset text formatting: '\e[m'
#   colors: '\e[1;34m' '\e[90m' '\e[97m' '\e[0m'
#   foreground and background colors: '\e[30;41m'
cat "${SBT_OUTPUT_FILE}" | ./scripts/ci/ansi2txt.sh > "temp_sbt_output" && \
  mv "temp_sbt_output" "${SBT_OUTPUT_FILE}" && \
  info_done "Remove control symbols from logfile"

if [[ "$CODE" == 0 ]]; then
  # Check and report whether sbt has output errors
  # Need to also apply ignore rules for the log, as errors in the log are emitted to stdout by default.
  if [[ "${FAIL_ON_ERROR_IN_OUTPUT,,}" == "true" || "${FAIL_ON_ERROR_IN_OUTPUT}" == "1" ]]; then
    ./scripts/ci/check-sbt-output.sh "${SBT_OUTPUT_FILE}" "project/errors-in-sbt-output-to-ignore.txt" "project/errors-in-log-to-ignore.txt"
    CODE=$?
  fi
else
  err "SBT piped command exit code: PIPESTATUS=${CODE}"
fi
