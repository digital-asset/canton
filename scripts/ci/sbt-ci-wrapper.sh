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

# GHA MIGRATION: In observed GHA runs, /bin/bash subprocess locale behavior differs from the
# expected shell.nix locale setup. Instead of assuming why, validate locales in the same shell
# context and fall back only when that subprocess reports "cannot change locale".
# This block is gated to GHA to avoid changing CircleCI behavior in this PR.
if [[ "$IS_GHA" == "true" ]]; then
  choose_working_locale() {
    local candidate output
    for candidate in en_US.UTF-8 C.UTF-8 C; do
      output=$(LC_ALL="$candidate" LANG="$candidate" /bin/bash -c 'locale >/dev/null' 2>&1 || true)
      if [[ "$output" != *"cannot change locale"* ]]; then
        echo "$candidate"
        return 0
      fi
    done
    echo "C"
  }

  TARGET_LOCALE="$(choose_working_locale)"
  export LANG="$TARGET_LOCALE"
  export LC_ALL="$TARGET_LOCALE"
  if [[ "$TARGET_LOCALE" == "en_US.UTF-8" ]]; then
    info "Using locale ${TARGET_LOCALE} for sbt-ci-wrapper"
  else
    info "en_US.UTF-8 is not usable in this GHA subprocess context, using ${TARGET_LOCALE} instead."
  fi
fi


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

# The GHA ARC runner pods have no CPU limit, request or cpuset, so the JVM detects the whole node
# CPU count (e.g. 96) and sizes CPU-derived pools to it, far above the few cores a shard actually
# uses. The Scala global execution context is already pinned below via scala.concurrent.context.*,
# so this value covers what that setting does not reach: GC threads, JIT compiler threads, the JVM
# common ForkJoinPool and libraries that read availableProcessors directly (Netty, Pekko, gRPC).
# Applied on GHA only (see the gated block below), because CircleCI declares EXECUTOR_NUM_CPUS per
# resource class and its pods have CPU limits. Defaults to EXECUTION_CONTEXT_SIZE with a floor of 4:
# a job that dials the Scala execution context right down (the sequential deadlock-discovery run uses
# EC=1, protobuf continuity uses 2) is making a test-serialization choice, not declaring the shard's
# real CPU budget, so we keep a few threads for GC and JIT rather than starving them. An explicit
# override is taken as-is. Per-job tuning of this value can be a follow-up if the floor is too coarse.
if [[ -z "${EXECUTOR_ACTIVE_PROCESSOR_COUNT:-}" ]]; then
  EXECUTOR_ACTIVE_PROCESSOR_COUNT=$(( 10#${EXECUTION_CONTEXT_SIZE} > 4 ? 10#${EXECUTION_CONTEXT_SIZE} : 4 ))
fi

# SBT output mode
DEBUG="${DEBUG:-false}"
# Use Azure DevOps Maven mirror for dependencies
USE_MAVEN_MIRROR="${USE_MAVEN_MIRROR:-false}"
 # Init vars and assign values or default values
if [[ "$IS_GHA" == "true" ]]; then
  EXECUTOR_JVM_HEAP_SIZE="${EXECUTOR_JVM_HEAP_SIZE:-6500M}"
  # A GHA test shard runs a couple hundred suites in a single sbt JVM, so the
  # loaded-class footprint accumulates until Metaspace is exhausted and the JVM
  # aborts with OutOfMemoryError (exit 134). 4000M sat right at the edge and
  # tipped over intermittently across shards, so give it headroom. The runners
  # already provision 14000M of heap, so this extra Metaspace is cheap.
  EXECUTOR_JVM_METASPACE_SIZE="${EXECUTOR_JVM_METASPACE_SIZE:-6000M}"
else
  EXECUTOR_JVM_HEAP_SIZE="${EXECUTOR_JVM_HEAP_SIZE:-6500M}"
  EXECUTOR_JVM_METASPACE_SIZE="${EXECUTOR_JVM_METASPACE_SIZE:-2500M}"
fi
TIMEOUT="${TIMEOUT:-25m}"
MAX_SINGLE_TEST_MINUTES="${MAX_SINGLE_TEST_MINUTES:-0}"
SUCCEED_ON_ERROR="${SUCCEED_ON_ERROR:-0}"
RETRY_FETCH="${RETRY_FETCH:-0}"
REPORT_TO_DATADOG="${REPORT_TO_DATADOG:-true}"
SBT_BOOTSTRAP_RETRY_ATTEMPTS="${SBT_BOOTSTRAP_RETRY_ATTEMPTS:-2}"
SBT_BOOTSTRAP_RETRY_SLEEP_SECONDS="${SBT_BOOTSTRAP_RETRY_SLEEP_SECONDS:-30}"
SBT_BOOTSTRAP_RETRY_PATTERN="${SBT_BOOTSTRAP_RETRY_PATTERN:-Server returned HTTP response code: 429|CantDownloadModule|could not retrieve sbt}"
FAIL_ON_ERROR_IN_OUTPUT="${FAIL_ON_ERROR_IN_OUTPUT:-1}"

if [[ "${DEBUG,,}" == "true" || "${DEBUG,,}" == "1" ]]; then
  FAIL_ON_ERROR_IN_OUTPUT="false"
fi

CODE=0
WATCHDOG_TIMEOUT_TRIGGERED=false

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
            if [[ "$WATCHDOG_TIMEOUT_TRIGGERED" == "true" ]]; then
                echo "TESTCASE_TIMEOUT_TRIGGERED=true" >> "$GITHUB_ENV"
            fi
        fi
    if [[ "$IS_CCI" == "true" ]]; then
        echo "export STATUS=$CODE" >> "$BASH_ENV"
        if [[ "$WATCHDOG_TIMEOUT_TRIGGERED" == "true" ]]; then
            echo "export TESTCASE_TIMEOUT_TRIGGERED=true" >> "$BASH_ENV"
        fi
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
        elif [ "$CODE" == 190 ]
          then
            err "sbt has been stopped by the per-test watchdog after MAX_SINGLE_TEST_MINUTES=$MAX_SINGLE_TEST_MINUTES."
            HINT_MSG="single test timeout watchdog"
        else
            err "The script has failed with exit code $CODE."
        fi
        if [[ "${REPORT_TO_DATADOG,,}" == "true" || "${REPORT_TO_DATADOG}" == "1" ]]; then
          if [[ -z "${DATADOG_API_KEY:-}" ]]; then
            err "REPORT_TO_DATADOG is enabled, but DATADOG_API_KEY is not set"
            exit 1
          else
            python3 ./scripts/ci/report_failing_tests.py "SBT exited with code $CODE ($HINT_MSG)"
          fi
        else
          info "REPORT_TO_DATADOG is disabled, skipping Datadog reporting"
        fi
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
# (Making sure to not overwrite pre-existing SBT_OPTS which contain maven mirror settings setup by Github Action runner)
export SBT_OPTS="${SBT_OPTS} -Xmx$EXECUTOR_JVM_HEAP_SIZE"

# Create a local temp folder in the working directory
# This prevents protoc failures caused by 'noexec' locks on the global /tmp partition.
mkdir -p .citmp
export TEMPDIR="$PWD/.citmp"
export SBT_OPTS="${SBT_OPTS} -Djava.io.tmpdir=${TEMPDIR}"

_print_header "${c_white}Running parameters:${c_reset}"
# Print variable = value of configuration
for i in EXECUTION_CONTEXT_SIZE \
         MAX_CONCURRENT_SBT_TEST_TASKS \
         EXECUTOR_NUM_CPUS \
         EXECUTOR_ACTIVE_PROCESSOR_COUNT \
         EXECUTOR_JVM_HEAP_SIZE \
         EXECUTOR_JVM_METASPACE_SIZE \
         TIMEOUT \
         MAX_SINGLE_TEST_MINUTES \
         SUCCEED_ON_ERROR \
         REPORT_TO_DATADOG \
         RETRY_FETCH \
         SBT_BOOTSTRAP_RETRY_ATTEMPTS \
         SBT_BOOTSTRAP_RETRY_SLEEP_SECONDS \
         SBT_BOOTSTRAP_RETRY_PATTERN \
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

if ! [[ "$MAX_SINGLE_TEST_MINUTES" =~ ^[0-9]+$ ]]; then
  err "MAX_SINGLE_TEST_MINUTES must be a non-negative integer, got: $MAX_SINGLE_TEST_MINUTES"
  CODE=1
  exit 1
fi

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
# Use azure maven mirror (in CircleCI only)
# ${variable,,} -- convert value to lowercase (Bash ver > 4)
if [[ ("${USE_MAVEN_MIRROR,,}" == "true" || "${USE_MAVEN_MIRROR}" == "1") && "$IS_GHA" != "true" ]]; then
  # Allow override repositories
  SBT_CMD+=("-Dsbt.override.build.repos=true")
  SBT_CMD+=("-Dsbt.repository.config=${ABSDIR}/repositories")
  #  *** Credentials for Azure maven mirror ***
  #   Two separate credential mechanisms are needed:
  #
  #   1. Coursier (used by regular compile/test): reads credentials.sbt as Scala.
  #      Passes null realm; Coursier matches by host only.
  #      Variables: MAVEN_USERNAME, MAVEN_PASSWORD (stored in CircleCI context `maven-mirror`).
  #
  #   2. Ivy (used by sbt-license-report's updateLicenses): reads a Java .properties file.
  #      -Dsbt.credentials.file CANNOT read Scala .sbt files; it silently produces empty
  #      credentials if given one. We generate a proper properties file at runtime.
  #      Realm is left empty; sbt's IvyAuthenticator matches credentials by host only,
  #      so the exact WWW-Authenticate realm string does not need to match.
  #   -Dsbt.boot.credentials also expects a .properties file (same format), so we reuse
  #   the generated file for all three properties.
  #
  # Fail early if password is missing: the generated file would be written with an empty
  # password and the failure would surface much later as a cascade of "module not found".
  : "${MAVEN_PASSWORD:?MAVEN_PASSWORD must be set when USE_MAVEN_MIRROR=true}"
  # Generate a Java properties credentials file for Coursier boot, Ivy, and sbt boot.
  # Empty realm is intentional: sbt's Credentials.forHost matches by host, ignoring realm.
  IVY_CREDS_FILE="${TEMPDIR}/sbt-ivy-credentials.properties"
  install -m 600 /dev/null "${IVY_CREDS_FILE}"
  printf 'realm=\nhost=%s\nuser=%s\npassword=%s\n' \
    "${MAVEN_HOST:-pkgs.dev.azure.com}" \
    "${MAVEN_USERNAME:-digitalasset}" \
    "${MAVEN_PASSWORD}" > "${IVY_CREDS_FILE}"
  SBT_CMD+=("-Dsbt.boot.credentials=${IVY_CREDS_FILE}")
  SBT_CMD+=("-Dsbt.coursier.credentials=${ABSDIR}/credentials.sbt")
  SBT_CMD+=("-Dsbt.credentials.file=${IVY_CREDS_FILE}")
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

# Pin the JVM's view of the available CPU count so GC threads, JIT compiler threads, the JVM common
# ForkJoinPool and availableProcessors-based library pools (Netty, Pekko, gRPC) are sized to the
# intended parallelism rather than the full (uncapped) node CPU count. This does not touch the Scala
# global execution context, which scala.concurrent.context.numThreads/maxThreads above already pins.
# Gated to GHA to avoid changing CircleCI behavior in this PR: CircleCI declares EXECUTOR_NUM_CPUS per
# resource class and its pods have CPU limits, so the uncapped-node problem this solves does not apply
# there. Only applied when the value resolves to a positive integer, so a non-numeric override leaves
# the JVM default untouched. The 10# prefix forces base-10 so a value with leading zeros (e.g. 08) is
# not misread as octal.
if [[ "$IS_GHA" == "true" && "${EXECUTOR_ACTIVE_PROCESSOR_COUNT}" =~ ^[0-9]+$ && "$((10#${EXECUTOR_ACTIVE_PROCESSOR_COUNT}))" -gt 0 ]]; then
  SBT_CMD+=("-J-XX:ActiveProcessorCount=${EXECUTOR_ACTIVE_PROCESSOR_COUNT}")
fi

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
attempt=1

# Parses the elapsed time from a ScalaTest slowpoke warning line emitted by sbt.
# The format is produced by ScalaTest's -W flag (houserules.sbt) and looks like:
#
#   [info] *** Test still running after 23 minutes, 53 seconds: suite name: Foo, test name: bar.
#
# The leading "[info] *** " prefix is matched by the glob wildcard, so it does not
# need to be stripped explicitly. If ScalaTest ever changes this format the match
# silently stops firing, so the self-test below locks in the expected input shape.
extract_slowpoke_seconds() {
  local line="$1"
  local duration
  local hours=0
  local minutes=0
  local seconds=0

  [[ "$line" == *"Test still running after "* ]] || return 1

  duration="${line#*Test still running after }"
  duration="${duration%%:*}"

  if [[ "$duration" =~ ([0-9]+)[[:space:]]+hour ]]; then
    hours="${BASH_REMATCH[1]}"
  fi
  if [[ "$duration" =~ ([0-9]+)[[:space:]]+minute ]]; then
    minutes="${BASH_REMATCH[1]}"
  fi
  if [[ "$duration" =~ ([0-9]+)[[:space:]]+second ]]; then
    seconds="${BASH_REMATCH[1]}"
  fi

  echo $((hours * 3600 + minutes * 60 + seconds))
}

# Self-test: feed a known slowpoke line through extract_slowpoke_seconds to catch
# any future format drift early, before the watchdog silently stops working.
_self_test_extract_slowpoke_seconds() {
  local sample="[info] *** Test still running after 23 minutes, 53 seconds: suite name: Foo, test name: bar."
  local expected=1433  # 23*60 + 53
  local got
  got=$(extract_slowpoke_seconds "$sample") || {
    err "extract_slowpoke_seconds self-test: function returned non-zero for a valid line"
    exit 1
  }
  if [[ "$got" != "$expected" ]]; then
    err "extract_slowpoke_seconds self-test failed: expected $expected seconds, got $got"
    exit 1
  fi
}
_self_test_extract_slowpoke_seconds

emit_retry_fetch_tokens() {
  local retries="${RETRY_FETCH:-0}"
  local i=0

  if ! [[ "$retries" =~ ^[0-9]+$ ]]; then
    echo "RETRY_FETCH must be a non-negative integer, got: $retries" >&2
    return 1
  fi

  while [[ "$i" -lt "$retries" ]]; do
    echo r
    i=$((i + 1))
  done
}

run_sbt_with_optional_single_test_watchdog() {
  local current_attempt="$1"
  local enabled="false"
  local watchdog_seconds=0
  local pipe_path="${TEMPDIR}/sbt-output-${current_attempt}.pipe"
  local job_pid=0

  if (( MAX_SINGLE_TEST_MINUTES > 0 )); then
    enabled="true"
    watchdog_seconds=$((MAX_SINGLE_TEST_MINUTES * 60))
    info "Single-test watchdog enabled: ${MAX_SINGLE_TEST_MINUTES} minute(s)"
  fi

  : > "${SBT_OUTPUT_FILE}"

  # Fast path: when the watchdog is disabled, avoid the FIFO/per-line read loop.
  # Keep live logs on stdout and append to file with a single tee process.
  if [[ "$enabled" != "true" ]]; then
    (
      set -o pipefail
      emit_retry_fetch_tokens | \
        timeout --kill-after=30s "${TIMEOUT}" "${SBT_CMD[@]}" 2>&1 | tee -a "${SBT_OUTPUT_FILE}"
    )
    CODE=$?
    return
  fi

  rm -f "$pipe_path"
  mkfifo "$pipe_path"

  if ! command -v setsid >/dev/null 2>&1; then
    err "setsid is required for watchdog process-group cleanup but was not found in the CI environment"
    exit 1
  fi

  # Launch timeout as the session leader and keep its child in the same foreground process group.
  # This lets the watchdog kill timeout -> sbt -> JVM and all descendants with one group signal,
  # instead of only terminating an outer shell while timeout keeps the FIFO open until TIMEOUT.
  export -f emit_retry_fetch_tokens
  export RETRY_FETCH
  setsid timeout --foreground --kill-after=30s "${TIMEOUT}" \
    bash -c 'set -o pipefail; emit_retry_fetch_tokens | "$@"' -- "${SBT_CMD[@]}" \
    >"$pipe_path" 2>&1 &
  job_pid=$!

  # Slow path: watchdog enabled, keep one fd open for file appends to avoid
  # spawning printf|tee processes for every log line.
  exec 3>>"${SBT_OUTPUT_FILE}"
  while IFS= read -r line || [[ -n "$line" ]]; do
    printf '%s\n' "$line"
    printf '%s\n' "$line" >&3

    if [[ "$enabled" == "true" && "$WATCHDOG_TIMEOUT_TRIGGERED" == "false" ]]; then
      if elapsed_seconds=$(extract_slowpoke_seconds "$line"); then
        if (( elapsed_seconds >= watchdog_seconds )); then
          WATCHDOG_TIMEOUT_TRIGGERED=true
          err "Detected test case running for ${elapsed_seconds}s, exceeding MAX_SINGLE_TEST_MINUTES=${MAX_SINGLE_TEST_MINUTES}. Stopping this shard run."
          # Kill the entire process group so timeout, the JVM and all descendants go down together.
          # job_pid is the setsid leader (timeout), so its PGID == job_pid.
          kill -TERM -- "-$job_pid" 2>/dev/null || true
          sleep 5
          kill -KILL -- "-$job_pid" 2>/dev/null || true
        fi
      fi
    fi
  done <"$pipe_path"

  exec 3>&-

  wait "$job_pid"
  CODE=$?

  rm -f "$pipe_path"

  if [[ "$WATCHDOG_TIMEOUT_TRIGGERED" == "true" ]]; then
    # Dedicated code to distinguish a per-test watchdog timeout from other sbt failures.
    CODE=190
  fi
}

# Note: RETRY_FETCH pipes newlines into sbt to prompt it to resume stalled dependency downloads
# (in-process, single run). The outer bootstrap retry loop below is complementary: it re-invokes
# the entire sbt process when the resolver itself is throttled (429) or the sbt launcher fails to
# bootstrap. These two mechanisms target different failure modes and are not redundant.
# Worst-case wall clock time is SBT_BOOTSTRAP_RETRY_ATTEMPTS x TIMEOUT (default: 2 x 25m = 50m).
# Ensure the GHA job's timeout-minutes is set to at least that value.
while true; do
  run_sbt_with_optional_single_test_watchdog "$attempt"

  # Retry only for transient bootstrap and repository throttling errors.
  if [[ "$CODE" != 0 ]] && grep -E -q "${SBT_BOOTSTRAP_RETRY_PATTERN}" "${SBT_OUTPUT_FILE}"; then
    if [[ "$attempt" -lt "$SBT_BOOTSTRAP_RETRY_ATTEMPTS" ]]; then
      err "sbt bootstrap/download failed (attempt ${attempt}/${SBT_BOOTSTRAP_RETRY_ATTEMPTS}), retrying in ${SBT_BOOTSTRAP_RETRY_SLEEP_SECONDS}s..."
      attempt=$((attempt + 1))
      sleep "${SBT_BOOTSTRAP_RETRY_SLEEP_SECONDS}"
      continue
    fi
  fi

  break
done

# Use filter 'ansi2txt' to remove control characters starting with '\e' like:
#   reset text formatting: '\e[m'
#   colors: '\e[1;34m' '\e[90m' '\e[97m' '\e[0m'
#   foreground and background colors: '\e[30;41m'
./scripts/ci/ansi2txt.sh < "${SBT_OUTPUT_FILE}" > "temp_sbt_output" && \
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
