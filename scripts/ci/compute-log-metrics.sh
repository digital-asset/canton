#!/usr/bin/env bash
set -eu -o pipefail

LOGFILE="$1"

# Get the full path to the deployment directory
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$SRCDIR/../.."

# Load utility functions
source "$PROJECT_ROOT/scripts/ci/common.sh"
source "$PROJECT_ROOT/scripts/ci/gnu-tools.sh"

if [[ ! -f "$LOGFILE" ]]
then
  info "Logfile '$LOGFILE' does not exist. Aborting..."
  exit 0
fi

# File size
LOG_SIZE="$(wc -c < "$LOGFILE")"
echo "export LOG_SIZE=$LOG_SIZE"

if [[ $LOG_SIZE -eq 0 ]]; then
  info "Logfile is empty. Aborting..."
  exit 0
fi

# Line count and length
cat "$LOGFILE" |\
"$GNU_GREP" -v -E \
-e "ApiSubmissionService" \
-e "ApiCommandCompletionService" \
-e "LedgerTimeAwareCommandExecutor" \
-e "requires authorizers TXRejectMultiActorMissingAuth-alpha" \
-e "requires authorizers SemanticPartialSignatories-alpha" \
-e "requires one of the stakeholders TreeSet\(SemanticPrivacyProjections-alpha" \
-e "Details: Last location: \[.*\], partial transaction:" \
|\
awk '{ lines = lines + 1; sum = sum + length }
  length > max_length { max_length = length }
  END { printf("export LOG_LINE_COUNT=%d\n", lines);
    printf("export AVG_LOG_LINE_LENGTH=%.1f\n", sum / lines);
    printf("export MAX_LOG_LINE_LENGTH=%d\n", max_length);
  }'

# Entry size

if ! command -v lnav &> /dev/null
then
  info "lnav is not installed. Skipping computation of log entry statistics..."
  exit 0
fi

# Install the canton log format
if ! output="$(lnav -i "$PROJECT_ROOT"/canton.lnav.json 2>&1)"; then
    err "Install the canton log format failed: $output"
    exit 1
fi
# Check the configuration
if ! output="$(lnav -C 2>&1)"; then
    err "lnav configuration check failed: $output"
    exit 1
fi

entry_stats=$(lnav -n -c \
';SELECT printf("%.1f", AVG(length(log_body))) as AVG_LOG_ENTRY_SIZE,
MAX(length(log_body)) as MAX_LOG_ENTRY_SIZE
FROM canton_log
WHERE log_body not like "Successfully executed '\''java -jar %"
AND log_body not like "%java.%Exception: %"
' \
"$LOGFILE")

# shellcheck disable=SC2206
entry_stats_arr=($entry_stats)
echo "export ${entry_stats_arr[0]}=${entry_stats_arr[2]}"
echo "export ${entry_stats_arr[1]}=${entry_stats_arr[3]}"
