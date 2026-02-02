#!/usr/bin/env bash
set -eo pipefail
# Get the full path to the deployment directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${ABSDIR}/common.sh"
source "${ABSDIR}"/gnu-tools.sh
source "${ABSDIR}"/io-utils.sh

_print_header "Check logs output "

LOGFILE="$1"
FILE_WITH_IGNORE_PATTERNS="$2"

# Succeed if logfile does not exist
if [[ ! -f "$LOGFILE" ]]
then
  warn "$LOGFILE does not exist."
  warn "Skipping log file check."
  exit 0
fi

# Check rg tool
if [ ! -x "$(command -v rg)" ]; then
  err  "Missing tool required: rg"
  exit 1
fi

# Load ignore patterns for later use
IGNORE_PATTERNS=$(remove_comment_and_blank_lines < "$FILE_WITH_IGNORE_PATTERNS")

# Keywords are based on `canton.lnav.json`
MATCH_PREFIX="^[0-9\\-]{10} [0-9:,]+ \\[[A-Za-z0-9\\-_]+\\]"
MIN_LOG_LEVEL_WARNING="${MATCH_PREFIX} WARN|${MATCH_PREFIX} ERROR"

# rg returns 1 if there were not matches
set +o pipefail

# Check for errors and warnings
cat "$LOGFILE" |
  join_daml_error_lines |
  rg -a -v -f <(echo "$IGNORE_PATTERNS") |
  rg -a -e "$MIN_LOG_LEVEL_WARNING" |
  sed "s/ \\[[^ ]*\\] / [⋮] /" |  # replaces non-whitespace [canton-env-execution-context-123] with [⋮]
  output_problems "problems" "$LOGFILE"

