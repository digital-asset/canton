#!/usr/bin/env bash
set -eo pipefail
# Get the full path to the deployment directory
ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
source "${ABSDIR}/common.sh"
source "${ABSDIR}"/gnu-tools.sh
source "${ABSDIR}"/io-utils.sh

_print_header "Check SBT output "

SBT_OUTPUT_FILE="$1"
shift
FILES_WITH_IGNORE_PATTERNS=("$@")

# Succeed if logfile does not exist
if [[ ! -f "${SBT_OUTPUT_FILE}" ]]; then
  warn "${SBT_OUTPUT_FILE} does not exist."
  warn "Skipping sbt log file check"
  exit 0
fi

# Check rg tool
if [ ! -x "$(command -v rg)" ]; then
  err  "Missing tool required: rg"
  exit 1
fi

read_sbt_output() {
  cat "$SBT_OUTPUT_FILE"
}

patterns_to_ignore() {
  cat "${FILES_WITH_IGNORE_PATTERNS[@]}" | remove_comment_and_blank_lines
}

filter_errors() {
  rg -a -i -e error -e severe -e warn -e warning -e exception -e critical -e fatal || true
}

remove_ignored_patterns() {
  rg -U -a -v -f <(patterns_to_ignore)
}

compact_thread_info() {
  sed "s/ \\[.*\\] / [⋮] /"
}

# Process the output
read_sbt_output |
join_daml_error_lines |
remove_ignored_patterns |
filter_errors |
compact_thread_info |
output_problems "problems" "${SBT_OUTPUT_FILE}"

