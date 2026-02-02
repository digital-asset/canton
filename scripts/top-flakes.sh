#!/bin/bash

# Get the full path to the deployment directory
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$SRCDIR/.."

# Load utility functions
source "$PROJECT_ROOT/scripts/ci/gnu-tools.sh"


if [[ -z "$1" ]]; then
  echo "Usage: $0 NUM_DAYS [TOP_N_FLAKES]"
  exit 1
fi

LIMIT=${2:-10}

GNU_DATE=$(find_gnu_tool "date" "gdate")

# Assume input.json is your JSON file containing the array of objects
gh api graphql -F query=@$SRCDIR/list-flaky-test-issues.graphql | jq '.data.repository.milestone.issues.nodes[] | .title, .number, .body' | while read -r title; read -r number; read -r body; do
  # Count matching lines in the body
  match_count=$(echo -e $body | $GNU_GREP -E "^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"  | \
  awk -v start=$($GNU_DATE -d "$1 days ago" +%Y-%m-%d) -v end=$($GNU_DATE +%Y-%m-%d) '$1 >= start && $1 <= end' | wc -l)

  # Output the title, number, and count for later sorting
  echo "$match_count $(echo "$title" | $GNU_SED -e 's/^"//' -e 's/"$//') https://github.com/DACH-NY/canton/issues/$number"
done | sort -rn | head -n $LIMIT
