#! /usr/bin/env bash

set -euo pipefail

REPO="DACH-NY/canton"
PULL_NUMBER=$((echo "${CIRCLE_PULL_REQUEST-}" | grep -o -E '[0-9]+$') || true)

is_pull () {
  [ ! -z "${CIRCLE_PULL_REQUEST-}" ]
}

get_pull_labels () {
  echo "Requesting labels on the PR..."
  curl \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    "https://api.github.com/repos/${REPO}/issues/${PULL_NUMBER}/labels" > .pull_labels
}

interrupt_if_skip_ci () {
  echo "Checking for the label 'skip ci'..."
  EXIT_CODE=0
  (cat .pull_labels | grep "name" | grep -q "skip ci") || EXIT_CODE=$?
  if [ $EXIT_CODE -eq 0 ]; then
    echo "Label 'skip ci' found, exiting with 1 to interrupt ci"
    exit 1
  else
    echo "Label 'skip ci' not found"
    return 0
  fi
}

if is_pull; then
  echo "Detected a PR!"
  get_pull_labels
  interrupt_if_skip_ci
else
  echo "No PR detected!"
fi
