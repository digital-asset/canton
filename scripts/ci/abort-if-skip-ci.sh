#!/usr/bin/env bash
set -euo pipefail

if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
  PLATFORM="gha"
  REPO="${GITHUB_REPOSITORY}"
  if [[ -n "${GITHUB_EVENT_NUMBER:-}" ]]; then
    PULL_NUMBER="${GITHUB_EVENT_NUMBER}"
  else
    PULL_NUMBER=$(gh pr list --head "${GITHUB_REF_NAME}" --limit 1 --json number --jq '.[0].number' || echo "")
  fi
elif [[ "${CIRCLECI:-}" == "true" ]]; then
  PLATFORM="cci"
  REPO="${CIRCLE_PROJECT_USERNAME}/${CIRCLE_PROJECT_REPONAME}"
  PULL_NUMBER=$(echo "${CIRCLE_PULL_REQUEST-}" | grep -o -E '[0-9]+$' || echo "")
else
  echo "Local execution - skipping"
  exit 0
fi

is_pull() {
  [[ -n "${PULL_NUMBER:-}" ]]
}

get_pull_labels() {
  echo "Requesting labels on PR #${PULL_NUMBER}..."
  curl -s -f \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    "https://api.github.com/repos/${REPO}/issues/${PULL_NUMBER}/labels" > .pull_labels
}

interrupt_if_skip_ci() {
  echo "Checking for 'skip ci' label..."
  if grep -qi '"name"[^}]*skip ci' .pull_labels; then
    echo "'skip ci' found - interrupting CI"
    case $PLATFORM in
      "gha")
        echo "skip=true" >> "$GITHUB_OUTPUT"
        exit 0
        ;;
      "cci")
        echo "Skip CI label detected - halting gracefully"
        circleci-agent step halt
        ;;
    esac
  else
    echo "No skip labels"
    if [[ "$PLATFORM" == "gha" ]]; then
     echo "skip=false" >> "$GITHUB_OUTPUT"
    fi
    return 0
  fi
}

if is_pull; then
  get_pull_labels
  interrupt_if_skip_ci
else
  echo "Not a PR - continuing"
   if [[ "$PLATFORM" == "gha" ]]; then
      echo "skip=false" >> "$GITHUB_OUTPUT"
   fi
fi
