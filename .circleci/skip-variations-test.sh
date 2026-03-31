#! /usr/bin/env bash

set -euo pipefail

REPO="${CIRCLE_PROJECT_USERNAME}/${CIRCLE_PROJECT_REPONAME}"
PULL_NUMBER=$((echo "${CIRCLE_PULL_REQUEST-}" | grep -o -E '[0-9]+$') || true)

is_pull () {
  [ ! -z "${CIRCLE_PULL_REQUEST-}" ]
}

get_pr_github_user () {
  curl \
    -s -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    "https://api.github.com/repos/${REPO}/pulls/${PULL_NUMBER}" | jq -r .user.login
}

if is_pull; then
  echo "Detected a PR!"
  echo "Checking for the github user of the PR..."
  user=$(get_pr_github_user)
  echo "Found PR user: $user"
  forced_users=$(cat .circleci/github-users-forcing-variations-tests.txt)
  force_check=""
  while read forced_user; do
    if [[ "$forced_user" = "$user" ]]; then
      force_check="yes"
    fi
  done <<< "$forced_users"
  if [[ "$force_check" = "yes" ]]; then
    echo "Forcing variation tests as PR user is amongst github-users-forcing-variations-tests.txt"
  else
    echo "Skipping variation tests as PR user is not amongst github-users-forcing-variations-tests.txt"
    circleci-agent step halt
  fi
else
  echo "No PR detected! Skipping variation tests."
  circleci-agent step halt
fi
