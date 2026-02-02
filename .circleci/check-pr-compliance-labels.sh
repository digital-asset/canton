#! /usr/bin/env bash

set -euo pipefail

REPO="DACH-NY/canton"
LABELS=( "Standard-Change" )
COMPLIANCE_PATH_PREFIXES=( ".ci" "release" "deployment" "scripts/ci" "Dockerfile" "nix" "shell.nix" "project" "build.sbt" "dependencies.json" ) # See <root>/CODEOWNERS
PULL_NUMBER=$((echo "${CIRCLE_PULL_REQUEST-}" | grep -o -E '[0-9]+$') || true)

# compares the candidate trunk branches with the current commit using `git diff` and picks the least different one
detect_base () {
  TRUNK_BRANCHES=( $(git branch --all | grep origin/release-line) )
  BASE="remotes/origin/main"
  BASE_DIFF_LINES=$(git diff --merge-base origin/main HEAD | wc -l)
  for b in "${TRUNK_BRANCHES[@]}"; do
    DIFF_LINES=$(git diff --merge-base "$b" HEAD | wc -l)
    if [[ "${DIFF_LINES}" -lt "${BASE_DIFF_LINES}" ]]; then
      BASE="$b"
      BASE_DIFF_LINES=DIFF_LINES
    fi
  done
  echo "$BASE"
}

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

get_pull_files_changed () {
  echo "Requesting files changed on the PR..."
  curl \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    "https://api.github.com/repos/${REPO}/pulls/${PULL_NUMBER}/files?per_page=100" \
      | (grep "filename" || true ) \
      | sed -n -e 's/\s*"filename": "//p' | sed -n -e 's/",//p' \
      | (grep -v -x -f .circleci/compliance-files-exceptions.txt || true ) > .files_changed
}

get_merge_base_files_changed () {
  BASE=$(detect_base)
  echo "Using git diff to detect changes vs the base merge branch '$BASE'..."
  git diff --merge-base --name-only "$BASE" HEAD | grep -v -x -f .circleci/compliance-files-exceptions.txt > .files_changed
}

has_changed_path_prefix () {
  cat .files_changed | grep -q "^$1"
}

is_missing_label () {
  echo "Checking for the label '$1'..."
  EXIT_CODE=0
  (cat .pull_labels | grep "name" | grep -q "$1") || EXIT_CODE=$?
  if [ $EXIT_CODE -eq 0 ]; then
    echo "Compliance label '$1' is present, thank you for you diligence!"
    return 0
  else
    echo "The PR is missing the compliance label '$1'"
    return 1
  fi
}

add_pull_labels () {
  curl \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    "https://api.github.com/repos/${REPO}/issues/${PULL_NUMBER}/labels" \
    -d "{\"labels\":[$1]}"
}

if is_pull; then
  echo "Detected a PR!"
  get_pull_files_changed
else
  echo "No PR detected!"
  get_merge_base_files_changed
fi
echo "Files changed against the base:"
cat .files_changed

CHANGED=1
for p in "${COMPLIANCE_PATH_PREFIXES[@]}"; do
  if has_changed_path_prefix "$p"; then
    CHANGED=0
  fi
done

if [ "${CHANGED}" -eq "0" ]; then
  echo "Compliance controlled sources changed!"
  if is_pull; then
    get_pull_labels
    SUCCESS=1
    for l in "${LABELS[@]}"; do
      is_missing_label "$l" || SUCCESS=0
    done
    if [ "$SUCCESS" -eq "0" ]; then
      echo "Adding compliance labels to the PR..."
      labels_quoted_delimited=$(printf "\"%s\"," "${LABELS[@]}")
      labels_quoted_delimited="${labels_quoted_delimited:0:-1}"
      add_pull_labels "${labels_quoted_delimited}"
    fi

  else
    echo "Failing this task as file changes require a PR labeled with
    'Standard-Change' for compliance reasons.
    Please re-run this task once PR has been opened!"
    exit 1
  fi

else
  echo "No compliance labels check required on the PR"
fi
