#!/usr/bin/env bash
set -e

IS_GHA="${GITHUB_ACTIONS:-false}"

# checkTodo.sc expects specific CIRCLE CI envs so here for GHA we set them to their equivalent from GHA
if [[ "$IS_GHA" == "true" ]]; then
  if [[ -n "${GITHUB_EVENT_PATH:-}" && -f "${GITHUB_EVENT_PATH}" ]]; then
      CIRCLE_PR_URL="$(jq -r '.pull_request.html_url // empty' "${GITHUB_EVENT_PATH}")"
  else
    CIRCLE_PR_URL=""
  fi
  export CIRCLE_PULL_REQUEST="$CIRCLE_PR_URL"
  export CIRCLE_BRANCH="${GITHUB_HEAD_REF:-$GITHUB_REF_NAME}"
  export CI="true"
fi

amm .circleci/todo-script/src/checkTodos.sc

if [[ "$IS_GHA" == "true" && -f "todo-out/count" ]]; then
    echo "count=$(cat todo-out/count)" >> "$GITHUB_OUTPUT"
fi
