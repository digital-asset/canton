#!/usr/bin/env bash

set -eu -o pipefail

LOCK="${HOME}/lock"
LAST_REV_FILE="$(pwd)/last_rev"

# Give up if lock is taken
if mkdir "$LOCK" &>/dev/null; then
  trap 'rmdir $LOCK' EXIT
  echo "Acquired lock. No other performance test is running."
else
  echo "Another performance test is running. Giving up."
  exit 1
fi

cd canton

# load nix env
eval "$(direnv export bash)"

# needed so that bail-out-on-unrelated-processes.sh can send a slack message
. scripts/canton-testing/util/load-settings.sh scripts/canton-testing/canton-testing.env
scripts/canton-testing/util/bail-out-on-unrelated-processes.sh

# setting the branch here, because load-settings.sh sets BRANCH as well and therefore would
# overwrite the branch passed in as an argument
BRANCH="${1:-main}"

git fetch --all

if [[ -e $LAST_REV_FILE ]]; then
  NEXT_REV="$(git log --pretty=%H "$(cat "$LAST_REV_FILE")"..origin/"$BRANCH" | tail -1)"

  if [[ -z $NEXT_REV ]]; then
    echo
    echo "***** Last revision has already been tested. Exiting."
    cat "$LAST_REV_FILE"
    exit 0
  fi
else
  echo
  echo "***** No last revision stored. Choosing latest revision."
  NEXT_REV="$(git log -1 --pretty=%H origin/"$BRANCH")"
fi

echo
echo "***** Checking out $BRANCH:$NEXT_REV ..."
sudo git clean -xfd # sudo to also delete files if sbt has accidentally been run as root
git reset --hard
git checkout "$BRANCH"
git reset --hard "$NEXT_REV"

echo
echo "***** Testing the following version:"
git log -1

FAILURES_FILE="../failures"
MAX_FAILURES=1
if scripts/canton-testing/test-after-main-merge.sh scripts/canton-testing/canton-testing.env; then
  cat > "$LAST_REV_FILE" <<<"$NEXT_REV"
  rm "$FAILURES_FILE"
else
  if [[ -e $FAILURES_FILE ]]; then
    NFAILURES=$(( $(cat "$FAILURES_FILE") + 1 ))
  else
    NFAILURES=1
  fi

  echo
  echo "***** Number of failures is $NFAILURES"

  if [[ $NFAILURES -lt $MAX_FAILURES ]]; then
    echo "Storing number of failures."
    cat > "$FAILURES_FILE" <<<$NFAILURES
  else
    echo "Giving up on this revision."
    cat > "$LAST_REV_FILE" <<<"$NEXT_REV"
    rm -f "$FAILURES_FILE" # if $MAX_FAILURES is 1, the file does not exist
  fi
fi
