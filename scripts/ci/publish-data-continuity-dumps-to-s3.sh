#!/usr/bin/env bash
set -eo pipefail

ABSDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# shellcheck source=./common.sh
source "$ABSDIR/common.sh"

if [[ -z "$RELEASE_SUFFIX" ]]; then
    # the suffix gets defined by a previous step. we just reuse it here.
    err "ERROR, NO RELEASE_SUFFIX DEFINED! You need to run this job after the obtain_release_suffix step."
    exit 1
fi

# We don't publish "SNAPSHOT" releases, only properly dated snapshots or full releases.
if [[ "$RELEASE_SUFFIX" == *"-SNAPSHOT" ]]; then
  info "Skip publishing of snapshot release"
  exit 0
fi


if [ -x "$(command -v aws)" ]; then
  aws --version
else
  echo "Missing tool required: aws"
  exit 1
fi

# We expect /tmp/canton/data-continuity-dumps to contain a single directory with the dumps
# To handle the case of nothing generated, we generalize the logic to handle multiple/none directories
DUMP_DIRS=$(ls /tmp/canton/data-continuity-dumps)
info "Found data continuity dumps: $DUMP_DIRS"

for DUMP_DIR in $DUMP_DIRS; do
  # The "sync" command will only update and copy new files to the destination.
  # Existing files in the destination that are not in the source will NOT be deleted.
  # This allows both accumulating dumps from parallel executors and updating existing dumps.
  # We use RELEASE_SUFFIX as a destination to keep the history of dumps for snapshot releases.
  info "Syncing data continuity dumps to s3 for $DUMP_DIR"
  run "Sync data continuity dumps to s3" aws s3 sync "/tmp/canton/data-continuity-dumps/$DUMP_DIR" "s3://canton-public-releases/data-continuity-dumps/$RELEASE_SUFFIX"
done
