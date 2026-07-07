#!/usr/bin/env bash
set -euo pipefail

TARGET="community/app/target/release"
mkdir -p "$TARGET"

shopt -s nullglob
archives=($TMP_WORKSPACE/community-release/*.tar.gz)
if [[ ${#archives[@]} -eq 0 ]]; then
  echo "No packaged community release archives found in $TMP_WORKSPACE/community-release"
  exit 1
fi

for archive in "${archives[@]}"; do
  echo "Extracting $archive into $TARGET"
  tar xzf "$archive" -C "$TARGET"
done

# The packaged release directory is versioned (for example canton-open-source-<version>).
# docs-open expects a stable path: community/app/target/release/canton/bin/canton.
if [[ ! -x "$TARGET/canton/bin/canton" ]]; then
  candidates=("$TARGET"/*/bin/canton)
  if [[ ${#candidates[@]} -eq 1 ]]; then
    candidate_bin="${candidates[0]}"
    candidate_dir="$(dirname "$(dirname "$candidate_bin")")"
    echo "Using release directory: $candidate_dir"
    ln -sfn "$(basename "$candidate_dir")" "$TARGET/canton"
  fi
fi

if [[ ! -x "$TARGET/canton/bin/canton" ]]; then
  echo "Expected executable missing: $TARGET/canton/bin/canton"
  find "$TARGET" -maxdepth 4 -type f | head -n 100
  exit 1
fi
