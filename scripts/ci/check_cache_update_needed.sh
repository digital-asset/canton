#!/bin/bash
set -euo pipefail
# Checks whether the cache is up to date
# by comparing the current hash with the hash
# encoded in the matched cache key and assign
# variable cache-up-to-date either true or false

CURRENT_HASH="$1"
CACHE_HIT="$2"
MATCHED_KEY="$3"

if [[ "$CACHE_HIT" == "true" ]]; then
  echo "cache-up-to-date=true" >> "$GITHUB_OUTPUT"
  echo "reason=exact-match"
  exit 0
fi

if [[ -z "$MATCHED_KEY" ]]; then
  echo "cache-up-to-date=false" >> "$GITHUB_OUTPUT"
  echo "reason=no-cache"
  exit 0
fi

MATCHED_HASH=$(echo "$MATCHED_KEY" | sed -E 's/.*hash=([^-]+)-sha=[^-]+$/\1/')

if [[ "$MATCHED_HASH" == "$CURRENT_HASH" ]]; then
  echo "cache-up-to-date=true" >> "$GITHUB_OUTPUT"
  echo "reason=same-hash:$MATCHED_HASH"
else
  echo "cache-up-to-date=false" >> "$GITHUB_OUTPUT"
  echo "reason=hash-diff:$MATCHED_HASH!=$CURRENT_HASH"
fi
