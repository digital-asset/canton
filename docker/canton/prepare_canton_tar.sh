#!/usr/bin/env bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Usage: ./prepare_canton_tar.sh
set -euo pipefail

patterns=( "canton-open-source-*.tar.gz" )

# ── 2. Directories to search ────────────────────────────────────────
search_roots=()

# Local dev build
local_dir="../../community/app/target/release"
[[ -d "$local_dir" ]] && search_roots+=("$local_dir")

# CircleCI workspace
[[ -d "/tmp/workspace" ]] && search_roots+=("/tmp/workspace")

# Manual override for debugging
[[ -n "${CANTON_ARTEFACT_DIR:-}" ]] && search_roots=("$CANTON_ARTEFACT_DIR" "${search_roots[@]}")

# De‑dup roots
declare -A seen; unique_roots=()
for d in "${search_roots[@]}"; do
  [[ -n "${seen[$d]:-}" ]] || { unique_roots+=("$d"); seen[$d]=1; }
done

# ── 3. Locate exactly one artefact ──────────────────────────────────
matches=()
for root in "${unique_roots[@]}"; do
  for pat in "${patterns[@]}"; do
    while IFS= read -r -d '' f; do matches+=("$f"); done \
      < <(find "$root" -type f -name "$pat" ! -name '*-protobuf.tar.gz' -print0 2>/dev/null)
  done
done

if (( ${#matches[@]} == 0 )); then
  echo "❌ No artefact matching (${patterns[*]}) in (${unique_roots[*]})" >&2
  exit 1
elif (( ${#matches[@]} > 1 )); then
  echo "❌ Multiple artifacts found; please clean or specify one:" >&2
  printf '  %s\n' "${matches[@]}" >&2
  exit 1
fi

src="${matches[0]}"
echo "✅ Found artefact: $src"

# ── 4. Decompress into target/canton.tar ───────────────────────────
TARGET_DIR="images/canton-base/target/canton"
rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"

echo "🔄 Extracting (stripping canton‑* root)…"
tar --strip-components=1 -xzf "$src" -C "$TARGET_DIR"
rm -r ${TARGET_DIR:?}/config \
  ${TARGET_DIR:?}/daml \
  ${TARGET_DIR:?}/dars \
  ${TARGET_DIR:?}/examples \
  ${TARGET_DIR:?}/scripts
echo "✅ Extracted contents to $TARGET_DIR"

# The Dockerfile can now just:
#   COPY target/canton/ /opt/canton/
