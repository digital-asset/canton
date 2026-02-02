#!/usr/bin/env bash
set -euo pipefail

# Where the Dockerfile expects them
TARGET_DIR="images/canton-base"

# ────────────────────────────────────────────────────────────
# 1. Source locations (relative to docker/canton)
# ────────────────────────────────────────────────────────────
LOGBACK_SRC="../../community/app/src/main/resources/logback.xml"
LICENSE_SRC="../../LICENSE.txt"   # stays where it is (just sanity‑check)

# ────────────────────────────────────────────────────────────
# 2. Sanity‑check all inputs exist
# ────────────────────────────────────────────────────────────

[[ -f "$LOGBACK_SRC" ]] || { echo "❌ Missing $LOGBACK_SRC" >&2; exit 1; }
[[ -f "$LICENSE_SRC"  ]] || { echo "❌ Missing $LICENSE_SRC" >&2;  exit 1; }

# ────────────────────────────────────────────────────────────
# 3. Copy everything into target/
# ────────────────────────────────────────────────────────────
mkdir -p "$TARGET_DIR"


cp "$LOGBACK_SRC" "${TARGET_DIR}/logback.xml"
cp "$LICENSE_SRC" "${TARGET_DIR}/LICENSE.txt"   # add this line

echo "✅ Dependencies copied into ${TARGET_DIR}/"
