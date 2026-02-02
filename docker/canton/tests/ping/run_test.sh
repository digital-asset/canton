#!/usr/bin/env bash
set -euo pipefail

# --- args & env ---------------------------------------------------------------
release_suffix="${1:-local}"   # default to :local if not provided
oci_path=${OCI_PATH:-"local/da-images/public-unstable/docker/"}


export RELEASE_SUFFIX="$release_suffix"
export OCI_PATH="$oci_path"
# Where to dump logs if the test fails
LOG_DIR="ci-artifacts/compose-logs"

# --- helpers -----------------------------------------------------------------
collect_logs() {
  mkdir -p "$LOG_DIR"
  echo "📥 Collecting docker compose diagnostics to $LOG_DIR"

  # ps (json), full stack logs
  docker compose ps --format json     > "$LOG_DIR/ps.json"          || true
  docker compose logs --no-color --timestamps > "$LOG_DIR/stack.log" || true

  # Per-service logs (helps when one chatty service drowns the rest)
  for s in $(docker compose config --services 2>/dev/null || true); do
    docker compose logs --no-color --timestamps "$s" > "$LOG_DIR/$s.log" || true
  done

  # Capture the resolved compose config for debugging
  docker compose config > "$LOG_DIR/resolved-compose.yml" 2>/dev/null || true
}

down_stack() {
  echo "🧹 Cleaning up docker compose..."
  docker compose down -v || true
}

# Ensure we clean up the stack even on Ctrl-C etc.
trap 'down_stack' EXIT

# --- run ---------------------------------------------------------------------
echo "▶️ Building & starting stack (RELEASE_SUFFIX=$RELEASE_SUFFIX)..."
set +e
docker compose up --build --abort-on-container-exit --exit-code-from test test
rc=$?
set -e

if [[ $rc -ne 0 ]]; then
  echo "❌ Test container failed (exit code $rc)."
  collect_logs
else
  echo "✅ Test container passed."
fi

exit $rc
