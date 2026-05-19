#!/usr/bin/env bash
set -euo pipefail

# --- args & env ---------------------------------------------------------------
release_suffix="${1:-local}"   # default to :local if not provided
oci_path=${OCI_PATH:-"local/da-images/public-unstable/docker/"}


export RELEASE_SUFFIX="$release_suffix"
export OCI_PATH="$oci_path"
# Where to dump logs if the test fails
LOG_DIR="ci-artifacts/compose-logs"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_WARNINGS_TO_IGNORE="$SCRIPT_DIR/log-warnings-to-ignore.txt"

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

check_service_logs() {
  local found=0
  local scanned=0

  for service in participant sequencer mediator synchronizer; do
    local log="$LOG_DIR/$service.log"
    [[ -f "$log" ]] || continue
    scanned=$((scanned + 1))

    local problems
    local _ignore
    _ignore=$(rg -v '^\s*(#|$)' "$LOG_WARNINGS_TO_IGNORE" 2>/dev/null || true)
    if [[ -n "$_ignore" ]]; then
      problems=$(rg '"level":"(WARN|ERROR)"' "$log" \
                 | rg -v -f <(printf '%s\n' "$_ignore") || true)
    else
      problems=$(rg '"level":"(WARN|ERROR)"' "$log" || true)
    fi

    if [[ -n "$problems" ]]; then
      echo "⚠️  Unexpected WARN/ERROR in $service:"
      echo "$problems"
      found=1
    fi
  done

  if (( scanned == 0 )); then
    echo "❌ check_service_logs: no service log files found under $LOG_DIR" >&2
    return 1
  fi

  return $found
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

# Always collect logs so check_service_logs can scan them
collect_logs

if [[ $rc -ne 0 ]]; then
  echo "❌ Test container failed (exit code $rc)."
  exit $rc
fi
echo "✅ Test container passed."

echo "🔍 Checking service logs for unexpected WARN/ERROR..."
if ! check_service_logs; then
  echo "❌ Unexpected log warnings found; see above."
  exit 1
fi
echo "✅ No unexpected log warnings."
