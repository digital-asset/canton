#!/usr/bin/env bash
# No ERR trap; we handle failures manually so we can retry.
set -u  # keep -e OFF so a failing grpcurl doesn't kill the loop
set -o pipefail

echo "release_suffix=${RELEASE_SUFFIX:-unknown}"
echo "Testing gRPC connectivity to ${HOSTPORT:-participant:5001} method ${SERVICE}.${METHOD}"
echo "Timeout=${TIMEOUT:-60}s, Interval=${INTERVAL:-1}s"

echo "--- env ----"
echo "HOSTPORT=${HOSTPORT:-unset}"
echo "SERVICE=${SERVICE:-unset}"
echo "METHOD=${METHOD:-unset}"
echo "--- system ----"
uname -a || true
echo "arch: $(uname -m)"
echo "--- binaries ----"
command -v grpcurl || true
grpcurl -version || true
command -v jq || true
jq --version || true

timeout="${TIMEOUT:-120}"
interval="${INTERVAL:-1}"
elapsed=0

while [ "$elapsed" -lt "$timeout" ]; do
  ts=$(date +"%Y-%m-%d %H:%M:%S")
  echo "[$ts] attempt ${elapsed}/${timeout}"

  # quick TCP probe first (non-fatal)
  if command -v nc >/dev/null 2>&1; then
    nc -vz -w 1 "${HOSTPORT%:*}" "${HOSTPORT#*:}" 2>&1 || echo "  tcp probe: not ready yet"
  fi

  # Try grpcurl; capture exit + output
  output="$(grpcurl -plaintext -format json -d '{}' "${HOSTPORT}" "${SERVICE}.${METHOD}" 2>&1)"
  rc=$?
  echo "  grpcurl exit=$rc"
  printf '%s\n' "$output" | sed 's/^/    /'

  if [ $rc -eq 0 ]; then
    if printf '%s' "$output" | jq -e 'any(.connectedSynchronizers[]?; .healthy == true)' >/dev/null; then
      echo "TEST_RESULT=PASS ✅ participant reports healthy synchronizer connection"
      exit 0
    else
      echo "  jq: no healthy synchronizers yet"
    fi
  else
    # Highlight common early failure modes
    if echo "$output" | grep -qiE 'connection refused|unavailable|deadline|no such host|i/o timeout'; then
      echo "  note: server likely not ready yet"
    fi
  fi

  sleep "$interval"
  elapsed=$((elapsed + interval))
done

echo "TEST_RESULT=FAIL ❌ participant did not become healthy within ${timeout}s"
exit 1
