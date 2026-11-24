#!/usr/bin/env bash
# No ERR trap; we handle failures manually so we can retry.
set -u  # keep -e OFF so a failing probe doesn't kill the loop
set -o pipefail

# Deployment smoke phases. Phases 1-3 are skipped when their env is unset:
#   0  admin API reports a healthy synchronizer connection
#   1  ledger API accepts the minted token and rejects unauthenticated calls
#   2  ping a party hosted on participant 1 via the admin PingService (generates ledger activity)
#   3  PQS projects the bootstrap's seed Iou as an active contract

# ---- config -----------------------------------------------------------------
# Phase 0 (existing admin probe)
ADMIN_HOSTPORT="${HOSTPORT:-participant:5002}"
CONN_SERVICE="${SERVICE:-com.digitalasset.canton.admin.participant.v30.SynchronizerConnectivityService}"
CONN_METHOD="${METHOD:-ListConnectedSynchronizers}"
TIMEOUT="${TIMEOUT:-180}"
INTERVAL="${INTERVAL:-3}"

# Phase 1-2 (ledger API + auth)
LEDGER_HOSTPORT="${LEDGER_HOSTPORT:-}"          # e.g. participant:5001, empty means skip
AUTH_TOKEN_URL="${AUTH_TOKEN_URL:-}"            # e.g. http://auth:8080/token

# Phase 3 (PQS visibility)
PQS_PGHOST="${PQS_PGHOST:-}"                     # empty => skip PQS assertion
PQS_PGPORT="${PQS_PGPORT:-5432}"
PQS_PGUSER="${PQS_PGUSER:-cnadmin}"
PQS_PGPASSWORD="${PQS_PGPASSWORD:-cnadmin}"
PQS_PGDATABASE="${PQS_PGDATABASE:-pqs}"
PQS_TIMEOUT="${PQS_TIMEOUT:-180}"
# Template of the contract participant-bootstrap.sc seeds for PQS.
PQS_SEED_TEMPLATE="Iou:Iou"

log() { printf '%s %s\n' "$(date +'%H:%M:%S')" "$*"; }
hr() { printf '=%.0s' {1..72}; printf '\n'; }

# ---- helpers ----------------------------------------------------------------
get_token() { curl -fsS "$AUTH_TOKEN_URL" || true; }

# grpcurl with reflection. Extra args (headers, data) passed through "$@".
gc() { grpcurl -plaintext -format json "$@"; }

# ---- Phase 0: admin connectivity (backward compatible) ----------------------
phase0_admin_connectivity() {
  hr; log "Phase 0: admin connectivity on ${ADMIN_HOSTPORT} (${CONN_METHOD})"
  local elapsed=0 output="" rc
  while [ "$elapsed" -lt "$TIMEOUT" ]; do
    if command -v nc >/dev/null 2>&1; then
      nc -vz -w 1 "${ADMIN_HOSTPORT%:*}" "${ADMIN_HOSTPORT#*:}" >/dev/null 2>&1 \
        || log "  tcp probe: ${ADMIN_HOSTPORT} not ready yet"
    fi
    output="$(gc -d '{}' "${ADMIN_HOSTPORT}" "${CONN_SERVICE}.${CONN_METHOD}" 2>&1)"; rc=$?
    if [ $rc -eq 0 ] && printf '%s' "$output" \
        | jq -e 'any(.connectedSynchronizers[]?; .healthy == true)' >/dev/null 2>&1; then
      log "  PASS: participant reports a healthy synchronizer connection"
      return 0
    fi
    sleep "$INTERVAL"; elapsed=$((elapsed + INTERVAL))
  done
  log "  FAIL: no healthy synchronizer within ${TIMEOUT}s, last response:"
  printf '%s\n' "$output" | sed 's/^/    /'
  return 1
}

# ---- Phase 1: ledger API auth (jwt-jwks) ------------------------------------
phase1_ledger_auth() {
  [ -n "$LEDGER_HOSTPORT" ] || { log "Phase 1: skipped (LEDGER_HOSTPORT unset)"; return 0; }
  hr; log "Phase 1: ledger API auth on ${LEDGER_HOSTPORT}"

  # Wait for the ledger API to answer a PUBLIC call first (no auth needed).
  local elapsed=0
  until gc -d '{}' "${LEDGER_HOSTPORT}" \
      com.daml.ledger.api.v2.VersionService.GetLedgerApiVersion >/dev/null 2>&1; do
    if [ "$elapsed" -ge "$TIMEOUT" ]; then
      log "  FAIL: ledger API not ready within ${TIMEOUT}s"; return 1
    fi
    sleep "$INTERVAL"; elapsed=$((elapsed + INTERVAL))
  done

  local token; token="$(get_token)"
  if [ -z "$token" ]; then log "  FAIL: could not obtain auth token"; return 1; fi
  log "  obtained token (${#token} chars)"

  # Authenticated admin-scoped call must SUCCEED with the minted token.
  local out rc
  out="$(gc -H "authorization: Bearer ${token}" -d '{}' "${LEDGER_HOSTPORT}" \
         com.daml.ledger.api.v2.admin.PackageManagementService.ListKnownPackages 2>&1)"; rc=$?
  if [ $rc -ne 0 ]; then
    log "  FAIL: authenticated ListKnownPackages rejected (rc=$rc)"; printf '%s\n' "$out" | sed 's/^/    /'
    return 1
  fi
  log "  PASS: minted token accepted on the ledger API"

  # The same call WITHOUT a token must be rejected as UNAUTHENTICATED, proving auth is enforced.
  out="$(gc -d '{}' "${LEDGER_HOSTPORT}" \
         com.daml.ledger.api.v2.admin.PackageManagementService.ListKnownPackages 2>&1)"; rc=$?
  if [ $rc -eq 0 ]; then
    log "  FAIL: unauthenticated call was accepted (auth not enforced)"; return 1
  fi
  if ! printf '%s' "$out" | grep -q 'Code: Unauthenticated'; then
    log "  FAIL: unauthenticated call failed with an unexpected error (rc=$rc)"
    printf '%s\n' "$out" | sed 's/^/    /'
    return 1
  fi
  log "  PASS: unauthenticated call rejected with UNAUTHENTICATED"
  return 0
}

# ---- Phase 2: drive ledger activity -----------------------------------------
phase2_ledger_activity() {
  [ -n "$LEDGER_HOSTPORT" ] || { log "Phase 2: skipped (LEDGER_HOSTPORT unset)"; return 0; }
  hr; log "Phase 2: drive ledger activity (self-ping)"
  local token; token="$(get_token)"
  [ -n "$token" ] || { log "  FAIL: no token for party discovery"; return 1; }

  # Pick a party this participant hosts. grpcurl may print snake_case or camelCase.
  local parties party rc
  parties="$(gc -H "authorization: Bearer ${token}" -d '{}' "${LEDGER_HOSTPORT}" \
             com.daml.ledger.api.v2.admin.PartyManagementService.ListKnownParties 2>&1)"; rc=$?
  if [ $rc -ne 0 ]; then
    log "  FAIL: authenticated ListKnownParties failed (rc=$rc)"
    printf '%s\n' "$parties" | sed 's/^/    /'
    return 1
  fi
  party="$(printf '%s' "$parties" | jq -r '
    (.partyDetails // .party_details // [])
    | map(select(.isLocal == true or .is_local == true))
    | (.[0].party // empty)' 2>/dev/null)"
  if [ -z "$party" ] || [ "$party" = "null" ]; then
    log "  WARN: could not discover a local party, relying on bootstrap ping activity"
    printf '%s\n' "$parties" | sed 's/^/    /'
    return 0
  fi
  log "  discovered local party ${party}"

  # Ping/Pong contracts show up on the stream PQS consumes.
  local out
  out="$(gc -d "{\"target_parties\":[\"${party}\"],\"timeout\":\"60s\"}" "${ADMIN_HOSTPORT}" \
         com.digitalasset.canton.admin.participant.v30.PingService.Ping 2>&1)"; rc=$?
  if [ $rc -eq 0 ] && printf '%s' "$out" | jq -e '.success' >/dev/null 2>&1; then
    log "  PASS: self-ping succeeded, ledger activity generated"
    return 0
  fi
  # Not fatal: the bootstrap ping already produced activity.
  log "  WARN: explicit ping did not confirm success (rc=$rc), continuing"
  printf '%s\n' "$out" | sed 's/^/    /'
  return 0
}

# ---- Phase 3: PQS visibility oracle -----------------------------------------
psql_q() {
  PGPASSWORD="$PQS_PGPASSWORD" psql -h "$PQS_PGHOST" -p "$PQS_PGPORT" -U "$PQS_PGUSER" \
    -d "$PQS_PGDATABASE" -tAc "$1" 2>&1
}

phase3_pqs_visibility() {
  [ -n "$PQS_PGHOST" ] || { log "Phase 3: skipped (PQS_PGHOST unset)"; return 0; }
  hr; log "Phase 3: active ${PQS_SEED_TEMPLATE} in PQS ${PQS_PGDATABASE}@${PQS_PGHOST}:${PQS_PGPORT}"
  local elapsed=0 out
  while [ "$elapsed" -lt "$PQS_TIMEOUT" ]; do
    # Errors until PQS has migrated its schema and ingested the DAR.
    out="$(psql_q "select count(*) from active('${PQS_SEED_TEMPLATE}');")"
    if [ "$out" -gt 0 ] 2>/dev/null; then
      log "  PASS: PQS shows ${out} active ${PQS_SEED_TEMPLATE} contract(s)"
      return 0
    fi
    sleep "$INTERVAL"; elapsed=$((elapsed + INTERVAL))
  done
  log "  FAIL: no active ${PQS_SEED_TEMPLATE} in PQS within ${PQS_TIMEOUT}s, last psql output:"
  printf '%s\n' "$out" | sed 's/^/    /'
  return 1
}

# ---- run --------------------------------------------------------------------
log "release_suffix=${RELEASE_SUFFIX:-unknown}"
grpcurl -version 2>/dev/null || true

rc=0
phase0_admin_connectivity || rc=1
[ $rc -eq 0 ] && { phase1_ledger_auth   || rc=1; }
[ $rc -eq 0 ] && { phase2_ledger_activity || rc=1; }
[ $rc -eq 0 ] && { phase3_pqs_visibility || rc=1; }

hr
if [ $rc -eq 0 ]; then
  echo "TEST_RESULT=PASS all smoke phases passed"
else
  echo "TEST_RESULT=FAIL see phase output above"
fi
exit $rc
