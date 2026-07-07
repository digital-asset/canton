#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Dumps debug.sequencer_events to stdout in a human-readable format.
#

set -euo pipefail

usage() {
    echo "Usage: $0 -h db_host -p db_port -d db_name -U db_user [-P password] [-F min_timestamp] [-T max_timestamp]" >&2
    echo "Dumps debug.sequencer_events to stdout." >&2
    echo "" >&2
    echo "  Timestamps should be in Canton format, e.g. 2025-01-01T00:00:00.000000Z" >&2
    exit 1
}

db_host=""
db_port=""
db_name=""
db_user=""
ts_min=""
ts_max=""

while getopts "h:p:d:U:P:F:T:" opt; do
    case "${opt}" in
        h) db_host="${OPTARG}" ;;
        p) db_port="${OPTARG}" ;;
        d) db_name="${OPTARG}" ;;
        U) db_user="${OPTARG}" ;;
        P) export PGPASSWORD="${OPTARG}" ;;
        F) ts_min="${OPTARG}" ;;
        T) ts_max="${OPTARG}" ;;
        *) usage ;;
    esac
done

# Validate required parameters
missing=()
[[ -z "${db_host}" ]] && missing+=("-h host")
[[ -z "${db_port}" ]] && missing+=("-p port")
[[ -z "${db_name}" ]] && missing+=("-d database")
[[ -z "${db_user}" ]] && missing+=("-U user")

if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Error: missing required parameters: ${missing[*]}" >&2
    echo >&2
    usage
fi

# Build optional WHERE clause for timestamp filtering
where=""
if [[ -n "${ts_min}" && -n "${ts_max}" ]]; then
    where="WHERE ts >= '${ts_min}' AND ts <= '${ts_max}'"
elif [[ -n "${ts_min}" ]]; then
    where="WHERE ts >= '${ts_min}'"
elif [[ -n "${ts_max}" ]]; then
    where="WHERE ts <= '${ts_max}'"
fi

psql -h "${db_host}" -p "${db_port}" -d "${db_name}" -U "${db_user}" --no-password --csv -c "
SELECT
    ts as timestamp,
    node_index,
    event_type,
    message_id,
    sender,
    recipients,
    payload_id,
    topology_timestamp,
    consumed_cost,
    extra_traffic_consumed,
    base_traffic_remainder,
    error
FROM debug.sequencer_events
${where}
ORDER BY ts ASC;
"

