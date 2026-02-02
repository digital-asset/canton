#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Reads OS related metrics from $TELEGRAF_METRICS_FILES and
# writes reports to $DATADOG_METRICS_FILE and $SLACK_METRICS_FILE.
# Assumes the variable bindings from extract-throughput-metrics.sh
###############################################################################

echo
echo "***** Extracting OS metrics from $TELEGRAF_METRICS_FILES..."

if [[ -z $TELEGRAF_METRICS_FILES ]] ; then
  echo "Unable to find OS metrics at '$TELEGRAF_METRICS_FILES'. Skipping..."
  exit 0
fi

# This machinery is required to allow the 'query-metrics' function to terminate this script,
# even if invoked through a subshell $(...).
trap "exit 1" TERM
export TOP_PID=$$

### Helpers for accessing metrics JSON files

# A metric report for a given TS will be the latest report between TS - TS_WINDOW and TS.
TS_WINDOW=90

# Prefetch relevant metrics data so that we have to read the big metrics files only once.
# shellcheck disable=SC2086
FILTERED_METRICS_DATA="$(jq --slurp "
  map(select(
    ($((JOB_START_TS - TS_WINDOW)) <= .timestamp and .timestamp <= $JOB_START_TS) or
    ($((EARLY_TS - TS_WINDOW)) <= .timestamp and .timestamp <= $EARLY_TS) or
    ($((LATE_TS - TS_WINDOW)) <= .timestamp and .timestamp <= $LATE_TS)
  ))
  " $TELEGRAF_METRICS_FILES)"

query-metrics() {
  RESULT="$(jq "$1" <<<"$FILTERED_METRICS_DATA")"

  if [[ $RESULT == "null" ]] || [[ -z $RESULT ]]; then
    echo "Missing telegraf data. Query: $1" >&2
    kill -s TERM "$TOP_PID"
  else
    echo "$RESULT"
  fi
}

resource-usage-at() {
  local UPPER_TS="$1"
  local LOWER_TS="$((UPPER_TS - TS_WINDOW))"

  query-metrics "
    map(select($LOWER_TS <= .timestamp and .timestamp <= $UPPER_TS and $2 and .fields.$3 != null)) |
    max_by(.timestamp) |
    .fields.$3"
}

summed-resource-usage-at() {
  local UPPER_TS="$1"
  local LOWER_TS="$((UPPER_TS - TS_WINDOW))"

  query-metrics "
    map(select($LOWER_TS <= .timestamp and .timestamp <= $UPPER_TS and $2 and .fields.$3 != null)) |
    group_by(.timestamp) |
    max_by(.[0].timestamp) |
    map(.fields.$3) |
    add"
}

### Disk usage per transaction

disk-usage-at() {
  summed-resource-usage-at "$1" '.name == "disk"' used
}

disk-usage-per-tx() {
  EARLY="$(disk-usage-at "$EARLY_TS")"
  LATE="$(disk-usage-at "$LATE_TS")"
  rate $((LATE - EARLY)) "$TX_EARLY_TO_LATE_COUNT"
}

DISK_USAGE_PER_TX="$(disk-usage-per-tx)"

cat >> "$DATADOG_METRICS_FILE" <<EOI
$DATADOG_METRIC_PREFIX.disk.usage-per-tx=$DISK_USAGE_PER_TX
EOI

cat >> "$SLACK_METRICS_FILE" <<EOI
Disk usage: $(print-bytes "$DISK_USAGE_PER_TX")/root node
EOI

### Disk io per transaction

diskio-at() {
  summed-resource-usage-at "$1" '.name == "diskio"' "$2"
}

diskio-per-tx() {
  EARLY="$(diskio-at "$EARLY_TS" "$1")"
  LATE="$(diskio-at "$LATE_TS" "$1")"
  rate $((LATE - EARLY)) "$TX_EARLY_TO_LATE_COUNT"
}

DISK_READ_BYTES_PER_TX="$(diskio-per-tx read_bytes)"
DISK_WRITE_BYTES_PER_TX="$(diskio-per-tx write_bytes)"

cat >> "$DATADOG_METRICS_FILE" <<EOI
$DATADOG_METRIC_PREFIX.disk.read-bytes-per-tx=$DISK_READ_BYTES_PER_TX
$DATADOG_METRIC_PREFIX.disk.write-bytes-per-tx=$DISK_WRITE_BYTES_PER_TX
EOI

cat >> "$SLACK_METRICS_FILE" <<EOI
Disk io: $(print-bytes "$DISK_READ_BYTES_PER_TX") read/root node, $(print-bytes "$DISK_WRITE_BYTES_PER_TX") written/root node
EOI

### Network io per transaction

networkio-at() {
  resource-usage-at "$1" ".name == \"net\" and .tags.interface == \"$2\"" "$3"
}

networkio-per-tx() {
  EARLY="$(networkio-at "$EARLY_TS" "$@")"
  LATE="$(networkio-at "$LATE_TS" "$@")"
  rate $((LATE - EARLY)) "$TX_EARLY_TO_LATE_COUNT"
}

P1_LEDGER_API_BYTES_RECV_PER_TX="$(networkio-per-tx p1-ledger-api bytes_recv)"
P1_LEDGER_API_BYTES_SENT_PER_TX="$(networkio-per-tx p1-ledger-api bytes_sent)"
PARTICIPANT_POSTGRES_BYTES_RECV_PER_TX="$(networkio-per-tx par-postgres bytes_recv)"
PARTICIPANT_POSTGRES_BYTES_SENT_PER_TX="$(networkio-per-tx par-postgres bytes_sent)"
DA_PUBLIC_API_BYTES_RECV_PER_TX="$(networkio-per-tx da-public-api bytes_recv)"
DA_PUBLIC_API_BYTES_SENT_PER_TX="$(networkio-per-tx da-public-api bytes_sent)"
SYNCHRONIZER_POSTGRES_BYTES_RECV_PER_TX="$(networkio-per-tx sync-postgres bytes_recv)"
SYNCHRONIZER_POSTGRES_BYTES_SENT_PER_TX="$(networkio-per-tx sync-postgres bytes_sent)"

cat >> "$DATADOG_METRICS_FILE" <<EOI
$DATADOG_METRIC_PREFIX.network.p1-ledger-api.bytes-recv-per-tx=$P1_LEDGER_API_BYTES_RECV_PER_TX
$DATADOG_METRIC_PREFIX.network.p1-ledger-api.bytes-sent-per-tx=$P1_LEDGER_API_BYTES_SENT_PER_TX
$DATADOG_METRIC_PREFIX.network.participants-postgres.bytes-recv-per-tx=$PARTICIPANT_POSTGRES_BYTES_RECV_PER_TX
$DATADOG_METRIC_PREFIX.network.participants-postgres.bytes-sent-per-tx=$PARTICIPANT_POSTGRES_BYTES_SENT_PER_TX
$DATADOG_METRIC_PREFIX.network.da-public-api.bytes-recv-per-tx=$DA_PUBLIC_API_BYTES_RECV_PER_TX
$DATADOG_METRIC_PREFIX.network.da-public-api.bytes-sent-per-tx=$DA_PUBLIC_API_BYTES_SENT_PER_TX
$DATADOG_METRIC_PREFIX.network.synchronizers-postgres.bytes-recv-per-tx=$SYNCHRONIZER_POSTGRES_BYTES_RECV_PER_TX
$DATADOG_METRIC_PREFIX.network.synchronizers-postgres.bytes-sent-per-tx=$SYNCHRONIZER_POSTGRES_BYTES_SENT_PER_TX
EOI

cat >> "$SLACK_METRICS_FILE" <<EOI
Network io:
- Ledger api: $(print-bytes "$P1_LEDGER_API_BYTES_RECV_PER_TX") received/root node, $(print-bytes "$P1_LEDGER_API_BYTES_SENT_PER_TX") sent/root node
- Public api: $(print-bytes "$DA_PUBLIC_API_BYTES_RECV_PER_TX") received/root node, $(print-bytes "$DA_PUBLIC_API_BYTES_SENT_PER_TX") sent/root node
- Participant database connections: $(print-bytes "$PARTICIPANT_POSTGRES_BYTES_RECV_PER_TX") received/root node, $(print-bytes "$PARTICIPANT_POSTGRES_BYTES_SENT_PER_TX") sent/root node
- Synchronizer database connections: $(print-bytes "$SYNCHRONIZER_POSTGRES_BYTES_RECV_PER_TX") received/root node, $(print-bytes "$SYNCHRONIZER_POSTGRES_BYTES_SENT_PER_TX") sent/root node

EOI

### CPU

cpu-usage-at() {
  resource-usage-at "$1" '.name == "cpu" and .tags.cpu == "cpu-total"' usage_user_mean
}

CPU_USAGE="$(cpu-usage-at "$LATE_TS")"

cat >> "$DATADOG_METRICS_FILE" <<EOI
$DATADOG_METRIC_PREFIX.cpu-usage=$CPU_USAGE
EOI

CPU_USAGE_INT=${CPU_USAGE%.*}

cat >> "$SLACK_METRICS_FILE" <<EOI
CPU usage: $CPU_USAGE_INT%
EOI

### Memory

memory-usage-at() {
  used=$(resource-usage-at "$1" '.name == "mem"' used)
  shared=$(resource-usage-at "$1" '.name == "mem"' shared)
  echo $((used + shared))
}

MEMORY_USAGE_BEGINNING="$(memory-usage-at "$JOB_START_TS")"
MEMORY_USAGE_TOTAL="$(memory-usage-at "$LATE_TS")"
MEMORY_USAGE=$((MEMORY_USAGE_TOTAL - MEMORY_USAGE_BEGINNING))

cat >> "$DATADOG_METRICS_FILE" <<EOI
$DATADOG_METRIC_PREFIX.memory-usage=$MEMORY_USAGE
EOI

cat >> "$SLACK_METRICS_FILE" <<EOI
Memory usage: $(print-bytes "$MEMORY_USAGE")
EOI

### Threads

threads-at() {
  resource-usage-at "$1" '.name == "processes"' total_threads
}

THREADS_BEGINNING="$(threads-at "$JOB_START_TS")"
THREADS_TOTAL="$(threads-at "$LATE_TS")"
THREADS=$((THREADS_TOTAL - THREADS_BEGINNING))

cat >> "$DATADOG_METRICS_FILE" <<EOI
$DATADOG_METRIC_PREFIX.threads=$THREADS
EOI

cat >> "$SLACK_METRICS_FILE" <<EOI
Number of threads: $THREADS

EOI
