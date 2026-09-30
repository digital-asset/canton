#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Reads throughput metrics from $METRICS_DIR and
# writes reports to $DATADOG_METRICS_FILE and $SLACK_METRICS_FILE.
###############################################################################

echo
echo "***** Extracting throughput metrics from $METRICS_DIR..."

# See read-csv-metric.py to understand how these are used.
EARLY_EVENT_PERCENTILE="$1"
LATE_EVENT_PERCENTILE="$2"

### Read metrics csv files

load-metrics() {
	local metric_file="$1"
	local prefix="$2"
	# Note: 'flt' is evaluated as a Python regular expression (re.search).
	# If filtering with literal special regex characters like '.', '?', or '*', keep in mind they are evaluated as regex patterns.
	local flt="${3:-}"
	local calc_time="${4:-}"

	local is_known_missing_metrics="false"

	if [[ "$prefix" == *TX* && "${KNOWN_MISSING_TX_METRICS:-false}" == "true" ]]; then
		is_known_missing_metrics="true"
	elif [[ "$prefix" == *SEQUENCER* && "${KNOWN_MISSING_SEQUENCER_METRICS:-false}" == "true" ]]; then
		is_known_missing_metrics="true"
	elif [[ "$prefix" == *MEDIATOR* && "${KNOWN_MISSING_MEDIATOR_METRICS:-false}" == "true" ]]; then
		is_known_missing_metrics="true"
	elif [[ "$prefix" == *PARTICIPANT* && "${KNOWN_MISSING_PARTICIPANT_METRICS:-false}" == "true" ]]; then
		is_known_missing_metrics="true"
	elif [[ "$prefix" == *FAILED_TRADER* && "${KNOWN_MISSING_FAILED_TRADER_METRICS:-false}" == "true" ]]; then
		is_known_missing_metrics="true"
	fi

	# If file is missing and marked as known missing
	if [[ "$is_known_missing_metrics" == "true" && ! -f "$METRICS_DIR/$metric_file" ]]; then
		echo "[WARN] Metric file $metric_file not found in $METRICS_DIR. Defaulting $prefix metrics to 0."
		eval "${prefix}_EARLY_TS=0 ${prefix}_EARLY_COUNT=0 ${prefix}_LATE_TS=0 ${prefix}_LATE_COUNT=0 ${prefix}_EARLY_TO_LATE_COUNT=0 ${prefix}_EARLY_TO_LATE_TIME=0"
		return 0
	fi

	# file is missing and it is a local dev run (which might be short enough, not to record certain metrics)
	if [[ ! -f "$METRICS_DIR/$metric_file" && "${IS_LOCAL_DEV_RUN:-false}" == "true" ]]; then
		echo "[LOCAL RUN] Metric file $metric_file not found in $METRICS_DIR. Defaulting $prefix metrics to 0."
		eval "${prefix}_EARLY_TS=0 ${prefix}_EARLY_COUNT=0 ${prefix}_LATE_TS=0 ${prefix}_LATE_COUNT=0 ${prefix}_EARLY_TO_LATE_COUNT=0 ${prefix}_EARLY_TO_LATE_TIME=0"
		return 0
	fi

	echo "Loading metrics from $METRICS_DIR/$metric_file ..."

	eval "$(read-csv-metric.py "$METRICS_DIR/$metric_file" "$prefix" "$EARLY_EVENT_PERCENTILE" "$LATE_EVENT_PERCENTILE" "$flt" "$calc_time")"
}

# Setting up metrics file locations and the regex filters for extracting the proper lines out
TX_LOAD_METRICS_FILE="${TX_LOAD_METRICS_FILE:-"participant1.daml.participant.console.tx-nodes-emitted.csv"}"
TX_LOAD_METRICS_FILTER_PATTERN="${TX_LOAD_METRICS_FILTER_PATTERN:-"measurement=canton\.transactions-emitted"}"
UPDATES_LOAD_METRICS_FILE="${UPDATES_LOAD_METRICS_FILE:-"participant1.daml.participant.api.indexer.events.csv"}"
UPDATES_LOAD_METRICS_FILTER_PATTERN="${UPDATES_LOAD_METRICS_FILTER_PATTERN:-"PerformanceTest"}"
PARTICIPANT_EVENTS_METRICS_FILE="${PARTICIPANT_EVENTS_METRICS_FILE:-"participant1.synchronizer.daml.sequencer-client.handler.sequencer-events.csv"}"
MEDIATOR_EVENTS_METRICS_FILE="${MEDIATOR_EVENTS_METRICS_FILE:-"mediator.daml.sequencer-client.handler.sequencer-events.csv"}"
FAILED_TRADER1_METRICS_FILE="${FAILED_TRADER1_METRICS_FILE:-"canton.performance.failed.csv"}"
FAILED_TRADER1_METRICS_FILTER_PATTERN="${FAILED_TRADER1_METRICS_FILTER_PATTERN:-"role=(participant1-trader1|0Trad)"}"

# Note: the third parameter, is a regex string
load-metrics $TX_LOAD_METRICS_FILE TX "$TX_LOAD_METRICS_FILTER_PATTERN"
load-metrics "$UPDATES_LOAD_METRICS_FILE" "UPDATES" "$UPDATES_LOAD_METRICS_FILTER_PATTERN" "calc_test_time"
load-metrics "$PARTICIPANT_EVENTS_METRICS_FILE" PARTICIPANT_EVENTS
load-metrics "$MEDIATOR_EVENTS_METRICS_FILE" MEDIATOR_EVENTS
load-metrics "$FAILED_TRADER1_METRICS_FILE" FAILED_TRADER1 $FAILED_TRADER1_METRICS_FILTER_PATTERN

# Starting point for general throughput measurements
# shellcheck disable=SC2034
EARLY_TS="$PARTICIPANT_EVENTS_EARLY_TS"
if [[ "$EARLY_TS" == "0" || -z "$EARLY_TS" ]]; then
	# UPDATES_EARLY_TS should never be missing
	EARLY_TS="${UPDATES_EARLY_TS:-0}"
fi
# End point for general throughput measurements
# shellcheck disable=SC2034
LATE_TS="$TX_LATE_TS"
if [[ "$LATE_TS" == "0" || -z "$LATE_TS" ]]; then
	# UPDATES_LATE_TS should never be missing
	LATE_TS="${UPDATES_LATE_TS:-$EARLY_TS}"
fi

### Compute throughput

TX_THROUGHPUT="$(rate "$TX_EARLY_TO_LATE_COUNT" "$TX_EARLY_TO_LATE_TIME")"
UPDATES_THROUGHPUT="$(rate "$UPDATES_EARLY_TO_LATE_COUNT" "$UPDATES_EARLY_TO_LATE_TIME")"
PARTICIPANT_EVENTS_THROUGHPUT="$(rate "$PARTICIPANT_EVENTS_EARLY_TO_LATE_COUNT" "$PARTICIPANT_EVENTS_EARLY_TO_LATE_TIME")"
MEDIATOR_EVENTS_THROUGHPUT="$(rate "$MEDIATOR_EVENTS_EARLY_TO_LATE_COUNT" "$MEDIATOR_EVENTS_EARLY_TO_LATE_TIME")"

### Create datadog report

cat >> "$DATADOG_METRICS_FILE" <<EOI
$DATADOG_METRIC_PREFIX.throughput.transactions.avg=$TX_THROUGHPUT
$DATADOG_METRIC_PREFIX.throughput.updates.avg=$UPDATES_THROUGHPUT
$DATADOG_METRIC_PREFIX.throughput.participant-events.avg=$PARTICIPANT_EVENTS_THROUGHPUT
$DATADOG_METRIC_PREFIX.throughput.mediator-events.avg=$MEDIATOR_EVENTS_THROUGHPUT
EOI

### Create slack report

cat >> "$SLACK_METRICS_FILE" <<EOI
*Throughput*
• Transaction service: *$TX_THROUGHPUT root nodes/s* ($TX_EARLY_TO_LATE_COUNT tx root nodes in $TX_EARLY_TO_LATE_TIME s)
• Read service: *$UPDATES_THROUGHPUT updates/s* ($UPDATES_EARLY_TO_LATE_COUNT updates in $UPDATES_EARLY_TO_LATE_TIME s)
• Participant sequencer client: *$PARTICIPANT_EVENTS_THROUGHPUT events/s* ($PARTICIPANT_EVENTS_EARLY_TO_LATE_COUNT events in $PARTICIPANT_EVENTS_EARLY_TO_LATE_TIME s)
• Mediator sequencer client: *$MEDIATOR_EVENTS_THROUGHPUT events/s* ($MEDIATOR_EVENTS_EARLY_TO_LATE_COUNT events in $MEDIATOR_EVENTS_EARLY_TO_LATE_TIME s)
• Failed commands (trader1): *$FAILED_TRADER1_EARLY_TO_LATE_COUNT*

EOI

# JSON summary creation - and appending to the 'summary.json' file
TEST_NAME="$CURRENT_JOB_NAME"
if [[ ! -f "$REPOSITORY_ROOT/VERSION" ]]; then
	echo "Cannot open $REPOSITORY_ROOT/VERSION to read canton version"
	exit 1
fi
VERSION=$(cat "$REPOSITORY_ROOT/VERSION")
CANTON_VERSION="canton-open-source-${VERSION}"
HOST_NAME="$(hostname)"

if [[ -n "$UPDATES_EARLY_TO_LATE_TIME" && "$UPDATES_EARLY_TO_LATE_TIME" -gt 0 ]] 2>/dev/null; then
	TPS="$(awk "BEGIN {printf \"%.2f\", $UPDATES_EARLY_TO_LATE_COUNT / $UPDATES_EARLY_TO_LATE_TIME}")"
else
	echo
	echo "[ERROR] The extraction of test time is empty or zero!"
	echo "[ERROR] Hint: Perhaps the data is the same for $EARLY_EVENT_PERCENTILE and $LATE_EVENT_PERCENTILE?"
	echo "[ERROR] Please check the metrics in $METRICS_DIR/participant1.daml.participant.api.indexer.events.csv to verify!"
	return 1
fi

# We save the summary of the test into the metrics base directory
# that is the same across the tests run under the same nightly test run
SUMMARY_JSON_FILE="$METRICS_BASE_DIR/summary.json"
# export-telegraf-metrics.sh needs these variables for dynamic queries
export TEST_START_TIME="$UPDATES_START_TIME"
export TEST_DURATION="$UPDATES_DURATION_IN_SECS seconds"

echo
echo "Appending data to $SUMMARY_JSON_FILE ..."

# Get git branch 
GIT_BRANCH="$(git branch --show-current 2>/dev/null)"

if [[ -z "$GIT_BRANCH" ]]; then
	# If in detached HEAD state, fall back to short commit SHA
	GIT_BRANCH="$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")"
fi

jq -n \
	--arg test "$TEST_NAME" \
	--arg ver "$CANTON_VERSION" \
	--arg host "$HOST_NAME" \
	--arg start "$TEST_START_TIME" \
	--arg duration "$TEST_DURATION" \
	--arg branch "$GIT_BRANCH" \
	--argjson tps "$TPS" \
	'{
		"test-name": $test,
		"version": $ver,
		"branch": $branch,
		"machine": $host,
		"start-time": $start,
		"duration": $duration,
		"results": {
			"transactions-per-second": $tps
		}
	}' >> "$SUMMARY_JSON_FILE"
