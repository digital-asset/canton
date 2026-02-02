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
  # pass filter arguments for rows where we need to filter out the attributes
  flt=""
  if [ $# -gt 2 ]; then
    flt="$3"
  fi
  eval "$(read-csv-metric.py "$METRICS_DIR/$1" "$2" "$EARLY_EVENT_PERCENTILE" "$LATE_EVENT_PERCENTILE" "$flt")"
}

load-metrics participant1.daml.participant.console.tx-nodes-emitted.csv TX
# the indexer metrics include many events, but we are only interested in the indexer event updates
load-metrics participant1.daml.participant.api.indexer.events.csv UPDATES PerformanceTest
load-metrics participant1.synchronizer.daml.sequencer-client.handler.sequencer-events.csv PARTICIPANT_EVENTS
load-metrics mediator.daml.sequencer-client.handler.sequencer-events.csv MEDIATOR_EVENTS

# Starting point for general throughput measurements
# shellcheck disable=SC2034
EARLY_TS="$PARTICIPANT_EVENTS_EARLY_TS"
# End point for general throughput measurements
# shellcheck disable=SC2034
LATE_TS="$TX_LATE_TS"

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
Throughput (transaction service): $TX_THROUGHPUT root nodes/s ($TX_EARLY_TO_LATE_COUNT tx root nodes in $TX_EARLY_TO_LATE_TIME s)
Throughput (read service): $UPDATES_THROUGHPUT root nodes/s ($UPDATES_EARLY_TO_LATE_COUNT update root nodes in $UPDATES_EARLY_TO_LATE_TIME s)
Throughput (participant sequencer client): $PARTICIPANT_EVENTS_THROUGHPUT events/s ($PARTICIPANT_EVENTS_EARLY_TO_LATE_COUNT events in $PARTICIPANT_EVENTS_EARLY_TO_LATE_TIME s)
Throughput (mediator sequencer client): $MEDIATOR_EVENTS_THROUGHPUT events/s ($MEDIATOR_EVENTS_EARLY_TO_LATE_COUNT events in $MEDIATOR_EVENTS_EARLY_TO_LATE_TIME s)

EOI
