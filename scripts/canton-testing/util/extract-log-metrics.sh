#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Extracts metrics from the Canton log files and
# writes reports to $DATADOG_METRICS_FILE and $SLACK_METRICS_FILE.
###############################################################################

set -eu -o pipefail

echo
echo "***** Extracting log metrics from $SYNCHRONIZERS_LOG_FILE and $PARTICIPANTS_LOG_FILE..."

# Install the canton log format
lnav -i "$REPOSITORY_ROOT"/canton.lnav.json
# Check consistency of format
lnav -C

participants_num_warnings_or_errors=$(lnav -n -c ":set-min-log-level warning" "$PARTICIPANTS_LOG_FILE" | wc -l)
participants_log_size=$(wc -c < "$PARTICIPANTS_LOG_FILE")

synchronizers_num_warnings_or_errors=$(lnav -n -c ":set-min-log-level warning" "$SYNCHRONIZERS_LOG_FILE" | wc -l)
synchronizers_log_size=$(wc -c < "$SYNCHRONIZERS_LOG_FILE")

cat >> "$DATADOG_METRICS_FILE" <<EOI
${DATADOG_METRIC_PREFIX}.log.participants.warnings_or_errors=$((participants_num_warnings_or_errors))
${DATADOG_METRIC_PREFIX}.log.participants.size=$((participants_log_size))
${DATADOG_METRIC_PREFIX}.log.synchronizers.warnings_or_errors=$((synchronizers_num_warnings_or_errors))
${DATADOG_METRIC_PREFIX}.log.synchronizers.size=$((synchronizers_log_size))
EOI

cat >> "$SLACK_METRICS_FILE" <<EOI
Number of warnings / errors: $participants_num_warnings_or_errors (participants), $synchronizers_num_warnings_or_errors (synchronizers)
Log file size (uncompressed): $(print-bytes "$participants_log_size") (participants), $(print-bytes "$synchronizers_log_size") (synchronizers)

EOI
