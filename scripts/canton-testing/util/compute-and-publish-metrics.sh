#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Reads metrics from their respective files and
# sends metric reports to Slack and DataDog.
###############################################################################

set -eu -o pipefail
set -o allexport

echo
echo "***** Preparing metrics reports..."

# See read-csv-metric.py to understand how these are used.
EARLY_EVENT_PERCENTILE="$1"
LATE_EVENT_PERCENTILE="$2"

rate() {
  if [[ $2 -ne 0 ]]; then
    echo "$(( $1 / $2 ))"
  else
    echo 0
  fi
}

print-bytes() {
  if [[ $1 -le 10000 ]]; then
    echo "$1 B"
  elif [[ $1 -le 10000000 ]]; then
    echo "$(($1 / 1000)) kB"
  elif [[ $1 -le 10000000000 ]]; then
    echo "$(($1 / 1000000)) MB"
  else
    echo "$(($1 / 1000000000)) GB"
  fi
}

cat > "$SLACK_METRICS_FILE" <<EOI
*Performance test '$CURRENT_JOB_NAME' at $HOSTNAME:*

Last commit:
\`\`\`
$(git log -1 --pretty="%an %ci commit %h%n%D%n%s")
\`\`\`

Log location: \`$LOGS_DIR\`

EOI

# Sourcing this to make definitions available to subsequent scripts
. extract-throughput-metrics.sh "$EARLY_EVENT_PERCENTILE" "$LATE_EVENT_PERCENTILE"

extract-telegraf-metrics.sh

extract-log-metrics.sh

cat <<EOI
Exact timestamps used for computing this report:
- Job start: $(date -d "@$JOB_START_TS" "+%H:%M:%S %Z")
- Early: $(date -d "@$EARLY_TS" "+%H:%M:%S %Z")
- Late: $(date -d "@$LATE_TS" "+%H:%M:%S %Z")
EOI

cat $SLACK_METRICS_FILE

publish-metrics-to-slack.sh

publish-metrics-to-datadog.sh
