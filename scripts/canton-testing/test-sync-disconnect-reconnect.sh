#!/usr/bin/env bash
#
# Copyright (c) 2023-2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Runs the nightly performance test on synchronizer disconnect and reconnect.
###############################################################################

set -eu -o pipefail

echo
echo "*************************************************************************"
echo "* Starting test-sync-disconnect-reconnect ..."
echo "*************************************************************************"

export CURRENT_JOB_NAME="test-sync-disconnect-reconnect-benchmark"

SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
. "$SRCDIR"/util/load-settings.sh "$@"
. setup-standalone-services.sh
. terminate-subprocesses-on-exit.sh

bail-out-on-unrelated-processes.sh

# Note: db setup (including db environment variables) are not sourced in load-settings.sh call so
# we use standard local ports for standalone barebone Postgres
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="supersafe"
export POSTGRES_HOST="localhost"
export POSTGRES_PORT=5432
export POSTGRES_DB="postgres"

# Unified teardown function ensuring processes + containers are cleaned up
full_teardown() {
	set +e
	echo "***** Performing test teardown..."
	terminate-and-log
	teardown-postgres
}

trap full_teardown EXIT INT TERM

mkdir -p "$LOGS_DIR"

echo
echo "***** Starting standalone services..."
setup-postgres

echo
echo "***** Running test-sync-disconnect-reconnect..."
(
	cd "$REPOSITORY_ROOT"
	export LOG_FILE_NAME="$PARTICIPANTS_LOG_FILE"
	export LOG_FILE_ROLLING=1
	export JVM_NAME="sync_disconnect_reconnect_benchmark"

	export JAVA_OPTS="$COMMON_JAVA_OPTS $SYNC_DISCONNECT_RECONNECT_JAVA_OPTS"
	export CI=1
	export NIGHTLY_PERF_RUN=1

	# Bypassing network namespace
	run-in-namespace.sh "" \
		sbt -J-Dlogback.configurationFile=logback.xml \
				-J-Dcanton.metrics.reporters.csv.directory="$METRICS_DIR" \
				-J-Dio.opentelemetry.exporter.metrics.csv.directory="$METRICS_DIR" \
				"community-app/testOnly com.digitalasset.canton.integration.tests.benchmarks.SynchronizerDisconnectAndReconnectTestPostgres"
)

# Terminate test components before gathering metrics
terminate-subprocesses

export KNOWN_MISSING_FAILED_TRADER_METRICS="true"

# This is a Bong test as well so it doesn't use PerformanceRunner but the transactions
# are generally accepted without userId so we need to update filtering in the indexer metric csv file
export UPDATES_LOAD_METRICS_FILTER_PATTERN="event_type=transaction.*status=accepted"

export PARTICIPANT_EVENTS_METRICS_FILE="participant1.synchronizer1.daml.sequencer-client.handler.sequencer-events.csv"
export MEDIATOR_EVENTS_METRICS_FILE="mediator1.daml.sequencer-client.handler.sequencer-events.csv"

# Tell metrics processor that log files are unified
export COMMON_LOG_FILE="$PARTICIPANTS_LOG_FILE"

compute-and-publish-metrics.sh 10 90