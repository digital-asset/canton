#!/usr/bin/env bash
#
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

set -eu -o pipefail

echo "*************************************************************************"
echo "* Starting test-sequencer-replay..."
echo "*************************************************************************"

export CURRENT_JOB_NAME="test-sequencer-replay"

SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
. "$SRCDIR"/util/load-settings.sh "$@"
. setup-standalone-services.sh
. terminate-subprocesses-on-exit.sh

bail-out-on-unrelated-processes.sh

export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="supersafe"
export POSTGRES_HOST="localhost"
export POSTGRES_PORT=5432
export POSTGRES_DB="postgres"

# We have to give to the test a REPLAY_TESTS_DIR where the postgres dump files are saved
export REPLAY_TESTS_DIR=$SEQUENCER_REPLAY_TESTS_DIR

full_teardown() {
	set +e
	echo "***** Performing test teardown..."
	terminate-and-log
	teardown-postgres
	rm -rf "$SEQUENCER_REPLAY_TESTS_DIR"
}

trap full_teardown EXIT INT TERM

echo "***** Starting standalone Postgres service..."
setup-postgres

echo "***** Running Sequencer Replay Test..."
(
	cd "$REPOSITORY_ROOT"
	export LOG_FILE_NAME="$SYNCHRONIZERS_LOG_FILE"
	export LOG_FILE_ROLLING=1
	export JVM_NAME="sequencer_replay"

	export JAVA_OPTS="$COMMON_JAVA_OPTS $SEQUENCER_REPLAY_JAVA_OPTS"
	
	export CI=1
	export NIGHTLY_PERF_RUN=1
	# Sequencer replay is fast so we need to report more frequently
	export CSV_REPORTING_INTERVAL=1s
	
	run-in-namespace.sh "" \
		sbt -J-Dlogback.configurationFile=logback.xml \
			-J-Dreplay-tests.enable-recording=true \
			-J-Dreplay-tests.total-cycles="${SEQUENCER_REPLAY_TOTAL_CYCLES:-1000}" \
			-J-Dcanton.metrics.reporters.csv.directory="$METRICS_DIR" \
			-J-Dio.opentelemetry.exporter.metrics.csv.directory="$METRICS_DIR" \
			"community-app/testOnly com.digitalasset.canton.integration.tests.benchmarks.BftSequencerReplayBenchmark"
)

terminate-subprocesses

# Bypass missing trader metrics for replay runs
export KNOWN_MISSING_FAILED_TRADER_METRICS="true"
export KNOWN_MISSING_MEDIATOR_METRICS="true"
export KNOWN_MISSING_PARTICIPANT_METRICS="true"
export KNOWN_MISSING_TX_METRICS="true"

#We only have mediator logs
export COMMON_LOG_FILE="$SYNCHRONIZERS_LOG_FILE"

# Mediator replay is quite fast
export UPDATES_LOAD_METRICS_FILE="sequencer1.daml.sequencer.block.events.csv"
export UPDATES_LOAD_METRICS_FILTER_PATTERN="participant.*send-confirmation-response"

compute-and-publish-metrics.sh 0 100