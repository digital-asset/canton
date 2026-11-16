#!/usr/bin/env bash
#
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#
###############################################################################
# Runs the ledger API performance test.
###############################################################################

set -eu -o pipefail

wait_for_postgres() {
        for _ in $(seq 1 120); do
                pg_isready -q -h localhost -p 5432 && return 0
                sleep 1
        done
        echo "postgres did not become ready within 120 seconds"
        exit 1
}

wait_for_postgres_stop() {
        for _ in $(seq 1 120); do
                pgrep -x postgres >/dev/null || return 0
                sleep 1
        done
        echo "postgres did not stop within 120 seconds"
        exit 1
}

index_size_query=$(
        cat <<EOF
SELECT
    relname as index_name,
    pg_size_pretty(pg_total_relation_size(c.oid)) as index_size
FROM
    pg_class c
WHERE
    relkind = 'i'
ORDER BY
    index_size DESC;
EOF
)

postgres_stats() {
        stage="$1"
        target_file="$LAPI_REPORT_TARGET_PATH/pg_stats_${stage}.txt"
        {
                sudo -u postgres psql -X -d postgres -c "select * from pg_stat_wal" || true
                sudo -u postgres psql -X -d postgres -c "select * from pg_stat_bgwriter" || true
                sudo -u postgres psql -X -d postgres -c "select pg_size_pretty(pg_database_size('$LAPI_DB_NAME')) as db_size" || true
                sudo -u postgres psql -X -d $LAPI_DB_NAME -c "$index_size_query" || true
        } >$target_file 2>&1
}
STAT_PIDS=()
machine_stats() {
        iostat -mt 5 >>"$LAPI_REPORT_TARGET_PATH/iostats_$1.log" 2>&1 &
        STAT_PIDS+=($!)
        sudo pidstat -d 5 >>"$LAPI_REPORT_TARGET_PATH/pidstat_$1.log" 2>&1 &
        STAT_PIDS+=($!)

}

stop_machine_stats() {
        for pid in ${STAT_PIDS[@]}; do
                sudo kill $pid >/dev/null 2>&1 || true
        done
        wait "${STAT_PIDS[@]}" 2>/dev/null || true
        STAT_PIDS=()
}

read_config() {
        echo "*** config *** "
        # Test configuration is provided in a json file shared by bash python and scala.
        if [[ -f $CONFIG_FILE ]]; then
                echo "loading config from $CONFIG_FILE"
                export LAPI_REPORT_PATH=$(jq -r '.report_path' "$CONFIG_FILE")
                export LAPI_DB_NAME=$(jq -r '.db_name' "$CONFIG_FILE")
                export LAPI_RETENTION_DAYS=$(jq -r '.log_retention_days' "$CONFIG_FILE")

                mapfile -t CONFIG_ITEMS < <(jq -r '.stages[] | [.stage_name, .description, .scala_test, .new_db, .xmx] | @tsv' "$CONFIG_FILE")
                echo "loaded ${#CONFIG_ITEMS[@]} config items"
        else
                echo "Config file $CONFIG_FILE not found."
                exit 1
        fi
}

send_slack_message() {
        if [[ -n ${LAPI_SLACK_API_KEY+x} && -n ${LAPI_PERF_TEST_SLACK_CHANNEL_ID+x} ]]; then
                curl -s -S -X POST -H "Content-type: application/json;" -H "Authorization: Bearer $LAPI_SLACK_API_KEY" \
                        --data "{\"channel\":\"$LAPI_PERF_TEST_SLACK_CHANNEL_ID\", \"text\":\"$1\"}" \
                        https://slack.com/api/chat.postMessage
        else
                echo "Slack not configured message not sent: $1"
        fi
}

clean_up_files() {
        RETENTION_DAYS=${LAPI_RETENTION_DAYS:-14}
        LIMIT_DATE=$(date -d "$RETENTION_DAYS days ago" "+%Y-%m-%d")
        if [[ -d $1 ]]; then
                echo "Cleaning up files in $1 older than $LIMIT_DATE"
                #files not modified more recently than the limit date
                find $1 -type f ! -newermt "$LIMIT_DATE" -print -delete
                # remove directories that should be empty
                find "$1" -mindepth 1 -type d -empty -print -delete
        else
                echo "Directory $1 does not exist, skipping cleanup"
                return
        fi
}

move_canton_logs() {
        if [[ -d ./log && -f ./log/canton_test.log ]]; then
                mv ./log/canton_test.log $LAPI_REPORT_TARGET_PATH/canton_test_${TODAY}_${NOW}.log
        else
                echo "No canton log was found"
        fi
}

on_exit() {
        EXIT_CODE=$?
        echo "*** Reporting and cleaning up ***"
        ## could not find a proper way to return this address
        ## hostname returns name without dashes, without domain
        ssh_host="canton-network-testing-1.da-int.net"
        stop_machine_stats

        move_canton_logs

        if [[ -n ${REPORT_PATH+x} ]]; then
                echo "report path: $REPORT_PATH"
        fi
        clean_up_files "$REPORT_PATH"
        if [[ -n ${LAPI_CRON_OUTPUT_PATH+x} ]]; then
                clean_up_files "$LAPI_CRON_OUTPUT_PATH"
        fi
        if [[ $EXIT_CODE -eq 0 ]]; then
                message=$(
                        cat <<EOF
:tada: LAPI performance test finished succesfully
branch: $BRANCH
last commit: $(git log -1 --pretty=format:"%h %s (%ci) <%an>")
dashboard: <https://app.datadoghq.com/dashboard/rwe-rzq-zrf/lapi-nightly-performance-tests|LAPI datadog dashboard>
logs are in $LAPI_REPORT_TARGET_PATH
log in with ssh canton@$ssh_host
EOF
                )
                echo "$message"
                send_slack_message "$message"
                python "$SRCDIR"/util/lapi-perf-test-metric.py $CONFIG_FILE $TODAY $NOW

        else
                message=$(
                        cat <<EOF
<!here> :rotating_light: LAPI performance test failed with status $EXIT_CODE
branch: $BRANCH
last commit: $(git log -1 --pretty=format:"%h %s (%ci) <%an>")

logs are in $LAPI_REPORT_TARGET_PATH
log in with ssh canton@$ssh_host
EOF
                )
                echo "$message"
                send_slack_message "$message"
        fi
}

trap on_exit EXIT

recreate_db() {
        echo "*** Database setup ***"
        sudo -u postgres dropdb --if-exists "$LAPI_DB_NAME"
        sudo service postgresql restart
        wait_for_postgres
        sudo -u postgres createdb "$LAPI_DB_NAME"
        echo "checkpoint;" | sudo -u postgres psql -d "$LAPI_DB_NAME"
        echo "vacuum analyze;" | sudo -u postgres psql -d "$LAPI_DB_NAME"
}
reset_caches() {
        echo "*** restart database and clean cache ***"
        echo "checkpoint;" | sudo -u postgres psql -d "$LAPI_DB_NAME"
        echo "vacuum analyze;" | sudo -u postgres psql -d "$LAPI_DB_NAME"
        sudo service postgresql stop
        wait_for_postgres_stop
        sudo free && sudo sync && sudo sh -c 'echo 1 >/proc/sys/vm/drop_caches' && sudo free
        sudo service postgresql start
        wait_for_postgres
}

export BRANCH=$(git branch --show-current)
export CURRENT_JOB_NAME="lapi-performance[$BRANCH]"
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export CONFIG_FILE=${1:-"$SRCDIR"/lapi-performance-test-config.json}
read_config

TODAY=$(date +"%Y-%m-%d")
NOW=$(date +"%H-%M-%S")
REPORT_PATH="${LAPI_REPORT_PATH:-/tmp/lapi-reports}"
export LAPI_REPORT_TARGET_PATH="$REPORT_PATH/$TODAY/$NOW"

if [[ -e "$LAPI_REPORT_TARGET_PATH" && ! -d "$LAPI_REPORT_TARGET_PATH" ]]; then
        echo "a file exists with the intended report path name: $LAPI_REPORT_TARGET_PATH"
elif [[ ! -d "$LAPI_REPORT_TARGET_PATH" ]]; then
        mkdir -p "$LAPI_REPORT_TARGET_PATH"
fi

echo -e "\n**** sbt build ****\n"
#sbt clean
sbt "Perf / compile"

export CANTON_LOG_LEVEL=WARN
# if some items were defined in config execute them
if [[ ${#CONFIG_ITEMS[@]} -gt 0 ]]; then
        for item in "${CONFIG_ITEMS[@]}"; do
                IFS=$'\t' read -r stage_name description scala_test new_db xmx <<<"$item"
                echo -e "\n\n\n***$stage_name:  $description ***\n\n\n"
                if [[ $new_db == "true" ]]; then
                        recreate_db
                fi
                machine_stats $stage_name
                postgres_stats "${stage_name}_before"
                sbt -Dperf.xmx=$xmx "ledger-api-core/Perf/testOnly *$scala_test" | tee "$LAPI_REPORT_TARGET_PATH/${stage_name}_output.log"
                postgres_stats "${stage_name}_after"
                reset_caches
                stop_machine_stats
        done
else
        echo "No configured stages found"
        exit 1
fi
