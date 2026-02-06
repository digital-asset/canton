#!/usr/bin/env bash

#
# Run Performance Test
#
# Flags (with defaults):
#   -h : help
#   -r : reset DB (false)
#   -t : use TLS (tls)
#   -p : use profile (heavy)
#   -d : use database (postgres)
#   -m : use metrics (prometheus)
#   -w : mode (empty = interactive)
#   -c : run script (performance) or use "manual" to use --manual-start
#   -n : topology (simple-topology)
#   -s : don't run at scale (default is scale e.g. suitable for sgx-testing)
#

set -ueo pipefail

function usage {
	echo "Usage: $0 [-h] [-r] [-s] [ -t {tls|notls} ] [ -p {heavy|light} ]  [ -d {postgres} ] [ -m {none|prometheus|csv} ] [-w {daemon|run} ] [ -c {performance|show} ] [ -n simple-topology|distributed-topology|... ]"
	[ $# -gt 0 ] && echo "Error: $1"
	exit 1
}

unset SCALE_TEST
unset TLS_ROOT_CERT

reset=false
useTls=tls
scaleTest=true
profile=heavy
dbinstance=postgres
metrics=prometheus
mode=""
bootstrap=performance
topology="simple-topology"

while getopts "rst:d:p:m:w:c:h:n:" OPT
do
  case "${OPT}" in
    r) reset=true ;;
    s) scaleTest=false ;;
    t) useTls=${OPTARG} ;;
    d) dbinstance=${OPTARG} ;;
    p) profile=${OPTARG} ;;
    m) metrics=${OPTARG} ;;
    w) mode=${OPTARG} ;;
    c) bootstrap=${OPTARG} ;;
    n) topology=${OPTARG} ;;
    *) usage ;;
  esac
done
shift $((OPTIND-1))

# Change to the release dir

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
RELEASE_DIR="$(dirname $SCRIPT_DIR)"
cd $RELEASE_DIR

persistence_conf="performance/$dbinstance/persistence.conf"
[ -f "$persistence_conf" ] ||  usage "Could not find DB persistence config: $persistence_conf"

profile_conf="performance/profiles/$profile.conf"
[ -f "$profile_conf" ] || usage "Could not find profile config: $profile_conf"

metrics_conf="performance/metrics/$metrics.conf"
[ -f "$metrics_conf" ] || usage "Could not find metrics config: $metrics_conf"

tls_conf="performance/tls/$useTls.conf"
[ -f "$tls_conf" ] || usage "Could not find TLS config: $tls_conf"

if [[ "${bootstrap}" == "manual" ]]; then
  script="--manual-start"
else
  scriptfile="performance/$bootstrap.canton"
  [ -f "$scriptfile" ] || usage "Could not find canton bootstrap script: $scriptfile"
  if [[ "${mode}" == "run" ]]; then
    script="${scriptfile}"
  else
    script="--bootstrap=${scriptfile}"
  fi
fi

if [[ "${topology}" != "two-synchronizers-topology" ]]; then
  export SINGLE_SYNCHRONIZER="1"
fi

topology_conf="performance/topology/$topology.conf"
[ -f "$topology_conf" ] || usage "Could not find topology file: $topology_conf"

. "performance/$dbinstance/db.env"

if $reset; then
	echo "Resetting database..."
	cd performance/$dbinstance
	../../config/utils/$dbinstance/db.sh reset
	cd $RELEASE_DIR
fi

if [ $useTls == "tls" ]; then
  echo "TLS enabled, setting root certificate"
  export TLS_ROOT_CERT="performance/tls/root-ca.crt"
fi

topology_prefix=$(echo $topology | cut -c 1-7)
if [ $topology_prefix == "remote-" ]; then
  if [ $useTls == "tls" ]; then
    echo "Remote topology cannot be used with TLS"
    exit 1
  else
    tls_parameter=""
  fi
else
  tls_parameter="--config $tls_conf"
fi

export MEMORY=4G
if $scaleTest;then
  echo "Running scale test"
  export SCALE_TEST=1
  MEMORY=32G
fi

mkdir -p log

export LOG_LEVEL_CANTON=INFO

HEAP_FILE="$(pwd)/log/heap.bin"
# Support the empty (interactive) mode:
# shellcheck disable=SC2086
export JAVA_OPTS="-Xmx${MEMORY} -Xlog:gc*:./log/garbagecollection.log:time -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${HEAP_FILE}" \

./bin/canton $mode "$script" \
  --log-file-appender=rolling \
  --log-file-rolling-history=168 \
  --log-file-rolling-pattern=yyyy-MM-dd-HH \
  --config "$topology_conf" \
  --log-immediate-flush=false \
  $tls_parameter \
  --config "$persistence_conf" \
  --config "$metrics_conf" \
  --config "$profile_conf" \
  "-Dcom.sun.management.jmxremote.ssl=false" \
  "-Dcom.sun.management.jmxremote.authenticate=false" \
  "-Dcom.sun.management.jmxremote.port=9010" \
  "-Dcom.sun.management.jmxremote.rmi.port=9011" \
  "-Djava.rmi.server.hostname=localhost" \
  "-Dcom.sun.management.jmxremote.local.only=true"
