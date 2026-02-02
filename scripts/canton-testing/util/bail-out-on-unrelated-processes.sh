#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Stop execution if another Java or Docker process is running.
# This lowers the risk of accidentally running two tests in parallel.
###############################################################################

set -eu -o pipefail

TESTNAME=${CURRENT_JOB_NAME:-"unknown"}
LOGS_DIR=${LOGS_DIR:-"unknown"}
UTIL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
basicInformation="*Test $TESTNAME on canton-testing:*
Last commit:
\`\`\`
$(git log -1 --pretty="%an %ci commit %h%n%D%n%s")
\`\`\`

Log location: \`$LOGS_DIR\`"

if ps -C java &> /dev/null;then
  javaPretty=$(ps -C java -o pid,user,state,start_time,args | sed G)
  prettyForSlack=$(printf "%s" "$javaPretty" | cut -c 1-120)
  echo "Another java process is running. Giving up."
  echo "$javaPretty"
  $UTIL_DIR/send-slack-message.sh "$basicInformation

  Test \`$TESTNAME\` was not started because of following Java process(es) (truncated to 120 characters):
\`\`\`
$prettyForSlack
\`\`\`
"
  exit 1
fi

if [ -n "$(docker ps -q)" ];then
  dockerPretty=$(docker ps --format "Image '{{.Image}}': container '{{.ID}}'")
  echo "Another docker process is running. Giving up."
  echo "$dockerPretty"
  $UTIL_DIR/send-slack-message.sh "$basicInformation

Test \`$TESTNAME\` was not started because of following Docker process(es):
  \`\`\`
  $dockerPretty
  \`\`\`
  "
  exit 1
fi

# Evict disk cache
sudo sync
echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

# Fail fast on excessive memory usage
free_mem="$(grep MemFree /proc/meminfo | tr -dc '0-9')"
total_mem="$(grep MemTotal /proc/meminfo | tr -dc '0-9')"
percent_free=$((free_mem * 100 / total_mem))
if [[ $percent_free -le 90 ]]; then
  echo "Not enough free memory: $free_mem kB out of $total_mem kB ($percent_free %)"
  exit 1
fi
