#!/bin/bash

CONFIG_DAY="Tue"
# If you need to amend when the report is created, add your dates here
DO_NOT_RUN="1999-01-01"
RUN_INSTEAD="1999-01-01"
TEAMS="sequencer security architecture lapi protocol"

#############################
DAYOFWEEK=$(date +"%a")
WEEK=$(date +"%V")
TODAY=$(date +"%Y-%m-%d")

echo $DO_NOT_RUN | grep $TODAY > /dev/null
if [ $? -eq 0 ]; then
  echo "Day is on the do not run today list."
  exit 0
fi

echo $RUN_INSTEAD | grep $TODAY > /dev/null
if [ $? -ne 0 ]; then
  if [ "$DAYOFWEEK" != "$CONFIG_DAY" ]; then
    echo "Day $DAYOFWEEK is not the configured day $CONFIG_DAY."
    exit 0
  fi
fi

echo "running planning"
for team in $TEAMS
do
  amm scripts/planning.sc --team $team --week $WEEK --slack "DACH-NY/canton" > planning.txt
  cat planning.txt
  if [[ -n "$SLACK_WEBHOOK_TEAM_CANTON_NOTIFICATIONS" ]]; then
    curl -s -S -X POST -H 'Content-type: application/json' \
        --data @planning.txt "$SLACK_WEBHOOK_TEAM_CANTON_NOTIFICATIONS"
  else
    echo "No SLACK_WEBHOOK_TEAM_CANTON_NOTIFICATIONS defined."
  fi
done
