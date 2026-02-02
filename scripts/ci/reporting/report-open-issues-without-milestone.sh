#!/bin/bash

items=$(hub issue -M "none" -f "%I %au %as  %t %U%n")
if [ -z "$items" ]; then
  echo "No open issues without milestone."
else
  echo "Open issues without milestone:" > report.txt
  echo "$items" | sed -E 's/"//g' >> report.txt
  cat report.txt
  if [ -n "$SLACK_WEBHOOK_TEAM_CANTON_NOTIFICATIONS" ]; then
    curl -X POST -H 'Content-type: application/json' --data "{\"text\":\"$(cat report.txt)\"}" $SLACK_WEBHOOK_TEAM_CANTON_NOTIFICATIONS
  else
    echo "No SLACK_WEBHOOK_TEAM_CANTON_NOTIFICATIONS defined."
  fi
fi

