#!/usr/bin/env bash
# Runs the full smoke on the newest public-unstable snapshot of the VERSION line.
# Manual snapshot releases share that path, so the tag may not come from main.
set -euo pipefail
cd "$(dirname "$0")"

registry="europe-docker.pkg.dev"
repo="da-images/public-unstable/docker"
line="$(awk -F. '{print $1 "." $2}' ../../../../VERSION)"

canton_tag="$(python3 latest_snapshot_tag.py "$registry" "$repo" "$line" \
  canton-base canton-participant canton-sequencer canton-mediator)"
pqs_tag="$(python3 latest_snapshot_tag.py "$registry" "$repo" "$line" participant-query-store)"
echo "▶️ Deployment smoke on Canton ${canton_tag} with PQS ${pqs_tag}"

OCI_PATH="${registry}/${repo}/" \
PQS_IMAGE="${registry}/${repo}/participant-query-store:${pqs_tag}" \
  exec ./run_test.sh "$canton_tag"
