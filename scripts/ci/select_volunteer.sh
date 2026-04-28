#!/usr/bin/env bash
# Selects a volunteer Slack user ID deterministically based on the current workflow run.
# Outputs the selected Slack user ID to stdout.
# Reads the workflow identifier from CIRCLE_WORKFLOW_ID (CircleCI) or GITHUB_RUN_ID (GitHub Actions).

set -euo pipefail

members=(
  # Architecture
  "U9AGNADBJ"   # Andreas L.
  "U056R7C1H1A" # Cristina
  # Security
  "U03RK2ZNZFH" # Christian
  "U03A208N537" # João Sá
  "U990L3H0R"   # Matthias
  "U0A0VU32CS0" # Simran
  "U276HDDB6"   # Soren
  "U03NCGBFCDP" # Thibault
  "UPED4TSGN"   # Simon
  # Protocol
  "U5LC1FB6D"   # Gerolf
  "U046D8XGD7H" # Kirill
  "U03FH83LRNH" # Meriam
  "UF766DLN4"   # Oliver
  "U020XUXADCP" # Raf
  "UDDVDCKD0"   # Raphael
  "U04DC4NAK1S" # Yves
  # LAPI
  "U08LBV2U75H" # Adrien
  "U03HVEFHGLU" # Andreas T.
  "U9AM249RP"   # Andris
  "U1CNLNUV7"   # Gergely
  "U029VGVV25Q" # Jarek
  "U57BTEKR9"   # Marcin
  "U01A2K1NWAX" # Marton
  "U01B2BD2CP4" # Tudor
  # BFT
  "U04S25SG9D5" # Daniel
  "U04TG4XMJ"   # Danilo
  "USE2D5GB1"   # Fabio
  "U04P9CS4F6Z" # Tom
  # QA
  "U0AHRL04HPV" # Tbo
)

WORKFLOW_ID="${CIRCLE_WORKFLOW_ID:-${GITHUB_RUN_ID:-}}"
if [ -z "$WORKFLOW_ID" ]; then
  echo "Warning: neither CIRCLE_WORKFLOW_ID nor GITHUB_RUN_ID is set, volunteer selection will not be deterministic." >&2
fi
WORKFLOW_IDENTIFIER=$(echo "$WORKFLOW_ID" | sha256sum | cut -c1-2 | awk '{ print "0x" toupper($0) }')
echo "${members[$WORKFLOW_IDENTIFIER % ${#members[@]}]}"
