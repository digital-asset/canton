#! /usr/bin/env bash
set -euo pipefail

gcloud beta auth activate-service-account --key-file=<(echo "${GAC_JSON}")
gcloud auth configure-docker "${1}"
