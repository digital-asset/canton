#!/usr/bin/env bash

set -eu -o pipefail

# Sets up environment and stdout redirection to run a command from cron.

cd "$(dirname "${BASH_SOURCE[0]}")"

OUTPUT_DIR="$1/$(date "+%Y-%m-%d")"
shift
OUTPUT_FILE="$OUTPUT_DIR/$(date "+%H-%M-%S").txt"

mkdir -p "$OUTPUT_DIR"

(
# Setup environment
set -o allexport
PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin"
USER="canton"
CRON_OUTPUT_FILE="$OUTPUT_FILE"
. /home/canton/.bash_profile
set +o allexport

"$@"
) 2>&1 | tee "$OUTPUT_FILE"

