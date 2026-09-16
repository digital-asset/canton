#!/bin/bash
set -euo pipefail

cd /app

performance/generate-remote-config.sh additional-config.conf
cat additional-config.conf
./entrypoint.sh
