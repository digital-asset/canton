#!/bin/bash
set -euo pipefail

# weird pwd issues if we don't control cd here
cd /app

exec ./entrypoint.sh
