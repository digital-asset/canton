#!/usr/bin/env bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

function json_log() {
    local message="$1"
    local level="INFO"
    local logger_name="json_log"
    local thread_name="${2:-'tools.sh'}" # use "thread name" field for nicer display/debugging

    local timestamp
    timestamp="$(date -u +%FT%T.000Z)"

    # The format used here is meant to be compatible with the canton-json lnav format
    # https://github.com/digital-asset/canton/blob/main/canton-json.lnav.json
    local f_timestamp='"@timestamp":"'$timestamp'"'
    local f_message='"message":"'$message'"'
    local f_level='"level":"'$level'"'
    local f_logger_name='"logger_name":"'$logger_name'"'
    local f_thread_name='"thread_name":"'$thread_name'"'

    local log='{'$f_timestamp','$f_message','$f_level','$f_logger_name','$f_thread_name'}'
    echo "$log"
}
