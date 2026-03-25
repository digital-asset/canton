#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Short test to quickly find performance regressions.
# Intended to be invoked after every change on main.
###############################################################################

set -eu -o pipefail

export CURRENT_JOB_NAME="test-after-main-merge"
SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
. "$SRCDIR"/util/load-settings.sh "$@"

. slack-exit-status.sh
trap slack-exit-status EXIT

bail-out-on-unrelated-processes.sh

build.sh

test-with-fast-db.sh "$@"
