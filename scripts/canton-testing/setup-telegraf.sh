#!/usr/bin/env bash
#
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
# Proprietary code. All rights reserved.
#

###############################################################################
# Run this to set up telegraf on the local system.
# Assumes Linux.
# Requires that telegraf is already installed.
###############################################################################

set -eu -o pipefail

SRCDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

sudo cp "$SRCDIR"/config/telegraf.conf /etc/telegraf

# If this step fails, run `telegraf --debug` to get debug output.
sudo systemctl restart telegraf
