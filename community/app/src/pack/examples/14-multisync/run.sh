#!/bin/bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Compile the Daml models
(cd "$SCRIPT_DIR/model" && dpm build)

# Start canton sandbox in multi-sync mode (3 participants, 2 synchronizers)
cd "$SCRIPT_DIR"
../../bin/canton sandbox --multi-sync
