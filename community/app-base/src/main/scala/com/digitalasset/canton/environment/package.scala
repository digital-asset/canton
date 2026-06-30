// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import config.CantonConfig

package object environment {
  type CantonEnvironmentFactory = EnvironmentFactory[CantonConfig, CantonEnvironment]
}
