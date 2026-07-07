// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.environment.{CantonEnvironmentFactory, CommunityEnvironmentFactory}

trait CommunityIntegrationTest extends CantonBaseIntegrationTest {
  this: CantonEnvironmentSetup =>

  override protected val environmentFactory: CantonEnvironmentFactory =
    CommunityEnvironmentFactory
}
