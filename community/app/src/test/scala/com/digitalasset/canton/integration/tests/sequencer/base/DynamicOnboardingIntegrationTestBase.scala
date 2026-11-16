// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.base

import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.sequencer.DynamicOnboardingIntegrationTest

abstract class DynamicOnboardingIntegrationTestBase(sequencerType: String)
    extends DynamicOnboardingIntegrationTest(sequencerType) {

  protected def plugin: EnvironmentSetupPlugin

  registerPlugin(plugin)
  registerPlugin(new UsePostgres(loggerFactory))
}
