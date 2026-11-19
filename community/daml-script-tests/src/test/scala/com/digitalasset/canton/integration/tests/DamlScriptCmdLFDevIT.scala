// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.integration.plugins.UseExtensionService
import com.digitalasset.canton.integration.tests.testsuite.{
  FeatureContractKeyTestSuite,
  FeatureCoreTestSuite,
  FeatureExternalCallTestSuite,
}
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}

class DamlScriptCmdLFDevIT
    extends DamlScriptTestSuiteFramework(
      FeatureCoreTestSuite,
      FeatureExternalCallTestSuite,
      FeatureContractKeyTestSuite,
    ) {
  // Needed for external call testing
  registerPlugin(new UseExtensionService(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransforms(
      ConfigTransforms.enableExperimentalExecutionConductor*
    )

  declareTests()
}
