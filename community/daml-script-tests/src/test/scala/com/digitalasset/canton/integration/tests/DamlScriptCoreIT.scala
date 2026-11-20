// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.integration.plugins.UseExtensionService
import com.digitalasset.canton.integration.tests.testsuite.{
  FeatureContractKeyTestSuite,
  FeatureCoreTestSuite,
  FeatureExternalCallTestSuite,
}

class DamlScriptCoreIT
    extends DamlScriptTestSuiteFramework(
      FeatureCoreTestSuite,
      FeatureContractKeyTestSuite,
      FeatureExternalCallTestSuite,
    ) {
  // Needed for external call testing
  registerPlugin(new UseExtensionService(loggerFactory))

  declareTests()
}
