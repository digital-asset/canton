// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.integration.tests.testsuite.FeatureUnstableTestSuite
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}

class DamlScriptUnstableIT extends DamlScriptTestSuiteFramework(FeatureUnstableTestSuite) {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransforms(ConfigTransforms.enableDevVersionSupport*)

  declareTests()
}
