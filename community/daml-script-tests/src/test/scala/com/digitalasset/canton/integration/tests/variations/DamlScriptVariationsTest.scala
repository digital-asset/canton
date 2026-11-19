// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.variations

import com.digitalasset.canton.integration.tests.DamlScriptTestSuiteFramework
import com.digitalasset.canton.integration.tests.testsuite.ContractKeysLatestStablePVTestSuite
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}

// TODO(#33803): change the base class of these tests to one containing the appropriate daml script tests

class DamlScriptLatestStableTinyCacheTest
    extends DamlScriptTestSuiteFramework(ContractKeysLatestStablePVTestSuite) {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransforms(ConfigTransforms.setTinyCache)

  declareTests()
}

class DamlScriptLatestStableNoCacheTest
    extends DamlScriptTestSuiteFramework(ContractKeysLatestStablePVTestSuite) {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransforms(ConfigTransforms.disableCache)

  declareTests()
}
