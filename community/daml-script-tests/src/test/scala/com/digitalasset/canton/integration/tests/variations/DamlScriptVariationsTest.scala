// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.variations

import com.digitalasset.canton.integration.tests.DamlScriptPV35LF23ITBase
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}

// TODO(#33803): change the base class of these tests to one containing the appropriate daml script tests
abstract class DamlScriptITLatestStable extends DamlScriptPV35LF23ITBase {
  override def doRunTests(scriptIdsToTest: List[String]) =
    // variation tests do not use protocolVersionForTesting filter given the canton version determined outside
    // variation tests do not check that all scriptIds should have expected results defined
    // instead only use those that do have, sparing some time to run only the tests required
    scriptIdsToTest
      .filter(expectedResults.keySet)
      .foreach(testScriptId => testScriptId in testGivenScriptId(testScriptId))

}

class DamlScriptLatestStableTinyCacheTest extends DamlScriptITLatestStable {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransforms(ConfigTransforms.setTinyCache)
  doRunTests(scriptIdsToTest)
}

class DamlScriptLatestStableNoCacheTest extends DamlScriptITLatestStable {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.addConfigTransforms(ConfigTransforms.disableCache)
  doRunTests(scriptIdsToTest)
}
