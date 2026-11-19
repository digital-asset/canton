// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests
package testsuite

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion

/** Instances of this trait are defined for each Daml project:
  *   - Daml project code is located in the `resources` directory
  *   - and instances of this trait are defined under the `testsuite` package.
  *
  * Instances of this trait allow Daml project code to be shared across multiple
  * `DamlScriptTestSuiteFRamework` instances.
  */
trait DamlScriptTestSuite extends BaseTest {
  def projectName: String

  def expectedResults: Map[String, DamlScriptTestSuiteFramework.ExpectedResult]

  def isEnabledFor(protocolVersion: ProtocolVersion, languageVersion: LanguageVersion): Boolean
}
