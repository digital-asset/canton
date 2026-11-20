// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests
package testsuite

import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion

object FeatureUnstableTestSuite extends DamlScriptTestSuite {
  import DamlScriptTestSuiteFramework.ExpectedResult.*

  override val projectName = "FeatureUnstableTests"

  override def isEnabledFor(
      protocolVersion: ProtocolVersion,
      languageVersion: LanguageVersion,
  ): Boolean =
    languageVersion.isDevVersion

  override val expectedResults = Map(
    "Experimental:run" -> Success()
  )
}
