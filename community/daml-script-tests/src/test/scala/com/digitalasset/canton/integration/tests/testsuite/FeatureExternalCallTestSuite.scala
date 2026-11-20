// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests
package testsuite

import com.digitalasset.canton.integration.plugins.UseExtensionService
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion
import io.circe.*

// When you use FeatureExternalCallTestSuite in your tests, you will need to register the UseExtensionService plugin
object FeatureExternalCallTestSuite extends DamlScriptTestSuite {
  import DamlScriptTestSuiteFramework.ExpectedResult.*

  override val projectName = "FeatureExternalCallTests"

  override def isEnabledFor(
      protocolVersion: ProtocolVersion,
      languageVersion: LanguageVersion,
  ): Boolean =
    true

  override val expectedResults = Map(
    "ExternalCallTests:externalCallReturnsServiceOutput" ->
      Success(Json.fromString(UseExtensionService.defaultResponseHex)),
    // The script handles the failure itself, but the participant still warns about the failed
    // extension call; assert the warning so it is suppressed rather than tripping the CI log check.
    "ExternalCallTests:unknownExtensionFailsWithExternalCallError" -> Success { (entry: LogEntry) =>
      entry.warningMessage should include(
        "External call to extension 'missing-extension' (function 'test-function') failed"
      )
      entry.warningMessage should include("status code = 404")
    },
  )
}
