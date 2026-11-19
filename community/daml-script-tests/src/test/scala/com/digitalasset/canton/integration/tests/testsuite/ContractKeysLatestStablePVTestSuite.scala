// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests
package testsuite

import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion

/** This is the `FeatureContractKeyTestSuite`, but restricted to the latest protocol version.
  */
object ContractKeysLatestStablePVTestSuite extends DamlScriptTestSuite {
  override val projectName = FeatureContractKeyTestSuite.projectName

  override val expectedResults = FeatureContractKeyTestSuite.expectedResults

  override def isEnabledFor(
      protocolVersion: ProtocolVersion,
      languageVersion: LanguageVersion,
  ): Boolean =
    FeatureContractKeyTestSuite.isEnabledFor(
      protocolVersion,
      languageVersion,
    ) && protocolVersion == ProtocolVersion.latest
}
