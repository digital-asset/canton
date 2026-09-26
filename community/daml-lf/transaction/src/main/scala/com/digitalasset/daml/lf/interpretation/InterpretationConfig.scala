// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package interpretation

import com.digitalasset.daml.lf.language.LanguageVersion

case class InterpretationConfig(
    allowedLanguageVersions: List[LanguageVersion]
)

object InterpretationConfig {
  val Stable: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = LanguageVersion.stableLfVersions
  )
  val Default: InterpretationConfig = Stable
  val Dev: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = LanguageVersion.allLfVersions
  )
}
