// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package interpretation

import com.digitalasset.daml.lf.language.LanguageVersion
import com.google.common.annotations.VisibleForTesting

case class InterpretationConfig(
    allowedLanguageVersions: List[LanguageVersion]
)

object InterpretationConfig {
  val V36: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions =
      List(LanguageVersion.v2_1, LanguageVersion.v2_2, LanguageVersion.v2_3, LanguageVersion.v2_4)
  )
  val V37: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions =
      List(LanguageVersion.v2_1, LanguageVersion.v2_2, LanguageVersion.v2_3, LanguageVersion.v2_4)
  )
  val Dev: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = LanguageVersion.allLfVersions
  )
  def Default = V36
  @VisibleForTesting
  def Stable = Default
}
