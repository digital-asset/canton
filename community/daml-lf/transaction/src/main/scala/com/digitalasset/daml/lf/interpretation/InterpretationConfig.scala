// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package interpretation

import com.digitalasset.daml.lf.language.LanguageVersion
import com.google.common.annotations.VisibleForTesting

case class InterpretationConfig(
    allowedLanguageVersions: List[LanguageVersion],
    errorOnContractKeyMismatch: Boolean,
)

object InterpretationConfig {
  val V36: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions =
      List(LanguageVersion.v2_1, LanguageVersion.v2_2, LanguageVersion.v2_3, LanguageVersion.v2_4),
    errorOnContractKeyMismatch = false,
  )
  val V37: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions =
      List(LanguageVersion.v2_1, LanguageVersion.v2_2, LanguageVersion.v2_3, LanguageVersion.v2_4),
    errorOnContractKeyMismatch = true,
  )
  val Dev: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = LanguageVersion.allLfVersions,
    errorOnContractKeyMismatch = true,
  )
  def Default = V36
  @VisibleForTesting
  def Stable = Default
}
