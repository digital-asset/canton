// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package interpretation

import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.NextGenContractStateMachine as ContractStateMachine
import com.google.common.annotations.VisibleForTesting

case class InterpretationConfig(
    allowedLanguageVersions: List[LanguageVersion],
    contractStateMode: ContractStateMachine.Mode,
)

object InterpretationConfig {
  val V34: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = List(LanguageVersion.v2_1, LanguageVersion.v2_2),
    contractStateMode = ContractStateMachine.Mode.NoKey,
  )
  val V35: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions =
      List(LanguageVersion.v2_1, LanguageVersion.v2_2, LanguageVersion.v2_3),
    contractStateMode = ContractStateMachine.Mode.Key,
  )
  val V36: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions =
      List(LanguageVersion.v2_1, LanguageVersion.v2_2, LanguageVersion.v2_3, LanguageVersion.v2_4),
    contractStateMode = ContractStateMachine.Mode.Key,
  )
  val Dev: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = LanguageVersion.allLfVersions,
    contractStateMode = ContractStateMachine.Mode.Key,
  )
  def Default = V36
  @VisibleForTesting
  def Legacy = V34
  @VisibleForTesting
  private[lf] def Key = Default
  @VisibleForTesting
  private[lf] def NoKey = Legacy
}
