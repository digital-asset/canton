// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package interpretation

import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{NextGenContractStateMachine => ContractStateMachine}

case class InterpretationConfig(
    allowedLanguageVersions: List[LanguageVersion],
    contractStateMode: ContractStateMachine.Mode

)

object InterpretationConfig {
  val Default: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = LanguageVersion.stableLfVersions,
    contractStateMode = ContractStateMachine.Mode.NUCK
  )
  val Dev: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = LanguageVersion.allLfVersions,
    contractStateMode = ContractStateMachine.Mode.NUCK
  )
  val Legacy: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = List(LanguageVersion.v2_1, LanguageVersion.v2_2),
    contractStateMode = ContractStateMachine.Mode.NoKey
  )
  def Key = Default
  def NoKey = Legacy

}
