// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.config.DeprecatedConfigUtils.DeprecatedFieldsFor
import com.digitalasset.canton.config.{DeprecatedConfigUtils, InitConfigBase}
import com.digitalasset.canton.participant.config.ParticipantInitConfig.{
  ParticipantLedgerApiInitConfig,
  ParticipantParametersInitConfig,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration

/** Init configuration specific to participant nodes
  * @param ledgerApi ledgerApi related init config
  * @param parameters participant init config parameters
  */
case class ParticipantInitConfig(
    identity: Option[InitConfigBase.Identity] = Some(InitConfigBase.Identity()),
    ledgerApi: ParticipantLedgerApiInitConfig = ParticipantLedgerApiInitConfig(),
    parameters: ParticipantParametersInitConfig = ParticipantParametersInitConfig(),
) extends InitConfigBase

object ParticipantInitConfig {

  // TODO(i10108): remove when backwards compatibility can be discarded
  /** Adds deprecations specific to DomainBaseConfig
    */
  object DeprecatedImplicits {
    implicit def deprecatedParticipantInitConfig[X <: ParticipantInitConfig]
        : DeprecatedFieldsFor[X] =
      new DeprecatedFieldsFor[ParticipantInitConfig] {
        override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
          DeprecatedConfigUtils.MovedConfigPath(
            "generate-legal-identity-certificate",
            "identity.generate-legal-identity-certificate",
          )
        )
      }
  }

  /** Init configuration of the ledger API for participant nodes
    * * @param maxDeduplicationDuration  Max deduplication duration of the participant's ledger configuration.
    */
  case class ParticipantLedgerApiInitConfig(
      maxDeduplicationDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(7L)
  )

  /** Init configuration of the ledger API for participant nodes
    * @param uniqueContractKeys Whether the participant can connect only to a single domain that has [[com.digitalasset.canton.protocol.StaticDomainParameters.uniqueContractKeys]] set
    * @param unsafeEnableCausalityTracking Experimental. Ensures that the event ordering on the participant node is consistent even for cross domain contracts
    */
  case class ParticipantParametersInitConfig(
      uniqueContractKeys: Boolean = true,
      unsafeEnableCausalityTracking: Boolean = false,
  )
}
