// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.config

import com.digitalasset.canton.config.DeprecatedConfigUtils.DeprecatedFieldsFor
import com.digitalasset.canton.config.{
  DeprecatedConfigUtils,
  InitConfigBase,
  NonNegativeFiniteDuration,
}
import com.digitalasset.canton.participant.config.ParticipantInitConfig.{
  ParticipantLedgerApiInitConfig,
  ParticipantParametersInitConfig,
}

/** Init configuration specific to participant nodes
  * @param ledgerApi ledgerApi related init config
  * @param parameters participant init config parameters
  */
final case class ParticipantInitConfig(
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
    * @param maxDeduplicationDuration The max deduplication duration reported by the participant's ledger configuration service.
    *                                 This duration defines a lower bound on the durations that the participant's command and command submission services accept as command deduplication durations.
    */
  final case class ParticipantLedgerApiInitConfig(
      maxDeduplicationDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(7L)
  )

  /** Init configuration of the ledger API for participant nodes
    * @param uniqueContractKeys Whether the participant can connect only to a single domain that has [[com.digitalasset.canton.protocol.StaticDomainParameters.uniqueContractKeys]] set
    */
  final case class ParticipantParametersInitConfig(
      uniqueContractKeys: Boolean = true
  )
}
