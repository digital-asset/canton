// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.environment.{CantonNodeParameters, HasGeneralCantonNodeParameters}
import com.digitalasset.canton.participant.admin.AdminWorkflowConfig
import com.digitalasset.canton.participant.config.{
  LedgerApiServerParametersConfig,
  ParticipantProtocolConfig,
  ParticipantStoreConfig,
  PartyNotificationConfig,
}
import com.digitalasset.canton.version.ProtocolVersion

case class ParticipantNodeParameters(
    general: CantonNodeParameters.General,
    partyChangeNotification: PartyNotificationConfig,
    adminWorkflow: AdminWorkflowConfig,
    maxUnzippedDarSize: Int,
    stores: ParticipantStoreConfig,
    transferTimeProofFreshnessProportion: NonNegativeInt,
    protocolConfig: ParticipantProtocolConfig,
    uniqueContractKeys: Boolean,
    enableCausalityTracking: Boolean,
    unsafeEnableDamlLfDevVersion: Boolean,
    ledgerApiServerParameters: LedgerApiServerParametersConfig,
    maxDbConnections: Int,
    excludeInfrastructureTransactions: Boolean,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters {
  override def dontWarnOnDeprecatedPV: Boolean = protocolConfig.dontWarnOnDeprecatedPV
  override def devVersionSupport: Boolean = protocolConfig.devVersionSupport
  override def initialProtocolVersion: ProtocolVersion = protocolConfig.initialProtocolVersion
}
