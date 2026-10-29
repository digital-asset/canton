// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.daml.ledger.api.v2 as proto
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.GrpcAdminCommandSupport.*
import com.digitalasset.canton.integration.util.GrpcServices.StateService
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.PartyId

abstract class GetConnectedSynchronizerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  private lazy val plugin =
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1"), InstanceName.tryCreate("sequencer2")),
          Set(InstanceName.tryCreate("sequencer3"), InstanceName.tryCreate("sequencer4")),
        )
      ),
    )

  registerPlugin(plugin)
  registerPlugin(new UsePostgres(loggerFactory))

  private val party1Name = "party1"
  private val party2Name = "party2"

  private var party1: PartyId = _
  private var party2: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S2M1_S2M1
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer2, alias = daName)

        participant1.synchronizers.connect_local(sequencer3, alias = acmeName)
        participant2.synchronizers.connect_local(sequencer4, alias = acmeName)

        // Allocate parties
        party1 = participant1.parties.enable(
          party1Name,
          synchronizeParticipants = Seq(participant2),
          synchronizer = daName,
        )
        participant1.parties.enable(
          party1Name,
          synchronizeParticipants = Seq(participant2),
          synchronizer = acmeName,
        )

        party2 = participant2.parties.enable(
          party2Name,
          synchronizeParticipants = Seq(participant1),
          synchronizer = daName,
        )
        participant2.parties.enable(
          party2Name,
          synchronizeParticipants = Seq(participant1),
          synchronizer = acmeName,
        )

        participant2.synchronizers.disconnect(acmeName)
      }

  protected def getConnectedSynchronizers(
      party: Option[PartyId],
      participant: LocalParticipantReference,
  ): proto.state_service.GetConnectedSynchronizersResponse =
    participant
      .runLapiAdminCommand(
        StateService.getConnectedSynchronizers(
          proto.state_service
            .GetConnectedSynchronizersRequest(
              party = party.fold("")(_.toLf),
              participantId = "",
              identityProviderId = "",
            )
        )
      )
      .tryResult

  private def testGetConnectedSynchronizers(
      party: Option[PartyId],
      participant: LocalParticipantReference,
      expectedSynchronizerIds: Seq[String],
      expectedPermission: proto.state_service.ParticipantPermission,
  ): Unit = {
    val response = getConnectedSynchronizers(party, participant)
    response.connectedSynchronizers.map(
      _.synchronizerId
    ) should contain theSameElementsAs expectedSynchronizerIds
    response.connectedSynchronizers.map(
      _.permission
    ) should contain theSameElementsAs expectedSynchronizerIds.map(_ => expectedPermission)
  }

  "StateService.GetConnectedSynchronizers" should {
    "list connected synchronizers if any" in { implicit env =>
      import env.*

      testGetConnectedSynchronizers(
        Some(party1),
        participant1,
        Seq(daId.logical.toProtoPrimitive, acmeId.logical.toProtoPrimitive),
        proto.state_service.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
      )

      testGetConnectedSynchronizers(
        Some(party2),
        participant2,
        Seq(daId.logical.toProtoPrimitive),
        proto.state_service.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
      )

      testGetConnectedSynchronizers(
        None,
        participant1,
        Seq(daId.logical.toProtoPrimitive, acmeId.logical.toProtoPrimitive),
        proto.state_service.ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED,
      )

      testGetConnectedSynchronizers(
        None,
        participant2,
        Seq(daId.logical.toProtoPrimitive),
        proto.state_service.ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED,
      )

      // disconnect a synchronizer
      participant2.synchronizers.disconnect(daName)
      testGetConnectedSynchronizers(
        Some(party2),
        participant2,
        Seq.empty,
        proto.state_service.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
      )
      testGetConnectedSynchronizers(
        None,
        participant2,
        Seq.empty,
        proto.state_service.ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED,
      )
    }
  }
}

class ReferenceGetConnectedSynchronizersIntegrationTest
    extends GetConnectedSynchronizerIntegrationTest
