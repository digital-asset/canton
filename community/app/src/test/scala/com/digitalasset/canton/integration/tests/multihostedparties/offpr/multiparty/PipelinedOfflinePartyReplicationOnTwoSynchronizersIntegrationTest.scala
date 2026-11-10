// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr.multiparty

import com.digitalasset.canton.admin.api.client.data.FlagNotSet
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.AutomaticAssignment
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.Party

/** Scenario: two parties hosted on two synchronizers, with contracts on both, are replicated step
  * by step on both. The target disconnects from both, each party authorizes its activation on one
  * synchronizer and then on the other, one at a time, the source exports one snapshot per party and
  * synchronizer from one `beginOffsetExclusive`, and the target imports all snapshots before it
  * reconnects to both. Between the parties' activations on the two synchronizers, the source
  * reassigns a contract of both parties from the first synchronizer to the second.
  *
  * Goal: the reassigned contract is in the first party's snapshot on both synchronizers, as it is
  * active on each of them at that party's activation there, and the target ends up with the
  * source's ACS on each synchronizer.
  */
final class PipelinedOfflinePartyReplicationOnTwoSynchronizersIntegrationTest
    extends PipelinedOfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
      )
      .withSetup { implicit env =>
        import env.*
        AutomaticAssignment.disable(Seq(sequencer1), daId)
        AutomaticAssignment.disable(Seq(sequencer2), acmeId)
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.synchronizers.connect_local(sequencer2, acmeName)
        Seq(sequencer1 -> daId, sequencer2 -> acmeId).foreach { case (sequencer, synchronizerId) =>
          participants.local.foreach(
            _.dars.upload(CantonExamplesPath, synchronizerId = synchronizerId)
          )
          sequencer.topology.synchronizer_parameters
            .propose_update(
              synchronizerId,
              _.update(reconciliationInterval = reconciliationInterval),
            )
        }
        source = participant1
        target = participant3
      }

  override protected def clearOnboardingFlags(
      parties: Seq[Party],
      targetLedgerEnd: Long,
      to: LocalParticipantReference,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    passDecisionTimeouts()
    eventually() {
      source.health.ping(to)
      for {
        synchronizerId <- Seq(daId, acmeId)
        party <- parties
      } to.parties.clear_party_onboarding_flag(
        party,
        synchronizerId.logical,
        targetLedgerEnd,
      ) shouldBe FlagNotSet
    }
  }

  "Parties replicated step by step on two synchronizers get the source's ACS on each, also of a contract reassigned between their activations" in {
    implicit env =>
      import env.*
      val parties = Seq("Alder", "Birch").map { name =>
        val party = source.parties.enable(
          name,
          synchronizer = Some(daName),
          synchronizeParticipants = Seq(target),
        )
        source.parties
          .enable(name, synchronizer = Some(acmeName), synchronizeParticipants = Seq(target))
          .discard
        party
      }
      val Seq(alder, birch) = parties: @unchecked
      IouSyntax.createIou(source, Some(daId))(alder, birch).discard
      IouSyntax.createIou(source, Some(acmeId))(alder, birch).discard
      val reassigned = IouSyntax.createIou(source, Some(daId))(alder, birch)
      val reassignedId = LfContractId.assertFromString(reassigned.id.contractId)

      Seq(daId, acmeId).foreach(synchronizerId => proposeOnTarget(parties, Some(synchronizerId)))
      target.synchronizers.disconnect_all()
      val beforeActivationOffset = source.ledger_api.state.end()
      val daActivations = authorizeOnSourceOneByOne(parties, beforeActivationOffset, Some(daId))
      // No timeout: the target is disconnected
      val unassigned = source.ledger_api.commands
        .submit_unassign(alder, Seq(reassignedId), daId, acmeId, timeout = None)
      source.ledger_api.commands
        .submit_assign(alder, unassigned.reassignmentId, daId, acmeId, timeout = None)
        .discard
      val acmeActivations =
        authorizeOnSourceOneByOne(parties, beforeActivationOffset, Some(acmeId))
      partiesSharingAnOffset(daActivations ++ acmeActivations) shouldBe empty

      val daFiles = exportAll(parties, beforeActivationOffset, Some(daId))
      val acmeFiles = exportAll(parties, beforeActivationOffset, Some(acmeId))
      Seq(daFiles, acmeFiles).foreach(files =>
        snapshotContractIds(files(alder)) should contain(reassigned.id.contractId)
      )
      importAll(daFiles, daId)
      importAll(acmeFiles, acmeId)
      val targetLedgerEnd = target.ledger_api.state.end()
      target.synchronizers.reconnect_all()

      clearOnboardingFlags(parties, targetLedgerEnd)
      assertSameAcsOnSourceAndTarget(parties)
  }
}
