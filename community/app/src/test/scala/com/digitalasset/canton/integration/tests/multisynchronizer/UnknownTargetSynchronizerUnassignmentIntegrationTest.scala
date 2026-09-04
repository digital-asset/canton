// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.FullUnassignmentTree
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.ContractInstance
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.slf4j.event.Level

import java.util.UUID

/** A submitter picks the set of reassigning participants, so it can name a participant that does
  * not know the target synchronizer. That participant should not crash.
  *
  * Topology:
  *   - Synchronizers: da and acme
  *   - signatory -> P1, connected to both
  *   - observer -> P2 (malicious submitter) and P3, both hosting on da, only P2 on acme
  */
final class UnknownTargetSynchronizerUnassignmentIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with EntitySyntax {

  private var signatory: PartyId = _
  private var observer: PartyId = _

  private var maliciousP2: MaliciousParticipantNode = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag)
      .withSetup { implicit env =>
        import env.*

        Seq(participant1, participant2).synchronizers.connect_local(sequencer1, alias = daName)
        Seq(participant1, participant2).synchronizers.connect_local(sequencer2, alias = acmeName)
        Seq(participant1, participant2).dars.upload(CantonExamplesPath, synchronizerId = daId)
        Seq(participant1, participant2).dars.upload(CantonExamplesPath, synchronizerId = acmeId)

        // participant3 never learns about acme, so it cannot resolve it as a target
        participant3.synchronizers.connect_local(sequencer1, alias = daName)
        participant3.dars.upload(CantonExamplesPath, synchronizerId = daId)

        def submissionOnP3(
            participantId: ParticipantId
        ): (PositiveInt, Set[(ParticipantId, ParticipantPermission)]) =
          (PositiveInt.one, Set(participantId -> Submission, participant3.id -> Submission))

        PartiesAllocator(participants.all.toSet)(
          Seq(
            "signatory" -> participant1,
            "observer" -> participant2,
          ),
          Map(
            "signatory" -> Map(
              daId -> submissionOnP3(participant1.id),
              acmeId -> (PositiveInt.one, Set(participant1.id -> Submission)),
            ),
            "observer" -> Map(
              daId -> submissionOnP3(participant2.id),
              acmeId -> (PositiveInt.one, Set(participant2.id -> Submission)),
            ),
          ),
        )

        signatory = "signatory".toPartyId(participant1)
        observer = "observer".toPartyId(participant2)

        maliciousP2 = MaliciousParticipantNode(
          participant2,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

  private def createContract()(implicit env: TestConsoleEnvironment): ContractInstance = {
    import env.*

    val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer, 1000)
    participant1.testing
      .acs_search(daName, exactId = iou.id.contractId, limit = PositiveInt.one)
      .loneElement
  }

  /** Unassignment request declaring participant3 as a reassigning participant, which it is not. */
  private def unassignDeclaringP3(contract: ContractInstance)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val pureCrypto = participant2.underlying.value.sync.syncCrypto
      .forSynchronizer(daId, staticSynchronizerParameters1)
      .value
      .pureCrypto

    val helpers = ReassignmentDataHelpers(
      contract = contract,
      sourceSynchronizer = Source(daId),
      targetSynchronizer = Target(acmeId),
      pureCrypto = pureCrypto,
      targetTimestamp = Target(environment.clock.now),
    )

    val fullTree: FullUnassignmentTree = helpers
      .unassignmentRequest(
        observer.toLf,
        participant2,
        MediatorGroupRecipient(NonNegativeInt.zero),
      )(Set(participant1.id, participant2.id, participant3.id))
      .toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        new SeedGenerator(pureCrypto).generateSaltSeed(),
        new UUID(10L, 0L),
      )

    maliciousP2.submitUnassignmentRequest(fullTree).futureValueUS.value
  }

  "A participant declared as reassigning that cannot resolve the target synchronizer" should {

    "abstain on the request" in { implicit env =>
      import env.*

      participant3.synchronizers.is_registered(acmeName) shouldBe false

      val contract = createContract()

      loggerFactory.assertLogsSeq(
        SuppressionRule.LoggerNameContains("UnassignmentProcessingSteps") &&
          SuppressionRule.Level(Level.INFO)
      )(
        {
          unassignDeclaringP3(contract)

          // The ping is sequenced after the forged request, so P3 answering it proves that P3
          // processed past the request instead of giving up on the subscription.
          participant3.health.ping(participant1)
        },
        entries =>
          forAtLeast(1, entries) { entry =>
            entry.loggerName should include("participant=participant3")
            entry.message should include("Sending an abstain verdict")
            entry.message should include("Unknown synchronizer")
          },
      )
    }
  }
}
