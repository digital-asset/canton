// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.FullUnassignmentTree
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.{ContractInstance, ReassignmentId}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.slf4j.event.Level

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

/** This integration test checks the behavior of participants that are declared as reassigning
  * participants but cannot act as such (because they are not connected to the source or target
  * synchronizer).
  *
  * Topology:
  *   - Synchronizers: da and acme
  *   - signatory -> P1, connected to both, and P3, connected to da only
  *   - observer -> P2 (malicious submitter), connected to both, P3, connected to da only, and P4,
  *     connected to acme only
  */
final class Phase7ReassigningParticipantIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers
    with EntitySyntax {

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var signatory: PartyId = _
  private var observer: PartyId = _

  private var maliciousP2: MaliciousParticipantNode = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1_S1M1
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

        // participant4 only knows the target synchronizer
        participant4.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant4.dars.upload(CantonExamplesPath, synchronizerId = acmeId)

        // participant3 hosts both parties on da, so that it is asked to confirm and can abstain
        def submissionOnP3(
            participantId: ParticipantId
        ): (PositiveInt, Set[(ParticipantId, ParticipantPermission)]) =
          (PositiveInt.one, Set(participantId -> Submission, participant3.id -> Submission))

        def submissionOn(
            participantId: ParticipantId
        ): (PositiveInt, Set[(ParticipantId, ParticipantPermission)]) =
          (PositiveInt.one, Set(participantId -> Submission))

        PartiesAllocator(participants.all.toSet)(
          Seq(
            "signatory" -> participant1,
            "observer" -> participant2,
          ),
          Map(
            "signatory" -> Map(
              daId -> submissionOnP3(participant1.id),
              acmeId -> submissionOn(participant1.id),
            ),
            "observer" -> Map(
              daId -> submissionOnP3(participant2.id),
              acmeId -> (
                PositiveInt.one,
                Set(participant2.id -> Submission, participant4.id -> Submission),
              ),
            ),
          ),
        )

        // Disable the automatic assignment, so that the assignment below is the only one submitted
        Seq(sequencer1, sequencer2).foreach { sequencer =>
          sequencer.topology.synchronizer_parameters.propose_update(
            sequencer.synchronizer_id,
            _.update(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
          )
        }

        signatory = "signatory".toPartyId(participant1)
        observer = "observer".toPartyId(participant2)

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)

        maliciousP2 = MaliciousParticipantNode(
          participant2,
          daId,
          testedProtocolVersion,
          defaultProtocolLimits,
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

  /** Unassignment request declaring `declared` as the reassigning participants. */
  private def unassignDeclaring(contract: ContractInstance, declared: Set[ParticipantId])(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val crypto = participant2.underlying.value.sync.syncCrypto
      .forSynchronizer(daId, staticSynchronizerParameters1)
      .value
      .pureCrypto

    val helpers = ReassignmentDataHelpers(
      contract = contract,
      sourceSynchronizer = Source(daId),
      targetSynchronizer = Target(acmeId),
      pureCrypto = crypto,
      targetTimestamp = Target(environment.clock.now),
    )

    val fullTree: FullUnassignmentTree = helpers
      .unassignmentRequest(
        observer.toLf,
        participant2,
        MediatorGroupRecipient(NonNegativeInt.zero),
      )(declared)
      .toFullUnassignmentTree(
        crypto,
        crypto,
        new SeedGenerator(crypto).generateSaltSeed(),
        UUID.randomUUID(),
      )

    maliciousP2.submitUnassignmentRequest(fullTree).futureValueUS.value
  }

  "A participant declared as reassigning that cannot resolve the target" should {

    "abstain on the request" in { implicit env =>
      import env.*

      participant3.synchronizers.is_registered(acmeName) shouldBe false

      val contract = createContract()

      loggerFactory.assertLogsSeq(
        SuppressionRule.LoggerNameContains("UnassignmentProcessingSteps") &&
          SuppressionRule.Level(Level.INFO)
      )(
        {
          unassignDeclaring(contract, Set(participant1.id, participant2.id, participant3.id))

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

    "alarm, do not write in the reassignment store, and keep processing when the verdict is an approve" in {
      implicit env =>
        import env.*

        participant3.synchronizers.is_registered(acmeName) shouldBe false

        val contract = createContract()

        loggerFactory.assertLogsSeq(
          SuppressionRule.LoggerNameContains("UnassignmentProcessingSteps") &&
            SuppressionRule.Level(Level.WARN)
        )(
          {
            replacingConfirmationResult(
              daId,
              sequencer1,
              mediator1,
              withMediatorVerdict(mediatorApprove),
            ) {
              unassignDeclaring(contract, Set(participant1.id, participant2.id, participant3.id))

              // Keeps the send policy installed until the unassignment result has gone through it.
              participant1.health.ping(participant2)
            }.discard

            // Proves that participant3 still commits on da after the result it could not process.
            participant3.health.ping(participant1)
          },
          entries =>
            forAtLeast(1, entries) { entry =>
              entry.loggerName should include("participant=participant3")
              entry.message should include("Unknown synchronizer")
              entry.message should include("Skipping storing unassignment data")
            },
        )

        participant3.testing.acs_search(daName, exactId = contract.contractId.coid) shouldBe empty
        participant3.ledger_api.state.acs.incomplete_unassigned_of_party(observer) shouldBe empty

        val incompleteOnP2 = participant2.ledger_api.state.acs
          .incomplete_unassigned_of_party(observer)
          .loneElement

        participant2.ledger_api.commands.submit_assign(
          observer,
          incompleteOnP2.reassignmentId,
          daId,
          acmeId,
        )

        participant2.testing
          .acs_search(acmeName, exactId = contract.contractId.coid)
          .map(_.contractId.coid) should contain(contract.contractId.coid)
    }
  }

  "A participant declared as reassigning that is connected to the target only" should {
    "alarm, do not write in the reassignment store, and keep processing when the assignment is approved" in {
      implicit env =>
        import env.*

        participant4.synchronizers.is_registered(daName) shouldBe false

        val contract = createContract()

        val unassigned = participant2.ledger_api.commands.submit_unassign(
          observer,
          Seq(contract.contractId),
          daId,
          acmeId,
        )

        val unassignmentData = participant2.underlying.value.sync.syncPersistentStateManager
          .get(acmeId)
          .value
          .reassignmentStore
          .lookup(ReassignmentId.tryCreate(unassigned.reassignmentId))
          .failOnShutdown
          .futureValue

        val maliciousP2OnAcme = MaliciousParticipantNode(
          participant2,
          acmeId,
          testedProtocolVersion,
          defaultProtocolLimits,
          timeouts,
          loggerFactory,
        )

        loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            replacingConfirmationResult(
              acmeId,
              sequencer2,
              mediator2,
              withMediatorVerdict(mediatorApprove),
            ) {
              maliciousP2OnAcme
                .submitAssignmentRequest(
                  observer.toLf,
                  unassignmentData.copy(reassigningParticipants =
                    unassignmentData.reassigningParticipants + participant4.id
                  ),
                )
                .futureValueUS
                .value

              // Keeps the send policy installed until the assignment result has gone through it.
              participant4.health.ping(participant2)
            }.discard

            participant4.health.ping(participant2)
          },
          entries =>
            forAtLeast(1, entries) { entry =>
              entry.loggerName should include("participant=participant4")
              entry.message should include("Unknown synchronizer")
              entry.message should include("Skipping storing assignment data")
            },
        )

        participant4.testing
          .acs_search(acmeName, exactId = contract.contractId.coid)
          .map(_.contractId.coid) should contain(contract.contractId.coid)

        participant4.ledger_api.state.acs.incomplete_assigned_of_party(observer) shouldBe empty

        participant4.health.ping(participant2)
    }
  }
}
