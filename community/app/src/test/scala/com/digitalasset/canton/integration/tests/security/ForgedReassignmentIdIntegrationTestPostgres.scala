// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.{CantonTimestamp, UnassignmentData}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{AcsInspection, EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.protocol.reassignment.{
  AssignmentProcessor,
  ReassignmentDataHelpers,
}
import com.digitalasset.canton.protocol.messages.Verdict
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId, ReassignmentId}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference

/** This integration test checks that participants commit an assignment approved by the mediator
  * whose reassignment id is either forged or reused. This is the expected behaviour: once the
  * mediator approves, a participant that refuses to commit forks the ledger. They commit, they do
  * not crash, and the contract ends up active on the target synchronizer on all three participants:
  * the reassigning participants behave like the others. The only issue is the indexer of the
  * reassigning participants, which fails when the reassignment id is reused. Topology:
  *   - Synchronizers: da (source) and acme (target)
  *   - signatory -> P1 (malicious submitter), connected to both
  *   - observer -> P2, connected to both, and P3, connected to acme only
  */
final class ForgedReassignmentIdIntegrationTestPostgres
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers
    with EntitySyntax
    with AcsInspection {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var signatory: PartyId = _
  private var observer: PartyId = _

  private var maliciousP1: MaliciousParticipantNode = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag)
      .withSetup { implicit env =>
        import env.*

        val signatoryName = "signatory"
        val observerName = "observer"
        Seq(participant1, participant2).synchronizers.connect_local(sequencer1, alias = daName)
        Seq(participant1, participant2).synchronizers.connect_local(sequencer2, alias = acmeName)
        participant3.synchronizers.connect_local(sequencer2, alias = acmeName)

        Seq(participant1, participant2).dars.upload(CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = acmeId)

        PartiesAllocator(participants.all.toSet)(
          Seq(signatoryName -> participant1, observerName -> participant2),
          Map(
            signatoryName -> Map(
              daId -> (PositiveInt.one, Set((participant1, Submission))),
              acmeId -> (PositiveInt.one, Set((participant1, Submission))),
            ),
            observerName -> Map(
              daId -> (PositiveInt.one, Set((participant2, Submission))),
              acmeId -> (PositiveInt.one, Set(
                (participant2, Submission),
                (participant3, Submission),
              )),
            ),
          ),
        )

        // Disable the automatic assignment, so that the assignments below are the only ones submitted
        Seq(sequencer1, sequencer2).foreach { sequencer =>
          sequencer.topology.synchronizer_parameters.propose_update(
            sequencer.synchronizer_id,
            _.update(assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero),
          )
        }

        signatory = signatoryName.toPartyId(participant1)
        observer = observerName.toPartyId(participant2)

        pureCryptoRef.set(sequencer2.crypto.pureCrypto)

        maliciousP1 = MaliciousParticipantNode(
          participant1,
          acmeId,
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

  /** Unassignment data for `contract` that was never sequenced on the source synchronizer. */
  private def forgedUnassignmentData(
      contract: ContractInstance,
      unassignmentTs: CantonTimestamp,
  )(implicit env: TestConsoleEnvironment): UnassignmentData = {
    import env.*

    val helpers = ReassignmentDataHelpers(
      contract = contract,
      sourceSynchronizer = Source(daId),
      targetSynchronizer = Target(acmeId),
      pureCrypto = pureCrypto,
      targetTimestamp = Target(environment.clock.now),
    )

    val tree = helpers.fullUnassignmentTree(
      signatory.toLf,
      participant1,
      MediatorGroupRecipient(NonNegativeInt.zero),
    )(reassigningParticipants = Set(participant1.id, participant2.id))

    UnassignmentData(tree, unassignmentTs)
  }

  private def lookupUnassignmentData(
      participant: LocalParticipantReference,
      reassignmentId: String,
  )(implicit env: TestConsoleEnvironment): UnassignmentData = {
    import env.*

    participant.underlying.value.sync.syncPersistentStateManager
      .get(acmeId)
      .value
      .reassignmentStore
      .lookup(ReassignmentId.tryCreate(reassignmentId))
      .failOnShutdown
      .futureValue
  }

  private def assignApproved(data: UnassignmentData)(
      synchronize: => Unit
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    replacingConfirmationResult(
      acmeId,
      sequencer2,
      mediator2,
      withMediatorVerdict(Verdict.Approve(testedProtocolVersion)),
    ) {
      maliciousP1.submitAssignmentRequest(signatory.toLf, data).futureValueUS.value
      synchronize
    }.discard
  }

  private def assertNotInAcs(
      participants: Seq[LocalParticipantReference],
      synchronizer: com.digitalasset.canton.SynchronizerAlias,
      cid: LfContractId,
  ): Unit =
    participants.foreach { participant =>
      eventually() {
        participant.testing.acs_search(synchronizer, exactId = cid.coid) shouldBe empty
      }
    }

  "An approved assignment with a consistent reassignment id" should {
    "be committed by all participants when forging a new reassignment" in { implicit env =>
      import env.*

      val contract = createContract()
      val cid = contract.contractId

      assignApproved(forgedUnassignmentData(contract, environment.clock.now))(
        participant2.health.ping(participant3, synchronizerId = Some(acmeId))
      )

      assertInAcsSync(Seq(participant1, participant2), daName, cid)
      assertInAcsSync(Seq(participant1, participant2, participant3), acmeName, cid)

      val unassigned =
        participant1.ledger_api.commands.submit_unassign(signatory, Seq(cid), daId, acmeId)

      loggerFactory.assertEventuallyLogsSeq(
        (SuppressionRule.LoggerNameContains("AssignmentProcessingSteps") ||
          SuppressionRule.forLogger[AssignmentProcessor]) && SuppressionRule.Level(Level.WARN)
      )(
        replacingConfirmationResult(
          acmeId,
          sequencer2,
          mediator2,
          withMediatorVerdict(Verdict.Approve(testedProtocolVersion)),
        ) {
          participant1.ledger_api.commands
            .submit_assign(signatory, unassigned.reassignmentId, daId, acmeId)
        }.discard,
        entries => {
          forAtLeast(1, entries) { entry =>
            entry.loggerName should include("participant=participant1")
            entry.message should include("LOCAL_VERDICT_ACTIVATES_EXISTING_CONTRACTS")
          }
          forEvery(Seq("participant1", "participant2", "participant3")) { participant =>
            forAtLeast(1, entries) { entry =>
              entry.loggerName should include(s"participant=$participant")
              entry.message should include("ReassignmentCounterShouldIncrease")
            }
          }
        },
      )

      assertNotInAcs(Seq(participant1, participant2), daName, cid)
      assertInAcsSync(Seq(participant1, participant2, participant3), acmeName, cid)

      Seq(participant1, participant2, participant3).foreach(_.health.ping(participant1))
    }

    "be committed by all participants when reusing a reassignment id" in { implicit env =>
      import env.*

      val contract = createContract()
      val cid = contract.contractId

      val unassigned =
        participant1.ledger_api.commands.submit_unassign(signatory, Seq(cid), daId, acmeId)
      val data = lookupUnassignmentData(participant1, unassigned.reassignmentId)
      participant1.ledger_api.commands
        .submit_assign(signatory, unassigned.reassignmentId, daId, acmeId)
        .discard

      loggerFactory.assertEventuallyLogsSeq(
        SuppressionRule.LevelAndAbove(Level.WARN) ||
          (SuppressionRule.LoggerNameContains("ParallelIndexerFactory") &&
            SuppressionRule.Level(Level.INFO))
      )(
        // participant3 is not a reassigning participant, so its ledger API keeps working
        assignApproved(data)(
          participant3.health.ping(participant3, synchronizerId = Some(acmeId))
        ),
        // TODO(#23636): The indexer of reassigning participants cannot store the global offset of
        //  a second assignment with the same reassignment id in the reassignment store and keeps failing
        entries =>
          forEvery(Seq("participant1", "participant2")) { participant =>
            forAtLeast(1, entries) { entry =>
              entry.loggerName should include(s"participant=$participant")
              entry.message should include("Unable to merge assignment offsets")
            }
          },
      )

      assertNotInAcs(Seq(participant1, participant2), daName, cid)
      assertInAcsSync(Seq(participant1, participant2, participant3), acmeName, cid)

      participant3.health.ping(participant3, synchronizerId = Some(acmeId))
    }
  }
}
