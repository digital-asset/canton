// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeProportion, PositiveInt}
import com.digitalasset.canton.config.{CommitmentSendDelay, DbConfig, PositiveDurationSeconds}
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  EntitySyntax,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
  PartiesAllocator,
}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.{BaseTest, config}
import monocle.macros.syntax.lens.*

import java.time.Duration as JDuration
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.*
import scala.util.chaining.*

/** In this test, we check how reassignments interact with pruning.
  *
  * Topology:
  *   - Two participants connected to both da and acme
  *   - Two parties (Alice, Bank) multi-hosted on P1 and P2 on both synchronizers. Confirmation
  *     threshold is one.
  */
sealed trait ReassignmentPruningIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasCycleUtils
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers {

  private var alice: PartyId = _
  private var bank: PartyId = _
  private var offsetP1AfterUnassign: Long = _
  private var offsetP2AfterUnassign: Long = _
  private var offsetP1AfterAssign: Long = _
  private var offsetP2AfterAssign: Long = _

  // These three parameters are needed to be able to wait sufficiently long to trigger a pruning timeout
  private val reconciliationInterval = JDuration.ofSeconds(10)
  private val confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)
  private val mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfSeconds(5)

  // Pick a low max dedup duration so that we don't delay pruning unnecessarily
  private val maxDedupDuration = JDuration.ofSeconds(5)
  private val pruningTimeout =
    Ordering[JDuration].max(
      reconciliationInterval
        .plus(confirmationResponseTimeout.duration)
        .plus(mediatorReactionTimeout.duration),
      maxDedupDuration,
    )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        // To ensure that pruning can happen quickly
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
        ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
      )
      // Don't delay sending ACS commitments
      .updateTestingConfig(
        _.focus(_.commitmentSendDelay).replace(
          Some(
            CommitmentSendDelay(
              Some(NonNegativeProportion.zero),
              Some(NonNegativeProportion.zero),
            )
          )
        )
      )
      .withSetup { implicit env =>
        import env.*

        Seq((sequencer1, daId), (sequencer2, acmeId)).foreach { case (sequencer, synchronizerId) =>
          // Disable automatic assignment
          sequencer.topology.synchronizer_parameters.propose_update(
            synchronizerId,
            _.update(
              assignmentExclusivityTimeout = config.NonNegativeFiniteDuration.Zero,
              reconciliationInterval = PositiveDurationSeconds(reconciliationInterval),
              confirmationResponseTimeout = confirmationResponseTimeout.toConfig,
              mediatorReactionTimeout = mediatorReactionTimeout.toConfig,
            ),
          )
        }

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)

        participant1.health.ping(participant2.id)

        withClue("Prepare parties") {
          val syncPermissions = Set[(ParticipantId, ParticipantPermission)](
            (participant1.id, ParticipantPermission.Submission),
            (participant2.id, ParticipantPermission.Submission),
          )

          val permissions = Map(
            daId -> (PositiveInt.one, syncPermissions),
            acmeId -> (PositiveInt.one, syncPermissions),
          )

          PartiesAllocator(Set(participant1, participant2))(
            newParties = Seq("Alice" -> participant1, "Bank" -> participant1),
            targetTopology = Map(
              "Alice" -> permissions,
              "Bank" -> permissions,
            ),
          )

          alice = "Alice".toPartyId()
          bank = "Bank".toPartyId()
        }

        participants.all.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(BaseTest.CantonExamplesPath, synchronizerId = acmeId)
        participant1.health.ping(participant2.id)
      }

  private def ensureOffsetSafeToPrune(
      desiredPruningOffset: Long,
      clock: SimClock,
      participant: LocalParticipantReference,
  ): Unit = {
    eventually() {
      val safeOffset = participant.pruning
        .find_safe_offset(clock.now.toInstant)
        .value
      safeOffset should be >= desiredPruningOffset
    }
    participant.pruning.prune(desiredPruningOffset)
  }

  // assigns the given iou from origin to target
  // returns the assignment offset on participant1 and the assignment offset on participant2
  private def assignIou(origin: SynchronizerId, target: SynchronizerId, reassignmentId: String)(
      implicit env: TestConsoleEnvironment
  ): (Long, Long) = {
    import env.*

    val ledgerEndP2BeforeAssign =
      participant2.ledger_api.state.end()
    val res = participant1.ledger_api.commands.submit_assign(alice, reassignmentId, origin, target)
    val assignOffsetP1 = res.reassignment.offset

    val assignOffsetP2 = participant2.ledger_api.updates
      .reassignments(
        partyIds = Set(bank),
        completeAfter = 1,
        beginOffsetExclusive = ledgerEndP2BeforeAssign,
      )
      .collectFirst { case wrapper: UpdateService.ReassignmentWrapper =>
        wrapper.reassignment.offset
      }
      .value

    (assignOffsetP1, assignOffsetP2)
  }

  /** Wait until P2 safe to prune timestamp reaches the provided timestamp.
    *
    * Returns the corresponding offset.
    */
  private def waitP2SafeTsReaches(
      targetTs: CantonTimestamp,
      targetOffset: Long,
  )(implicit env: TestConsoleEnvironment): Long = {
    import env.*

    eventually() {
      environment.simClock.value.advance(15.seconds.toJava)
      participant1.health.ping(participant2, synchronizerId = Some(env.daId))
      participant1.health.ping(participant2, synchronizerId = Some(env.acmeId))

      val computedSafeOffset =
        participant2.pruning.find_safe_offset(beforeOrAt = environment.clock.now.toInstant).value

      val safeTimestamp =
        participant2.underlying.value.sync.ledgerApiIndexer.asEval.value.ledgerApiStore
          .lastSynchronizerOffsetBeforeOrAt(
            env.daId,
            Offset.tryFromLong(computedSafeOffset),
          )
          .futureValueUS
          .value
          .recordTime
          .pipe(t => CantonTimestamp.assertFromInstant(t.toInstant))

      safeTimestamp should be > targetTs
      computedSafeOffset should be > targetOffset

      logger.debug(
        s"Got safeTimestamp=$safeTimestamp and offset=$computedSafeOffset when querying with targetTs=$targetTs"
      )

      computedSafeOffset
    }
  }

  "Pruning" should {
    /*
      Despite an incomplete reassignment, pruning can be done
      After pruning, the incomplete reassignment can still be queried
     */
    "not be blocked by incomplete reassignments" in { implicit env =>
      import env.*

      // Iou that will not get pruned
      IouSyntax.createIou(participant1, Some(daId))(bank, alice, 1.0)

      val transientIou1 = IouSyntax.createIou(participant1, Some(daId))(bank, alice, 2.0)
      IouSyntax.archive(participant1, Some(daId))(transientIou1, bank)

      val reassignmendId = withClue("Create an unassign an Iou") {
        val iou = IouSyntax.createIou(participant1, Some(daId))(bank, alice, 3.0)
        val iouCid = LfContractId.assertFromString(iou.id.contractId)

        participant1.ledger_api.commands
          .submit_unassign(alice, Seq(iouCid), daId, acmeId)
          .reassignmentId
      }

      val transientIou2 = IouSyntax.createIou(participant1, Some(daId))(bank, alice, 4.0)
      val transientIou2Archived = IouSyntax
        .archive(participant1, Some(daId))(transientIou2, bank)

      val offset =
        waitP2SafeTsReaches(transientIou2Archived.recordTime.value, transientIou2Archived.offset)

      // safe to prune offset progresses past the second transient contract (and thus past the unassignment)
      offset should be >= transientIou2Archived.offset

      participant2.pruning.prune(offset)

      // Despite pruning, the incomplete unassigned can still be found in the store
      val incompleteUnassigned = participant2.ledger_api.state.acs
        .incomplete_unassigned_of_party(bank)
        .loneElement

      incompleteUnassigned.reassignmentId shouldBe reassignmendId
    }

    "prune completed reassignments, not prune incomplete ones" in { implicit env =>
      import env.*

      val clock = environment.simClock.value

      // create iou on daId with Alice and the Bank as stakeholders
      val contractId = IouSyntax.createIou(env.participant2, Some(daId))(bank, alice).id

      // Prepare reassignment of the Iou from daId to acmeId
      // First unassign the Iou from daId to acmeId
      val reassignmentId = participant2.ledger_api.commands
        .submit_unassign(
          bank,
          Seq(contractId.toLf),
          daId,
          acmeId,
        )
        .reassignmentId

      // save the offsets to test acs snapshots
      offsetP1AfterUnassign = participant1.ledger_api.state.end()
      offsetP2AfterUnassign = participant2.ledger_api.state.end()

      // Make sure the unassignment is observed in acs commitments on daId, and all txns with earlier offsets are
      // observed in acs commitments on both synchronizers
      val wait = reconciliationInterval
      val waitx2 = wait.multipliedBy(2)

      val baseTime0 = clock.now
      val newTime0 = baseTime0.add(waitx2)
      clock.advanceTo(newTime0)

      participants.all.foreach(_.testing.fetch_synchronizer_times())

      participant1.testing.await_synchronizer_time(daId, newTime0, 5.seconds)
      participant2.testing.await_synchronizer_time(acmeId, newTime0, 5.seconds)

      participant1.health.ping(participantId = participant2, synchronizerId = Some(daId))
      participant1.health.ping(participantId = participant2, synchronizerId = Some(acmeId))

      // Wait for the pruning timeout
      val baseTime1 = clock.now
      val newTime1 = baseTime1.add(pruningTimeout)
      clock.advanceTo(newTime1)

      participants.all.foreach(_.testing.fetch_synchronizer_times())

      // Complete the unassignment by submitting the assign from daId to acmeId
      val (assignOffsetP1, assignOffsetP2) = assignIou(daId, acmeId, reassignmentId)

      // save the offsets to test acs snapshots later
      offsetP1AfterAssign = participant1.ledger_api.state.end()
      offsetP2AfterAssign = participant2.ledger_api.state.end()

      // Make sure the assignment is observed in acs commitments on acmeId, and all txns with earlier offsets are
      // observed in acs commitments on both synchronizers
      val baseTime2 = clock.now
      val newTime2 = baseTime2.add(waitx2)
      clock.advanceTo(newTime2)

      participants.all.foreach(_.testing.fetch_synchronizer_times())

      participant1.testing.await_synchronizer_time(daId, newTime2, 5.seconds)
      participant2.testing.await_synchronizer_time(acmeId, newTime2, 5.seconds)

      participant1.health.ping(participantId = participant2, synchronizerId = Some(daId))
      participant1.health.ping(participantId = participant2, synchronizerId = Some(acmeId))

      val baseTime3 = clock.now
      val newTime3 = baseTime3.add(pruningTimeout)
      clock.advanceTo(newTime3)

      participants.all.foreach(_.testing.fetch_synchronizer_times())

      participant1.testing.await_synchronizer_time(daId, newTime3, 5.seconds)
      participant2.testing.await_synchronizer_time(acmeId, newTime3, 5.seconds)

      // Check that the reassignment offset is safe to prune
      ensureOffsetSafeToPrune(assignOffsetP1, clock, participant1)
      ensureOffsetSafeToPrune(assignOffsetP2, clock, participant2)
    }

    "fail to obtain ACS snapshot at offset between the unassign and assign, after having pruned the reassignment after the assign" in {
      implicit env =>
        import env.*

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.state.acs
            .of_party(alice, activeAtOffsetO = Some(offsetP1AfterUnassign)),
          logEntry =>
            logEntry.errorMessage should include(
              "GrpcRequestRefusedByServer: FAILED_PRECONDITION/PARTICIPANT_PRUNED_DATA_ACCESSED"
            ),
        )

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant2.ledger_api.state.acs
            .of_party(bank, activeAtOffsetO = Some(offsetP2AfterUnassign)),
          logEntry =>
            logEntry.errorMessage should include(
              "GrpcRequestRefusedByServer: FAILED_PRECONDITION/PARTICIPANT_PRUNED_DATA_ACCESSED"
            ),
        )
    }

    "ACS snapshot succeeds at ledger end after pruning reassignments" in { implicit env =>
      import env.*
      participant1.ledger_api.state.acs
        .of_party(alice, activeAtOffsetO = Some(offsetP1AfterAssign))
      participant2.ledger_api.state.acs
        .of_party(bank, activeAtOffsetO = Some(offsetP2AfterAssign))
    }
  }
}

final class ReassignmentPruningIntegrationTestPostgres extends ReassignmentPruningIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}

//class ReassignmentPruningIntegrationTestH2 extends ReassignmentPruningIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.H2](
//      loggerFactory,
//      sequencerGroups = MultiSynchronizer(
//        Seq(
//          Set(InstanceName.tryCreate("sequencer1")),
//          Set(InstanceName.tryCreate("sequencer2")),
//        )
//      ),
//    )
//  )
//}
