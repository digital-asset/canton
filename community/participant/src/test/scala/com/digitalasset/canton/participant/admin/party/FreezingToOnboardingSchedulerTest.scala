// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.participant.admin.party.PartyReplicationTopologyWorkflow.FreezingToOnboardingStatus
import com.digitalasset.canton.participant.protocol.party.{
  FreezingToOnboardingOperation,
  OnboardingClearanceOperation,
}
import com.digitalasset.canton.store.memory.InMemoryPendingOperationStore
import com.digitalasset.canton.time.{SimClock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientWithInit,
  TopologySnapshotLoader,
}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreTestData}
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtoVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.mockito.ArgumentMatchers.eq as isEq
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*

class FreezingToOnboardingSchedulerTest
    extends AsyncWordSpec
    with BaseTest
    with MockitoSugar
    with HasExecutionContext {

  /** Returns the queued statuses one per call, then `afterQueue` for every further call. */
  private class FakePartyReplicationTopologyWorkflow(loggerFactory: NamedLoggerFactory)
      extends PartyReplicationTopologyWorkflow(participantId, timeouts, loggerFactory) {
    private val statuses = new ConcurrentLinkedQueue[FreezingToOnboardingStatus]()
    @volatile var afterQueue: Option[FreezingToOnboardingStatus] = None

    /** The `proposedAt` argument of each call. */
    val calls = new ConcurrentLinkedQueue[Option[PositiveInt]]()

    def addStatus(status: FreezingToOnboardingStatus): Unit = statuses.add(status).discard

    override private[party] def moveFreezingToOnboarding(
        partyId: PartyId,
        proposedAt: Option[PositiveInt],
        topologyContext: PartyReplicationTopologyWorkflow.SynchronizerTopologyContext,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, FreezingToOnboardingStatus] = {
      calls.add(proposedAt)
      EitherT.fromEither[FutureUnlessShutdown](
        Option(statuses.poll()).orElse(afterQueue).toRight("no more statuses")
      )
    }
  }

  private lazy val testData = new TopologyStoreTestData(testedProtocolVersion, loggerFactory)
  private lazy val partyId = testData.party1
  private lazy val participantId = testData.p1Id
  private lazy val psid = testData.synchronizer1_p1p2_physicalSynchronizerId
  private val synchronizerTime = CantonTimestamp.Epoch.plusSeconds(10)
  private val serial = PositiveInt.tryCreate(5)
  private val nextSerial = PositiveInt.tryCreate(6)

  private class Fixture {
    val timeTracker = mock[SynchronizerTimeTracker]
    val topologyClient = mock[SynchronizerTopologyClientWithInit]
    val snapshot = mock[TopologySnapshotLoader]
    val workflow = new FakePartyReplicationTopologyWorkflow(loggerFactory)

    /** If set, a deletion of a move's pending operation waits for it, as if the participant crashed
      * at that point.
      */
    @volatile var moveDeletionGate: Option[Promise[Unit]] = None

    val movesStore =
      new InMemoryPendingOperationStore[FreezingToOnboardingOperation, SynchronizerId](
        FreezingToOnboardingOperation,
        loggerFactory,
      ) {
        override def delete(
            synchronizer: Synchronizer,
            operationKey: String,
            operationName: NonEmptyString,
        )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
          FutureUnlessShutdown
            .outcomeF(moveDeletionGate.fold(Future.unit)(_.future))
            .flatMap(_ => super.delete(synchronizer, operationKey, operationName))
      }
    val clearancesStore =
      new InMemoryPendingOperationStore[OnboardingClearanceOperation, SynchronizerId](
        OnboardingClearanceOperation,
        loggerFactory,
      )

    when(timeTracker.requestTick(any[CantonTimestamp], any[Boolean])(any[TraceContext]))
      .thenReturn(SynchronizerTimeTracker.DummyTickRequest)
    when(topologyClient.awaitSequencedTimestamp(any[SequencedTime])(any[TraceContext]))
      .thenReturn(None)
    when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
      .thenReturn(FutureUnlessShutdown.pure(snapshot))
    when(snapshot.announcedLsu()(any[TraceContext]))
      .thenReturn(FutureUnlessShutdown.pure(Option.empty[(SynchronizerSuccessor, EffectiveTime)]))

    private val synchronizerContext =
      new PartyReplicationTopologyWorkflow.SynchronizerTopologyContext {
        override def topologyClient = Fixture.this.topologyClient
        override def topologyManager = mock[SynchronizerTopologyManager]
        override def topologyStore = mock[TopologyStore[SynchronizerStore]]
        override def timeTracker = Fixture.this.timeTracker
      }

    /** False as while the participant disconnects from the synchronizer. */
    @volatile var synchronizerAvailable: Boolean = true
    val synchronizerContextLookups = new AtomicInteger(0)

    private val workflowContext = new PartyReplicationTopologyWorkflow.TopologyWorkflowContext {
      override def psid: PhysicalSynchronizerId = FreezingToOnboardingSchedulerTest.this.psid
      override def workflow: PartyReplicationTopologyWorkflow = Fixture.this.workflow
      override def synchronizerContext
          : Option[PartyReplicationTopologyWorkflow.SynchronizerTopologyContext] = {
        synchronizerContextLookups.incrementAndGet().discard
        Option.when(synchronizerAvailable)(Fixture.this.synchronizerContext)
      }
    }

    val scheduler = new FreezingToOnboardingScheduler(
      participantId,
      workflowContext,
      movesStore,
      clearancesStore,
      new SimClock(synchronizerTime, loggerFactory),
      loggerFactory,
      timeouts,
      retryInitialDelay = 1.millisecond,
      retryMaxDelay = 5.milliseconds,
    )

    /** Records the pending operations that the party ACS import writes. */
    def recordImport(): Future[Unit] = {
      val rpv = OnboardingClearanceOperation
        .protocolVersionRepresentativeFor(ProtoVersion(30))
        .valueOr(err => fail(err.message))
      (for {
        _ <- movesStore.insert(
          FreezingToOnboardingOperation()(
            FreezingToOnboardingOperation.protocolVersionRepresentativeFor(ProtoVersion(30)).value
          ).toPendingOperation(psid.logical, partyId)
        )
        _ <- clearancesStore.insert(
          OnboardingClearanceOperation(None)(rpv).toPendingOperation(psid.logical, partyId)
        )
      } yield ()).valueOrFail("record the import").failOnShutdown
    }

    def moveRecorded: Boolean = movesStore
      .get(
        psid.logical,
        FreezingToOnboardingOperation.operationKey(partyId),
        FreezingToOnboardingOperation.operationName,
      )
      .isDefined
      .futureValueUS

    def clearanceRecorded: Boolean = clearancesStore
      .get(
        psid.logical,
        OnboardingClearanceOperation.operationKey(partyId),
        OnboardingClearanceOperation.operationName,
      )
      .isDefined
      .futureValueUS
  }

  private def expectAbortWarning(entry: LogEntry): org.scalatest.Assertion =
    entry.warningMessage should include("while it was Freezing after the party's ACS import")

  "FreezingToOnboardingScheduler" should {
    "catch up, propose the move until it is effective, and then delete its pending operation" in {
      val fixture = new Fixture
      import fixture.*
      workflow.addStatus(FreezingToOnboardingStatus.Proposed(serial))
      // A rejected proposal: the next serial is proposed after another change of the mapping
      workflow.addStatus(FreezingToOnboardingStatus.Proposed(nextSerial))
      workflow.addStatus(FreezingToOnboardingStatus.Moved)

      for {
        _ <- recordImport()
        _ <- scheduler.requestPendingMoves().failOnShutdown
      } yield {
        eventually() {
          moveRecorded shouldBe false
          scheduler.moves shouldBe empty
        }
        workflow.calls.asScala.toSeq shouldBe Seq(None, Some(serial), Some(nextSerial))
        verify(timeTracker, times(1))
          .requestTick(isEq(synchronizerTime), isEq(true))(any[TraceContext])
        verify(topologyClient, times(1))
          .awaitSequencedTimestamp(isEq(SequencedTime(synchronizerTime)))(any[TraceContext])
        // The onboarding clearance, started by the move's Onboarding event, deletes its own record
        clearanceRecorded shouldBe true
      }
    }

    "retry silently while a synchronizer upgrade is announced, and propose again afterwards" in {
      val fixture = new Fixture
      import fixture.*
      val successor = SynchronizerSuccessor(psid.opaque, CantonTimestamp.Epoch.plusSeconds(60))
      when(snapshot.announcedLsu()(any[TraceContext])).thenReturn(
        FutureUnlessShutdown.pure(Option.empty[(SynchronizerSuccessor, EffectiveTime)]),
        FutureUnlessShutdown.pure(Some(successor -> EffectiveTime(CantonTimestamp.Epoch))),
        FutureUnlessShutdown.pure(Option.empty[(SynchronizerSuccessor, EffectiveTime)]),
      )
      workflow.addStatus(FreezingToOnboardingStatus.Proposed(serial))
      workflow.addStatus(FreezingToOnboardingStatus.Moved)

      for {
        _ <- recordImport()
      } yield {
        scheduler.requestMoveInBackground(partyId)
        eventually() {
          moveRecorded shouldBe false
        }
        // The proposal before the announcement is rejected when sequenced after it
        workflow.calls.asScala.toSeq shouldBe Seq(None, None)
      }
    }

    "delete its pending operation and the onboarding clearance record if the entry is removed" in {
      val fixture = new Fixture
      import fixture.*
      workflow.addStatus(FreezingToOnboardingStatus.Aborted(None))

      for {
        _ <- recordImport()
      } yield {
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
          scheduler.requestMoveInBackground(partyId),
          entries => forExactly(1, entries)(expectAbortWarning),
        )
        eventually() {
          moveRecorded shouldBe false
          clearanceRecorded shouldBe false
        }
      }
    }

    "delete the onboarding clearance record before its pending operation when aborting" in {
      // A crash in between leaves the pending operation, so the abort runs again on the next connect
      val fixture = new Fixture
      import fixture.*
      val gate = Promise[Unit]()
      moveDeletionGate = Some(gate)
      workflow.addStatus(FreezingToOnboardingStatus.Aborted(None))

      for {
        _ <- recordImport()
      } yield {
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
          {
            scheduler.requestMoveInBackground(partyId)
            eventually() {
              clearanceRecorded shouldBe false
            }
            moveRecorded shouldBe true
            gate.success(())
          },
          entries => forExactly(1, entries)(expectAbortWarning),
        )
        eventually() {
          moveRecorded shouldBe false
        }
      }
    }

    "stop once an observed transaction removes the entry, even if it is Freezing again" in {
      val fixture = new Fixture
      import fixture.*
      // The workflow would keep reporting the entry as Freezing
      workflow.afterQueue = Some(FreezingToOnboardingStatus.Proposed(serial))
      val withoutParticipant = testData.makeSignedTx(
        PartyToParticipant.tryCreate(
          partyId,
          PositiveInt.one,
          Seq(HostingParticipant(testData.p2Id, ParticipantPermission.Submission)),
        ),
        serial = serial,
      )(testData.p1Key)

      for {
        _ <- recordImport()
      } yield {
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            scheduler.requestMoveInBackground(partyId)
            // More attempts than the backoff makes before it complains at WARN about a retry
            eventually() {
              workflow.calls.size should be > 10
            }
            scheduler
              .observed(
                SequencedTime(synchronizerTime),
                EffectiveTime(synchronizerTime),
                SequencerCounter.Genesis,
                Seq(withoutParticipant),
              )
              .futureValueUS
          },
          entries => expectAbortWarning(entries.loneElement),
        )
        eventually() {
          moveRecorded shouldBe false
          clearanceRecorded shouldBe false
          scheduler.moves shouldBe empty
        }
      }
    }

    "retry silently while the synchronizer is not available, also after many retries" in {
      val fixture = new Fixture
      import fixture.*
      workflow.afterQueue = Some(FreezingToOnboardingStatus.Proposed(serial))

      for {
        _ <- recordImport()
      } yield {
        loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            scheduler.requestMoveInBackground(partyId)
            // More attempts than the backoff makes before it complains at WARN about a retry
            eventually() {
              workflow.calls.size should be > 10
            }
            // The connected synchronizer is removed before its scheduler is closed
            synchronizerAvailable = false
            val lookups = synchronizerContextLookups.get()
            eventually() {
              synchronizerContextLookups.get() should be > lookups + 2
            }
            scheduler.close()
          },
          entries => entries shouldBe empty,
        )
        succeed
      }
    }

    "stop without proposing if its pending operation no longer exists" in {
      val fixture = new Fixture
      import fixture.*
      workflow.afterQueue = Some(FreezingToOnboardingStatus.Proposed(serial))

      scheduler.requestMoveInBackground(partyId)
      eventually() {
        scheduler.moves shouldBe empty
      }
      workflow.calls shouldBe empty
    }
  }
}
