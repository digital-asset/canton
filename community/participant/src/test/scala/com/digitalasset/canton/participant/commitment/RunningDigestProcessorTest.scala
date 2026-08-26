// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.InternalIndexService.AcsUpdate.EffectiveTopologyUpdate
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.Submission
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationLevel,
  GenericTopologyEvent,
}
import com.digitalasset.canton.ledger.participant.state.{AcsChange, InternalIndexService}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  CheckpointFence,
  CheckpointWritten,
  ContractChange,
  ContractChangeBatch,
  NotCheckpointFence,
  PartyAddedToParticipant,
  PartyOnboardingToParticipant,
  PartyRemovedFromParticipant,
  ProcessingContext,
}
import com.digitalasset.canton.participant.commitment.SynchronizerCommitmentState.{
  TickListener,
  TickSignaller,
}
import com.digitalasset.canton.participant.config.{AcsCommitmentConfig, AcsDigestTracingMode}
import com.digitalasset.canton.participant.metrics.{CommitmentMetrics, TestCommitmentMetrics}
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.{
  MaxEventsWithoutCheckpoint,
  PartyHostingChange,
  ReceivedCommitmentCheckpoint,
  ReconciliationIntervalBoundary,
}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryAcsCommitmentPeriodStore,
  InMemoryAcsDigestStore,
}
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.protocol.SynchronizerParameters.WithValidity
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.processing.TopologyTransactionTestFactory
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{
  SynchronizerParametersState,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ParticipantId,
  SynchronizerId,
  TestingTopology,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.signalling.{LocalEventSignaller, NotificationSignal}
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext, LfPartyId}
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.testkit.scaladsl.TestSink
import org.scalatest.Assertion

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*

@AcsCommitmentTest
class RunningDigestProcessorTest
    extends DigestProcessorTestBase
    with HasExecutionContext
    with HasActorSystem {

  import DigestProcessorTestBase.*

  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

  def mkRunningDigestProcessor(
      participant: ParticipantId = thisParticipant,
      indexService: InternalIndexService = mkIndexService(),
      tickSignaller: TickSignaller = mkTickSignaller(),
      counterpartyBatchSize: Int = 10,
      contractChangeClassificationBatchSize: Int = 2,
      partyTopology: Map[LfPartyId, PartyInfo] = Map.empty,
      maxNumUpdatesBetweenCheckpoints: PositiveInt = PositiveInt.tryCreate(5),
      reconciliationInterval: FiniteDuration = DynamicSynchronizerParameters
        .defaultValues(testedProtocolVersion)
        .reconciliationInterval
        .toFiniteDuration,
      metrics: CommitmentMetrics = TestCommitmentMetrics(),
  ): RunningDigestProcessorImpl = {
    val testingTopology = TestingTopology(
      topology = partyTopology,
      synchronizerParameters = List(
        WithValidity(
          CantonTimestamp.MinValue,
          None,
          DynamicSynchronizerParameters
            .defaultValues(testedProtocolVersion)
            .update(reconciliationInterval =
              PositiveSeconds.tryOfMicros(reconciliationInterval.toMicros)
            ),
        )
      ),
    ).build()

    val mockStringInterning = new MockStringInterning
    val acsDigestStore =
      InMemoryAcsDigestStore.create(Eval.now(mockStringInterning), loggerFactory)
    val acsPeriodStore =
      new InMemoryAcsCommitmentPeriodStore(
        Eval.now(mockStringInterning),
        loggerFactory,
        enableConsistencyChecks = true,
      )
    val digestAccumulator = new SequentialDigestAccumulator(
      acsDigestStore,
      mockStringInterning,
      AcsDigestTracingMode.Disabled,
      TestCommitmentMetrics(),
      loggerFactory,
    )
    val mockTopologyClient = mock[SynchronizerTopologyClient]
    when(mockTopologyClient.awaitSnapshot(any[CantonTimestamp])(anyTraceContext)).thenAnswer(
      (timestamp: CantonTimestamp, _: TraceContext) =>
        FutureUnlessShutdown.pure(
          testingTopology.topologySnapshot(timestampOfSnapshot = timestamp)
        )
    )

    new RunningDigestProcessorImpl(
      participant,
      synchronizerId = DefaultTestIdentities.synchronizerId,
      AcsCommitmentConfig(
        enableRunningDigestProcessor = true,
        maxNumUpdatesBetweenCheckpoints = maxNumUpdatesBetweenCheckpoints,
        counterpartyBatchSize = PositiveInt.tryCreate(counterpartyBatchSize),
        tracing = AcsDigestTracingMode.Disabled,
        contractChangeClassificationBatchSize =
          PositiveInt.tryCreate(contractChangeClassificationBatchSize),
      ),
      digestAccumulator,
      acsDigestStore,
      tickSignaller,
      indexService,
      new DigestProcessorTopologyLookup {
        override def topologyClientForRunningDigestProcessor(
            synchronizerId: SynchronizerId,
            timestamp: CantonTimestamp,
            previousTopologyClientO: Option[SynchronizerTopologyClient],
        )(implicit traceContext: TraceContext): FutureUnlessShutdown[SynchronizerTopologyClient] =
          FutureUnlessShutdown.pure(mockTopologyClient)

        override def topologySnapshotForReinitialization(
            synchronizerId: SynchronizerId,
            timestamp: CantonTimestamp,
        )(implicit traceContext: TraceContext): Option[TopologySnapshot] = ???
      },
      enableAdditionalConsistencyChecks = true,
      new AcsCommitmentPeriodWriter(acsDigestStore, acsPeriodStore, loggerFactory),
      metrics,
      timeouts,
      loggerFactory,
    )
  }

  def mkTickSignaller(): TickSignaller =
    new LocalEventSignaller[TickListener, Offset]("subscriber", timeouts, loggerFactory)

  "RunningDigestProcessor" when {

    "checkpointing" should {
      "emit a checkpoint fence for AcsChanges that cross a reconciliation interval boundary" in {
        val rdp = mkRunningDigestProcessor(
          reconciliationInterval = 5.seconds
        )

        val dummyAcsChange =
          InternalIndexService.AcsUpdate.AcsChangeUpdate(AcsChange(Map.empty, Map.empty))
        val result = Source(
          Seq(
            ProcessingContext(tp(2), dummyAcsChange),
            ProcessingContext(tp(3), dummyAcsChange),
            ProcessingContext(tp(5), dummyAcsChange),
            ProcessingContext(tp(6), dummyAcsChange),
            ProcessingContext(tp(7), dummyAcsChange),
          )
        ).via(rdp.checkpointing(None, TraceContext.empty)).runWith(Sink.seq).futureValue

        rdp.metrics.runningDigestProcessor.latestCheckpointedRecordTime.getValue shouldBe ts(
          7
        ).toMicros

        result.map(_.map(_.toEither)) should contain theSameElementsInOrderAs Seq(
          ProcessingContext(tp1_0, Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(2), Right(dummyAcsChange)),
          ProcessingContext(tp(3), Right(dummyAcsChange)),
          ProcessingContext(tp(5), Right(dummyAcsChange)),
          ProcessingContext(tp(5), Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(6), Right(dummyAcsChange)),
          ProcessingContext(tp(7), Right(dummyAcsChange)),
        )
      }

      "emit a checkpoint fence every n events without checkpoints" in {
        val rdp = mkRunningDigestProcessor(
          maxNumUpdatesBetweenCheckpoints = PositiveInt.two,
          // make sure that the checkpoint intervals do not come into play
          reconciliationInterval = 1.hour,
        )

        val dummyAcsChange =
          InternalIndexService.AcsUpdate.AcsChangeUpdate(AcsChange(Map.empty, Map.empty))
        val dummyTopologyUpdate =
          InternalIndexService.AcsUpdate.EffectiveTopologyUpdate(Set.empty, None)
        val dummyAcsCommitment =
          InternalIndexService.AcsUpdate.AcsCommitment(ByteString.empty)
        val dummyCheckpoint =
          InternalIndexService.AcsUpdate.OffsetCheckpoint
        val result = Source(
          Seq(
            ProcessingContext(Timepoint(off(2))(ts(2)), dummyAcsChange),
            ProcessingContext(Timepoint(off(3))(ts(2)), dummyAcsChange),
            // a checkpoint should be injected here
            ProcessingContext(Timepoint(off(4))(ts(2)), dummyAcsChange),
            ProcessingContext(Timepoint(off(5))(ts(3)), dummyAcsChange),
            // a checkpoint should be injected here
            ProcessingContext(Timepoint(off(8))(ts(5)), dummyAcsChange),
            ProcessingContext(Timepoint(off(9))(ts(6)), dummyAcsChange),
            // a checkpoint should be injected here
            // empty topology events don't trigger topology related checkpoints, but should count towards processed events
            ProcessingContext(tp(11), dummyTopologyUpdate),
            ProcessingContext(tp(12), dummyTopologyUpdate),
            // a checkpoint should be injected here
            ProcessingContext(tp(15), dummyTopologyUpdate),
            // received acs commitments don't trigger by themselves, but should count towards processed events
            ProcessingContext(tp(16), dummyAcsCommitment),
            // a checkpoint should be injected here
            ProcessingContext(tp(18), dummyCheckpoint),
            ProcessingContext(tp(20), dummyCheckpoint),
            // a checkpoint should be injected here
            ProcessingContext(tp(21), dummyCheckpoint),
          )
        ).via(rdp.checkpointing(None, TraceContext.empty)).runWith(Sink.seq).futureValue

        rdp.metrics.runningDigestProcessor.latestCheckpointedRecordTime.getValue shouldBe ts(
          20
        ).toMicros

        result.map(_.map(_.toEither)) shouldBe Seq(
          ProcessingContext(tp1_0, Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(2), Right(dummyAcsChange)),
          ProcessingContext(Timepoint(off(3))(ts(2)), Right(dummyAcsChange)),
          ProcessingContext(
            Timepoint(off(3))(ts(2)),
            Left(CheckpointType.MaxEventsWithoutCheckpoint),
          ),
          ProcessingContext(Timepoint(off(4))(ts(2)), Right(dummyAcsChange)),
          ProcessingContext(Timepoint(off(5))(ts(3)), Right(dummyAcsChange)),
          ProcessingContext(
            Timepoint(off(5))(ts(3)),
            Left(CheckpointType.MaxEventsWithoutCheckpoint),
          ),
          ProcessingContext(Timepoint(off(8))(ts(5)), Right(dummyAcsChange)),
          ProcessingContext(Timepoint(off(9))(ts(6)), Right(dummyAcsChange)),
          ProcessingContext(
            Timepoint(off(9))(ts(6)),
            Left(CheckpointType.MaxEventsWithoutCheckpoint),
          ),
          ProcessingContext(
            tp(12),
            Left(CheckpointType.MaxEventsWithoutCheckpoint),
          ),
          ProcessingContext(
            tp(16),
            Left(CheckpointType.ReceivedCommitmentCheckpoint),
          ),
          ProcessingContext(
            tp(20),
            Left(CheckpointType.MaxEventsWithoutCheckpoint),
          ),
        )
      }

      "emit the right checkpoints around reconciliation interval ticks" in {
        val rdp = mkRunningDigestProcessor(
          maxNumUpdatesBetweenCheckpoints = PositiveInt.two,
          reconciliationInterval = 5.seconds,
        )

        val dummyAcsChange =
          InternalIndexService.AcsUpdate.AcsChangeUpdate(AcsChange(Map.empty, Map.empty))
        val partyHostingChange =
          InternalIndexService.AcsUpdate.EffectiveTopologyUpdate(
            Set(PartyToParticipantAuthorization(alice, p2.toLf, Added(Submission))),
            None,
          )
        val result = Source(
          Seq(
            ProcessingContext(tp(2), dummyAcsChange),
            ProcessingContext(tp(5), dummyAcsChange), // falls on a recon tick
            // checkpoints MaxNumEventsWithoutCheckpoint and ReconciliationIntervalBoundary could be injected here,
            // but ReconciliationIntervalBoundary takes precedence
            ProcessingContext(tp(6), dummyAcsChange),
            ProcessingContext(tp(10), partyHostingChange),
            // checkpoints PartyHostingChange and ReconciliationIntervalBoundary could be injected here,
            // but ReconciliationIntervalBoundary takes precedence
            ProcessingContext(tp(11), dummyAcsChange),
          )
        ).via(rdp.checkpointing(None, TraceContext.empty)).runWith(Sink.seq).futureValue

        rdp.metrics.runningDigestProcessor.latestCheckpointedRecordTime.getValue shouldBe ts(
          11
        ).toMicros

        result.map(_.map(_.toEither)) shouldBe Seq(
          ProcessingContext(tp1_0, Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(2), Right(dummyAcsChange)),
          ProcessingContext(tp(5), Right(dummyAcsChange)),
          ProcessingContext(tp(5), Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(6), Right(dummyAcsChange)),
          ProcessingContext(tp(10), Right(partyHostingChange)),
          ProcessingContext(tp(10), Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(11), Right(dummyAcsChange)),
        )
      }

      "emit a checkpoint fence for topology changes" in {
        val topologyEvents = for {
          participant <- Seq(thisParticipant.toLf /* local change */, p2.toLf /* remote change*/ )
          authChange <- Seq(
            Onboarding(AuthorizationLevel.Submission),
            Added(AuthorizationLevel.Submission),
            Revoked,
          )
        } yield PartyToParticipantAuthorization(alice, participant, authChange)

        val inputEvents = topologyEvents.zip(Iterator.from(1)).map { case (event, timeOffset) =>
          ProcessingContext(tp(timeOffset), tte(event))
        }

        val rdp = mkRunningDigestProcessor(
          // make sure that the checkpoint intervals do not come into play
          maxNumUpdatesBetweenCheckpoints = PositiveInt.tryCreate(100),
          reconciliationInterval = 1.hour,
        )

        val result = Source(inputEvents)
          .via(rdp.checkpointing(None, TraceContext.empty))
          .runWith(Sink.seq)
          .futureValue

        rdp.metrics.runningDigestProcessor.latestCheckpointedRecordTime.getValue shouldBe inputEvents
          .map(_.recordTime.toMicros)
          .max

        // match on CheckpointFenceOr[InputEvent].toEither, so we don't have to match on the topology snapshot
        val expectedResult =
          // add a fence AFTER each input event with the same timestamp as the input event
          inputEvents
            .flatMap { input =>
              Seq(
                ProcessingContext(input.timepoint, Right(input.value)),
                ProcessingContext(input.timepoint, Left(CheckpointType.PartyHostingChange)),
              )
            }
            // The last checkpoint will only be emitted upon the next event, so we don't see it in this test.
            .dropRight(1)

        result.map(_.map(_.toEither)) shouldBe expectedResult
      }

      "emit a checkpoint fence for synchronizer parameter changes" in {
        val dummyAcsChange =
          InternalIndexService.AcsUpdate.AcsChangeUpdate(AcsChange(Map.empty, Map.empty))
        val inputEvents = Seq(
          ProcessingContext(tp(1), dummyAcsChange),
          ProcessingContext(
            tp(2),
            InternalIndexService.AcsUpdate.EffectiveTopologyUpdate(
              Set.empty,
              Some(
                GenericTopologyEvent.SynchronizerParametersState(
                  TopologyTransaction
                    .tryCreate(
                      Replace,
                      PositiveInt.one,
                      SynchronizerParametersState(
                        DefaultTestIdentities.synchronizerId,
                        DynamicSynchronizerParameters
                          .defaultValues(testedProtocolVersion)
                          // different value that what the test is set up with
                          .update(reconciliationInterval = PositiveSeconds.tryOfHours(2)),
                      ),
                      testedProtocolVersion,
                    )
                    .toByteStringChecked
                )
              ),
            ),
          ),
          ProcessingContext(tp(3), dummyAcsChange),
        )

        val rdp = mkRunningDigestProcessor(
          // make sure that the checkpoint intervals do not come into play
          maxNumUpdatesBetweenCheckpoints = PositiveInt.tryCreate(100),
          reconciliationInterval = 1.hour,
        )

        val result = Source(inputEvents)
          .via(rdp.checkpointing(None, TraceContext.empty))
          .runWith(Sink.seq)
          .futureValue

        rdp.metrics.runningDigestProcessor.latestCheckpointedRecordTime.getValue shouldBe inputEvents
          .map(_.recordTime)
          .max
          .toMicros

        // match on CheckpointFenceOr[InputEvent].toEither, so we don't have to match on the topology snapshot
        val expectedResult =
          Seq(
            ProcessingContext(tp(1), Right(dummyAcsChange)),
            ProcessingContext(tp(2), Left(CheckpointType.ReconciliationIntervalBoundary)),
            ProcessingContext(tp(3), Right(dummyAcsChange)),
          )

        result.map(_.map(_.toEither)) should contain theSameElementsInOrderAs (expectedResult)
      }

      "emit a checkpoint for received ACS commitments" in {
        val dummyAcsChange =
          InternalIndexService.AcsUpdate.AcsChangeUpdate(AcsChange(Map.empty, Map.empty))
        val dummyAcsCommitment =
          InternalIndexService.AcsUpdate.AcsCommitment(ByteString.empty)
        val inputEvents = Seq(
          ProcessingContext(tp(1), dummyAcsChange),
          ProcessingContext(tp(2), dummyAcsCommitment),
          ProcessingContext(tp(3), dummyAcsChange),
        )

        val rdp = mkRunningDigestProcessor(
          // make sure that the checkpoint intervals do not come into play
          maxNumUpdatesBetweenCheckpoints = PositiveInt.tryCreate(100),
          reconciliationInterval = 1.hour,
        )

        val result = Source(inputEvents)
          .via(rdp.checkpointing(None, TraceContext.empty))
          .runWith(Sink.seq)
          .futureValue

        rdp.metrics.runningDigestProcessor.latestCheckpointedRecordTime.getValue shouldBe
          inputEvents.map(_.recordTime).max.toMicros

        // match on CheckpointFenceOr[InputEvent].toEither, so we don't have to match on the topology snapshot
        val expectedResult =
          Seq(
            ProcessingContext(tp(1), Right(dummyAcsChange)),
            ProcessingContext(tp(2), Left(CheckpointType.ReceivedCommitmentCheckpoint)),
            ProcessingContext(tp(3), Right(dummyAcsChange)),
          )

        result.map(_.map(_.toEither)) shouldBe expectedResult
      }

      "respect the initial checkpoint timestamp from crash recovery when the checkpoint falls on the reconciliation boundary" in {
        val rdp = mkRunningDigestProcessor(
          reconciliationInterval = 5.seconds
        )

        val dummyAcsChange =
          InternalIndexService.AcsUpdate.AcsChangeUpdate(AcsChange(Map.empty, Map.empty))
        val result = Source(
          Seq(
            ProcessingContext(tp(6), dummyAcsChange),
            ProcessingContext(tp(7), dummyAcsChange),
            ProcessingContext(tp(11), dummyAcsChange),
          )
        ).via(rdp.checkpointing(Some(ts(5)), TraceContext.empty)).runWith(Sink.seq).futureValue

        rdp.metrics.runningDigestProcessor.latestCheckpointedRecordTime.getValue shouldBe ts(
          11
        ).toMicros

        result.map(_.map(_.toEither)) should contain theSameElementsInOrderAs Seq(
          // when the checkpoint falls exactly on a reconciliation boundary, the same checkpoint is emitted
          ProcessingContext(tp(5), Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(6), Right(dummyAcsChange)),
          ProcessingContext(tp(7), Right(dummyAcsChange)),
          ProcessingContext(tp(10), Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(11), Right(dummyAcsChange)),
        )
      }

      "respect the initial checkpoint timestamp from crash recovery for checkpoints not on reconciliation boundaries" in {
        val rdp = mkRunningDigestProcessor(
          reconciliationInterval = 5.seconds
        )

        val dummyAcsChange =
          InternalIndexService.AcsUpdate.AcsChangeUpdate(AcsChange(Map.empty, Map.empty))
        val result = Source(
          Seq(
            ProcessingContext(tp(7), dummyAcsChange),
            ProcessingContext(tp(11), dummyAcsChange),
          )
        ).via(rdp.checkpointing(Some(ts(6)), TraceContext.empty)).runWith(Sink.seq).futureValue

        rdp.metrics.runningDigestProcessor.latestCheckpointedRecordTime.getValue shouldBe ts(
          11
        ).toMicros

        result.map(_.map(_.toEither)) should contain theSameElementsInOrderAs Seq(
          // when the checkpoint falls exactly on a reconciliation boundary, the same checkpoint is emitted
          ProcessingContext(tp(7), Right(dummyAcsChange)),
          ProcessingContext(tp(10), Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(11), Right(dummyAcsChange)),
        )
      }
    }

    // test cases for the classification stage
    "classifying" should {
      "pass checkpoint fences through" in {
        val rdp = mkRunningDigestProcessor()

        val fence = ProcessingContext(tp1_0, CheckpointFence(ReconciliationIntervalBoundary))

        val result = Source
          .single(fence)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue
          .loneElement

        rdp.metrics.runningDigestProcessor.latestClassifiedRecordTime.getValue shouldBe tp1_0.recordTime.toMicros

        result shouldBe ProcessingContext(tp1_0, CheckpointFence(ReconciliationIntervalBoundary))
      }

      "handle ACS changes" in {
        val topologySnapshot = TestingTopology(topology =
          Map(
            partyHosting(alice)(p1, p2),
            partyHosting(bob)(p2, p3),
            partyHosting(charlie)(p1, p3, p4),
          )
        ).build().topologySnapshot()

        val event = AcsChange(
          activations = Map(
            // one of the stakeholders is hosted by thisParticipant
            cid(0) -> Set(alice, bob),
            // NONE of the stakeholders is hosted by thisParticipant. the change will be ignored
            cid(4) -> Set(bob),
          ),
          deactivations = Map(
            // both stakeholders of the contract are hosted by thisParticipant
            cid(1) -> Set(alice, charlie),
            cid(3) -> Set(bob, charlie),
            // NONE of the stakeholders is hosted by thisParticipant. the change will be ignored
            cid(5) -> Set(bob),
          ),
        )

        val toProcess =
          ProcessingContext(
            tp1_0,
            NotCheckpointFence(
              topologySnapshot,
              InternalIndexService.AcsUpdate.AcsChangeUpdate(event),
            ),
          )

        val rdp = mkRunningDigestProcessor()

        val result = Source
          .single(toProcess)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue

        rdp.metrics.runningDigestProcessor.latestClassifiedRecordTime.getValue shouldBe tp1_0.recordTime.toMicros

        result.map(_.value.tryValue) should contain theSameElementsAs Seq(
          ContractChangeBatch.tryCreate(
            Map(
              alice -> Set(p1.toLf, p2.toLf),
              bob -> Set(p2.toLf, p3.toLf),
              charlie -> Set(p1.toLf, p3.toLf, p4.toLf),
            ),
            ContractChange(
              stakeholders = Set(alice, bob),
              locallyHostedStakeholders = Seq(alice),
              cid(0),
              rc,
              isActivation = true,
            ),
            ContractChange(
              stakeholders = Set(alice, charlie),
              locallyHostedStakeholders = Seq(alice, charlie),
              cid(1),
              rc,
              isActivation = false,
            ),
          ),
          // the default contract change batch size for this test is 2, therefore the contract change should emit batches of size 2
          ContractChangeBatch.tryCreate(
            Map(bob -> Set(p2.toLf, p3.toLf), charlie -> Set(p1.toLf, p3.toLf, p4.toLf)),
            ContractChange(
              stakeholders = Set(bob, charlie),
              locallyHostedStakeholders = Seq(charlie),
              cid(3),
              rc,
              isActivation = false,
            ),
          ),
        )
      }

      "handle a party being onboarded on a remote participant" in {
        val event = PartyToParticipantAuthorization(
          bob,
          p3.toLf,
          Onboarding(AuthorizationLevel.Submission),
        )

        // mocked topology snapshot to verify that it is not being used.
        val topologySnapshot = mock[TopologySnapshot]
        val toProcess =
          ProcessingContext(tp1_0, NotCheckpointFence(topologySnapshot, tte(event)))
        val rdp = mkRunningDigestProcessor()

        val result = Source
          .single(toProcess)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue
          .loneElement

        rdp.metrics.runningDigestProcessor.latestClassifiedRecordTime.getValue shouldBe tp1_0.recordTime.toMicros

        result.value.tryValue shouldBe PartyOnboardingToParticipant(bob, p3.toLf)
        verifyZeroInteractions(topologySnapshot)
      }

      "handle the completion of a party onboarding on a remote participant" in {
        val event = PartyToParticipantAuthorization(
          bob,
          p3.toLf,
          Added(AuthorizationLevel.Submission),
        )

        val topologySnapshot = mock[TopologySnapshot]
        val toProcess =
          ProcessingContext(tp1_0, NotCheckpointFence(topologySnapshot, tte(event)))

        val rdp = mkRunningDigestProcessor()

        val result = Source
          .single(toProcess)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue
          .loneElement

        rdp.metrics.runningDigestProcessor.latestClassifiedRecordTime.getValue shouldBe tp1_0.recordTime.toMicros

        result.value.tryValue shouldBe PartyAddedToParticipant(bob, p3.toLf)
        verifyZeroInteractions(topologySnapshot)
      }

      "handle the removal of a party from a remote participant" in {
        val event = PartyToParticipantAuthorization(
          bob,
          p3.toLf,
          Revoked,
        )

        val topologySnapshot = mock[TopologySnapshot]
        val toProcess =
          ProcessingContext(tp1_0, NotCheckpointFence(topologySnapshot, tte(event)))

        val rdp = mkRunningDigestProcessor()

        val result = Source
          .single(toProcess)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue
          .loneElement

        rdp.metrics.runningDigestProcessor.latestClassifiedRecordTime.getValue shouldBe tp1_0.recordTime.toMicros

        result.value.tryValue shouldBe PartyRemovedFromParticipant(bob, p3.toLf)
        verifyZeroInteractions(topologySnapshot)
      }

      "handle multiple topology changes at the same effective time" in {
        /* the scenario:
          Two participant nodes P1 and P2.
          P1 hosts ALICE and CHARLIE.
          P2 hosts BOB, CHARLIE.
          Contract CID1 with stakeholders ALICE, BOB, and CHARLIE.

          A topology input event contains 3 topology changes at the same record time:
          1. P1 unhosts ALICE
          2. P2 hosts ALICE
          3. P1 hosts BOB
         */

        val testingTopology = TestingTopology(topology =
          Map(
            partyHosting(alice)(p1),
            partyHosting(bob)(p2),
            partyHosting(charlie)(p1, p2),
          )
        ).build()

        val p1_unhosts_alice = PartyToParticipantAuthorization(alice, p1.toLf, Revoked)
        val p2_hosts_alice =
          PartyToParticipantAuthorization(alice, p2.toLf, Added(AuthorizationLevel.Submission))
        val p1_hosts_bob =
          PartyToParticipantAuthorization(bob, p1.toLf, Added(AuthorizationLevel.Submission))

        val toProcess =
          ProcessingContext(
            tp(2),
            NotCheckpointFence(
              testingTopology.topologySnapshot(),
              tte(p1_unhosts_alice, p2_hosts_alice, p1_hosts_bob),
            ),
          )

        def processTopologyEventsWithParticipant(
            participant: ParticipantId
        ): Seq[BaseDigestProcessor.Classification] = {
          val rdp = mkRunningDigestProcessor(
            participant = participant,
            indexService = mkIndexService(
              (off(1), cid(1), Seq(alice, bob, charlie))
            ),
            partyTopology = testingTopology.getTopology().topology,
            counterpartyBatchSize = 10,
          )

          val result = Source
            .single(toProcess)
            .via(rdp.classification)
            .runWith(Sink.seq)
            .futureValue

          rdp.metrics.runningDigestProcessor.latestClassifiedRecordTime.getValue shouldBe result
            .map(_.recordTime.toMicros)
            .max

          result.map(_.value.tryValue)
        }
        val resultP1 = processTopologyEventsWithParticipant(p1)

        resultP1 should contain theSameElementsInOrderAs Seq(
          // p1_unhosts_alice
          PartyRemovedFromParticipant(alice, p1.toLf),
          ContractChangeBatch.tryCreate(
            Map(
              alice -> Set(),
              bob -> Set(p2.toLf),
              charlie -> Set(p1.toLf, p2.toLf),
            ),
            ContractChange(
              Set(alice, bob, charlie),
              Seq(alice),
              cid(1),
              rc,
              isActivation = false,
            ),
          ),

          // p2_hosts_alice
          PartyAddedToParticipant(alice, p2.toLf),

          // p1_hosts_bob
          ContractChangeBatch.tryCreate(
            Map(
              alice -> Set(p2.toLf),
              bob -> Set(p2.toLf),
              charlie -> Set(p1.toLf, p2.toLf),
            ),
            ContractChange(
              Set(alice, bob, charlie),
              Seq(bob),
              cid(1),
              rc,
              isActivation = true,
            ),
          ),
          PartyAddedToParticipant(bob, p1.toLf),
        )

        val resultP2 = processTopologyEventsWithParticipant(p2)

        resultP2 should contain theSameElementsInOrderAs Seq(
          // p1_unhosts_alice
          PartyRemovedFromParticipant(alice, p1.toLf),

          // p2_hosts_alice
          ContractChangeBatch.tryCreate(
            Map(
              alice -> Set(),
              bob -> Set(p2.toLf),
              charlie -> Set(p1.toLf, p2.toLf),
            ),
            ContractChange(
              Set(alice, bob, charlie),
              Seq(alice),
              cid(1),
              rc,
              isActivation = true,
            ),
          ),
          PartyAddedToParticipant(alice, p2.toLf),

          // p1_hosts_bob
          PartyAddedToParticipant(bob, p1.toLf),
        )

      }

      "handle adding a party to the local participant" in {
        // simulates that completion of onboarding alice to p1
        val testingTopology = TestingTopology(topology =
          Map(
            partyHosting(alice)(p2),
            partyHosting(bob)(p2, p3),
            partyHosting(charlie)(p1, p3, p4),
          )
        ).build()

        def classifyWithBatchSize(batchSize: Int) = {
          val rdp = mkRunningDigestProcessor(
            indexService = mkIndexService(
              (off(1), cid(0), Seq(alice, bob, charlie)),
              (off(2), cid(1), Seq(alice, charlie)),
              (off(2), cid(2), Seq(alice, bob)),
              (off(2), cid(3), Seq(alice)),
            ),
            partyTopology = testingTopology.getTopology().topology,
            counterpartyBatchSize = batchSize,
            contractChangeClassificationBatchSize = 1,
          )
          val event = PartyToParticipantAuthorization(
            alice,
            p1.toLf,
            Added(AuthorizationLevel.Submission),
          )

          val toProcess =
            ProcessingContext(
              tp(3),
              NotCheckpointFence(testingTopology.topologySnapshot(), tte(event)),
            )

          val result = Source
            .single(toProcess)
            .via(rdp.classification)
            .runWith(Sink.seq)
            .futureValue

          rdp.metrics.runningDigestProcessor.latestClassifiedRecordTime.getValue shouldBe result
            .map(_.recordTime.toMicros)
            .max

          val expectedContractsProcessed = batchSize match {
            case 1 =>
              // Every party's active contracts are processed individually.
              // So the number of processed contract changes is the sum of the stakeholder count over all contracts
              3 + 2 + 2 + 1
            case 2 =>
              // With a batch size of 2, it is non-deterministic how often we see each contract because
              // this depends on how the parties are grouped. So this case is not supported at the moment.
              fail("Not implemented yet")
            case other =>
              // As there are only three parties involved, the all fit into one batch of this size
              // and so every contract will be looked at only once. So this is the number of contracts.
              4
          }
          rdp.metrics.runningDigestProcessor.localPartyChangeContractChanges.getValue shouldBe expectedContractsProcessed
          rdp.metrics.runningDigestProcessor.localPartyChangeCounterparties.getValue shouldBe 3

          result.map(_.value.tryValue)
        }

        withClue("counterparty batch size 1") {
          val classifications = classifyWithBatchSize(1)
          classifications should contain theSameElementsAs Seq(
            // cid0
            ContractChangeBatch.tryCreate(
              Map(alice -> Set(p2.toLf)),
              ContractChange(
                stakeholders = Set(alice),
                locallyHostedStakeholders = Seq(alice),
                cid(0),
                rc,
                isActivation = true,
              ),
            ),
            ContractChangeBatch.tryCreate(
              Map(bob -> Set(p2.toLf, p3.toLf)),
              ContractChange(
                stakeholders = Set(bob),
                locallyHostedStakeholders = Seq(alice),
                cid(0),
                rc,
                isActivation = true,
              ),
            ),
            ContractChangeBatch.tryCreate(
              Map(charlie -> Set(p1.toLf, p3.toLf, p4.toLf)),
              ContractChange(
                stakeholders = Set(charlie),
                locallyHostedStakeholders = Seq(alice),
                cid(0),
                rc,
                isActivation = true,
              ),
            ),

            // cid1
            ContractChangeBatch.tryCreate(
              Map(alice -> Set(p2.toLf)),
              ContractChange(
                stakeholders = Set(alice),
                locallyHostedStakeholders = Seq(alice),
                cid(1),
                rc,
                isActivation = true,
              ),
            ),
            ContractChangeBatch.tryCreate(
              Map(charlie -> Set(p1.toLf, p3.toLf, p4.toLf)),
              ContractChange(
                stakeholders = Set(charlie),
                locallyHostedStakeholders = Seq(alice),
                cid(1),
                rc,
                isActivation = true,
              ),
            ),

            // cid2
            ContractChangeBatch.tryCreate(
              Map(alice -> Set(p2.toLf)),
              ContractChange(
                stakeholders = Set(alice),
                locallyHostedStakeholders = Seq(alice),
                cid(2),
                rc,
                isActivation = true,
              ),
            ),
            ContractChangeBatch.tryCreate(
              Map(bob -> Set(p2.toLf, p3.toLf)),
              ContractChange(
                stakeholders = Set(bob),
                locallyHostedStakeholders = Seq(alice),
                cid(2),
                rc,
                isActivation = true,
              ),
            ),

            // cid3
            ContractChangeBatch.tryCreate(
              Map(alice -> Set(p2.toLf)),
              ContractChange(
                stakeholders = Set(alice),
                locallyHostedStakeholders = Seq(alice),
                cid(3),
                rc,
                isActivation = true,
              ),
            ),

            // finally the classification that triggers p1's digest update
            PartyAddedToParticipant(alice, p1.toLf),
          )
        }

        withClue("counterparty batch size 5") {
          val classifications = classifyWithBatchSize(5)
          classifications should contain theSameElementsAs Seq(
            // cid0
            ContractChangeBatch.tryCreate(
              Map(
                alice -> Set(p2.toLf),
                bob -> Set(p2.toLf, p3.toLf),
                charlie -> Set(p1.toLf, p3.toLf, p4.toLf),
              ),
              ContractChange(
                stakeholders = Set(alice, bob, charlie),
                locallyHostedStakeholders = Seq(alice),
                cid(0),
                rc,
                isActivation = true,
              ),
            ),

            // cid1
            ContractChangeBatch.tryCreate(
              Map(alice -> Set(p2.toLf), charlie -> Set(p1.toLf, p3.toLf, p4.toLf)),
              ContractChange(
                stakeholders = Set(alice, charlie),
                locallyHostedStakeholders = Seq(alice),
                cid(1),
                rc,
                isActivation = true,
              ),
            ),

            // cid2
            ContractChangeBatch.tryCreate(
              Map(alice -> Set(p2.toLf), bob -> Set(p2.toLf, p3.toLf)),
              ContractChange(
                stakeholders = Set(alice, bob),
                locallyHostedStakeholders = Seq(alice),
                cid(2),
                rc,
                isActivation = true,
              ),
            ),

            // cid3
            ContractChangeBatch.tryCreate(
              Map(alice -> Set(p2.toLf)),
              ContractChange(
                stakeholders = Set(alice),
                locallyHostedStakeholders = Seq(alice),
                cid(3),
                rc,
                isActivation = true,
              ),
            ),

            // finally the classification that triggers p1's digest update
            PartyAddedToParticipant(alice, p1.toLf),
          )
        }
      }

      "batch contract changes in case of backpressure" in {
        // simulates that completion of onboarding alice to p1
        val testingTopology = TestingTopology(topology =
          Map(
            partyHosting(alice)(p2),
            partyHosting(bob)(p2, p3),
            partyHosting(charlie)(p1, p3, p4),
          )
        ).build()

        val numContracts = 100
        val contracts = (1 to numContracts).map(i => (off(i), cid(i), Seq(alice)))

        val rdp = mkRunningDigestProcessor(
          indexService = mkIndexService(contracts*),
          partyTopology = testingTopology.getTopology().topology,
          // by setting this value to 1, the running digest processor produces a lot of ContractChange values
          // that should get batched
          counterpartyBatchSize = 1,
          contractChangeClassificationBatchSize = 3,
        )
        val event = PartyToParticipantAuthorization(
          alice,
          p1.toLf,
          Added(AuthorizationLevel.Submission),
        )

        val toProcess =
          ProcessingContext(
            tp(numContracts + 1),
            NotCheckpointFence(testingTopology.topologySnapshot(), tte(event)),
          )

        val sink = Source
          .single(toProcess)
          .via(rdp.classification)
          .map(_.value.tryValue)
          .runWith(TestSink.probe)

        val count = new AtomicInteger(0)
        val receivedBatches = Seq.unfold(()) { _ =>
          sink.requestNext() match {
            case ContractChangeBatch(_, changes) =>
              val res = Option.when(count.getAndAdd(changes.size) < numContracts) {
                changes.size -> ()
              }
              sink.expectNoMessage()
              res
            case partyAdded: PartyAddedToParticipant =>
              count.get() shouldBe numContracts
              partyAdded shouldBe PartyAddedToParticipant(alice, p1.toLf)
              sink.expectComplete()
              None
            case otherwise => fail(s"unexpected classification: $otherwise")
          }
        }
        count.get() shouldBe numContracts
        receivedBatches.exists(_ > 1) shouldBe true

        rdp.metrics.runningDigestProcessor.latestClassifiedRecordTime.getValue shouldBe toProcess.recordTime.toMicros
      }

    }

    "running the pipeline" should {
      "notify subscribers of persisted checkpoints" in {
        val signaller = mkTickSignaller()
        val metrics = TestCommitmentMetrics()
        val rdp = mkRunningDigestProcessor(
          participant = thisParticipant,
          tickSignaller = signaller,
          reconciliationInterval = 5.seconds,
          maxNumUpdatesBetweenCheckpoints = PositiveInt.three,
          metrics = metrics,
        )

        val dummyAcsChange =
          InternalIndexService.AcsUpdate.AcsChangeUpdate(AcsChange(Map.empty, Map.empty))
        val dummyAcsCommitment =
          InternalIndexService.AcsUpdate.AcsCommitment(ByteString.empty)

        val emittedTicksF =
          signaller.readSignals(TickListener.TickOnlyListener, "tick subscriber").runWith(Sink.seq)
        val emittedTicksAndOffsetCheckpointsF = signaller
          .readSignals(
            TickListener.TicksAndReceivedCommitmentCheckpointsListener,
            "tick and offset subscriber",
          )
          .runWith(Sink.seq)
        val events = Source(
          Seq(
            ProcessingContext(tp(2), dummyAcsChange),
            ProcessingContext(tp(6), dummyAcsChange),
            ProcessingContext(tp(7), dummyAcsChange),
            ProcessingContext(tp(8), dummyAcsChange),
            ProcessingContext(tp(9), dummyAcsChange),
            ProcessingContext(tp(10), dummyAcsChange),
            ProcessingContext(
              tp(11),
              EffectiveTopologyUpdate(
                Set(PartyToParticipantAuthorization(bob, p2.toLf, Added(Submission))),
                None,
              ),
            ),
            ProcessingContext(tp(12), dummyAcsCommitment),
            ProcessingContext(tp(13), dummyAcsChange),
          )
        )

        val checkpointsFromPipeline = events
          .via(rdp.pipeline(None, None))
          .runWith(Sink.seq)
          .futureValue

        // verify the expected checkpoints to avoid bitrot
        checkpointsFromPipeline shouldBe Seq(
          CheckpointWritten(ts(0), off(1), ReconciliationIntervalBoundary),
          CheckpointWritten(ts(5), off(5), ReconciliationIntervalBoundary),
          CheckpointWritten(ts(8), off(8), MaxEventsWithoutCheckpoint),
          CheckpointWritten(ts(10), off(10), ReconciliationIntervalBoundary),
          CheckpointWritten(ts(11), off(11), PartyHostingChange),
          CheckpointWritten(ts(12), off(12), ReceivedCommitmentCheckpoint),
        )

        metrics.checkpointWatermark.getValue shouldBe ts(12).toMicros // Last checkpoint written
        metrics.tickWatermark.getValue shouldBe ts(10).toMicros // Last tick emitted

        signaller.close()

        def checkSignals(
            signals: Seq[NotificationSignal[Offset]],
            allowedSignals: Seq[CheckpointWritten],
        ): Assertion = {
          // verify that the checkpoints were emitted in offset order
          signals shouldBe signals.sortBy(_.signal)

          // verify that the ticks observed via the signaller are a subset of the output of the pipeline
          allowedSignals.map(_.offsetInclusive).toSet should contain allElementsOf
            signals.map(_.signal)
        }

        checkSignals(
          emittedTicksF.futureValue,
          checkpointsFromPipeline.filter(_.checkpointType.isTickCheckpoint),
        )

        checkSignals(
          emittedTicksAndOffsetCheckpointsF.futureValue,
          checkpointsFromPipeline.filter { checkpointWritten =>
            checkpointWritten.checkpointType.isTickCheckpoint || checkpointWritten.checkpointType == CheckpointType.ReceivedCommitmentCheckpoint
          },
        )
      }
    }
  }
}
