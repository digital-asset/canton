// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationLevel,
  GenericTopologyEvent,
}
import com.digitalasset.canton.ledger.participant.state.{AcsChange, InternalIndexService}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  AcsUpdate,
  CheckpointFence,
  NotCheckpointFence,
  PartyAddedToParticipant,
  PartyOnboardingToParticipant,
  PartyRemovedFromParticipant,
  ProcessingContext,
}
import com.digitalasset.canton.participant.config.{AcsCommitmentConfig, AcsDigestTracingMode}
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.ReconciliationIntervalBoundary
import com.digitalasset.canton.participant.store.memory.InMemoryAcsDigestStore
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.protocol.SynchronizerParameters.WithValidity
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.TopologyTransactionTestFactory
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{
  SynchronizerParametersState,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, ParticipantId, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext, LfPartyId}
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import scala.concurrent.duration.*

@AcsCommitmentTest
class RunningDigestProcessorTest
    extends BaseDigestProcessorTest
    with HasExecutionContext
    with HasActorSystem {

  import BaseDigestProcessorTest.*

  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

  def mkRunningDigestProcessor(
      participant: ParticipantId = thisParticipant,
      indexService: InternalIndexService = mkIndexService(),
      counterpartyBatchSize: Int = 10,
      partyTopology: Map[LfPartyId, PartyInfo] = Map.empty,
      maxNumUpdatesBetweenCheckpoints: PositiveInt = PositiveInt.tryCreate(5),
      reconciliationInterval: FiniteDuration = DynamicSynchronizerParameters
        .defaultValues(testedProtocolVersion)
        .reconciliationInterval
        .toFiniteDuration,
  ): RunningDigestProcessor = {
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

    new RunningDigestProcessor(
      participant,
      synchronizerId = DefaultTestIdentities.synchronizerId,
      AcsCommitmentConfig(
        enableRunningDigestProcessor = true,
        maxNumUpdatesBetweenCheckpoints = maxNumUpdatesBetweenCheckpoints,
        counterpartyBatchSize = PositiveInt.tryCreate(counterpartyBatchSize),
        AcsDigestTracingMode.Disabled,
      ),
      indexService,
      getTopologySnapshot = ts =>
        FutureUnlessShutdown.pure(testingTopology.topologySnapshot(timestampOfSnapshot = ts.value)),
      InMemoryAcsDigestStore.create(Eval.now(mock[StringInterning]), loggerFactory),
      mock[StringInterning],
      mock[HashOps],
      timeouts,
      loggerFactory,
    )
  }

  "RunningDigestProcessor" when {

    "checkpointing" should {
      "emit a checkpoint fence for AcsChanges that cross a reconciliation invertval boundary" in {
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
        val result = Source(
          Seq(
            ProcessingContext(tp(2), dummyAcsChange),
            ProcessingContext(Timepoint(off(3))(ts(2)), dummyAcsChange),
            ProcessingContext(Timepoint(off(4))(ts(2)), dummyAcsChange),
            ProcessingContext(Timepoint(off(5))(ts(3)), dummyAcsChange),
            ProcessingContext(Timepoint(off(8))(ts(5)), dummyAcsChange),
            ProcessingContext(Timepoint(off(9))(ts(6)), dummyAcsChange),
          )
        ).via(rdp.checkpointing(None, TraceContext.empty)).runWith(Sink.seq).futureValue

        result.map(_.map(_.toEither)) should contain theSameElementsInOrderAs Seq(
          ProcessingContext(tp1_0, Left(CheckpointType.ReconciliationIntervalBoundary)),
          ProcessingContext(tp(2), Right(dummyAcsChange)),
          ProcessingContext(Timepoint(off(3))(ts(2)), Right(dummyAcsChange)),
          ProcessingContext(
            Timepoint(off(3))(ts(2).immediatePredecessor),
            Left(CheckpointType.MaxEventsWithoutCheckpoint),
          ),
          ProcessingContext(Timepoint(off(4))(ts(2)), Right(dummyAcsChange)),
          ProcessingContext(Timepoint(off(5))(ts(3)), Right(dummyAcsChange)),
          ProcessingContext(
            Timepoint(off(7))(ts(5).immediatePredecessor),
            Left(CheckpointType.MaxEventsWithoutCheckpoint),
          ),
          ProcessingContext(Timepoint(off(8))(ts(5)), Right(dummyAcsChange)),
          ProcessingContext(Timepoint(off(9))(ts(6)), Right(dummyAcsChange)),
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

        // match on CheckpointFenceOr[InputEvent].toEither, so we don't have to match on the topology snapshot
        val expectedResult =
          // add a fence AFTER each input event with the same timestamp as the input event
          inputEvents.flatMap { input =>
            Seq(
              ProcessingContext(input.timepoint, Right(input.value)),
              ProcessingContext(input.timepoint, Left(CheckpointType.PartyHostingChange)),
            )
          }

        result.map(_.map(_.toEither)) should contain theSameElementsInOrderAs (expectedResult)
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

        // match on CheckpointFenceOr[InputEvent].toEither, so we don't have to match on the topology snapshot
        val expectedResult =
          Seq(
            ProcessingContext(tp(1), Right(dummyAcsChange)),
            ProcessingContext(tp(2), Left(CheckpointType.ReconciliationIntervalBoundary)),
            ProcessingContext(tp(3), Right(dummyAcsChange)),
          )

        result.map(_.map(_.toEither)) should contain theSameElementsInOrderAs (expectedResult)
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
            cid(2) -> Set(bob),
          ),
          deactivations = Map(
            // both stakeholders of the contract are hosted by thisParticipant
            cid(1) -> Set(alice, charlie),
            // NONE of the stakeholders is hosted by thisParticipant. the change will be ignored
            cid(3) -> Set(bob),
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

        result.map(_.value.tryValue) should contain theSameElementsAs Seq(
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1.toLf, p2.toLf), bob -> Set(p2.toLf, p3.toLf)),
            locallyHostedStakeholders = Seq(alice),
            cid(0),
            rc,
            isActivation = true,
          ),
          AcsUpdate(
            stakeholders =
              Map(alice -> Set(p1.toLf, p2.toLf), charlie -> Set(p1.toLf, p3.toLf, p4.toLf)),
            locallyHostedStakeholders = Seq(alice, charlie),
            cid(1),
            rc,
            isActivation = false,
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

          val rdp_p1 = mkRunningDigestProcessor(
            participant = participant,
            indexService = mkIndexService(
              (off(1), cid(1), Seq(alice, bob, charlie))
            ),
            partyTopology = testingTopology.getTopology().topology,
            counterpartyBatchSize = 10,
          )

          val result = Source
            .single(toProcess)
            .via(rdp_p1.classification)
            .runWith(Sink.seq)
            .futureValue
            .map(_.value.tryValue)

          result
        }
        val resultP1 = processTopologyEventsWithParticipant(p1)

        resultP1 should contain theSameElementsInOrderAs Seq(
          // p1_unhosts_alice
          PartyRemovedFromParticipant(alice, p1.toLf),
          AcsUpdate(
            Map(
              alice -> Set(),
              bob -> Set(p2.toLf),
              charlie -> Set(p1.toLf, p2.toLf),
            ),
            Seq(alice),
            cid(1),
            rc,
            isActivation = false,
          ),

          // p2_hosts_alice
          PartyAddedToParticipant(alice, p2.toLf),

          // p1_hosts_bob
          AcsUpdate(
            Map(
              alice -> Set(p2.toLf),
              bob -> Set(p2.toLf),
              charlie -> Set(p1.toLf, p2.toLf),
            ),
            Seq(bob),
            cid(1),
            rc,
            isActivation = true,
          ),
          PartyAddedToParticipant(bob, p1.toLf),
        )

        val resultP2 = processTopologyEventsWithParticipant(p2)

        resultP2 should contain theSameElementsInOrderAs Seq(
          // p1_unhosts_alice
          PartyRemovedFromParticipant(alice, p1.toLf),

          // p2_hosts_alice
          AcsUpdate(
            Map(
              alice -> Set(),
              bob -> Set(p2.toLf),
              charlie -> Set(p1.toLf, p2.toLf),
            ),
            Seq(alice),
            cid(1),
            rc,
            isActivation = true,
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

          Source
            .single(toProcess)
            .via(rdp.classification)
            .runWith(Sink.seq)
            .futureValue
            .map(_.value.tryValue)
        }

        withClue("counterparty batch size 1") {
          val classifications = classifyWithBatchSize(1)
          classifications should contain theSameElementsAs Seq(
            // cid0
            AcsUpdate(
              stakeholders = Map(alice -> Set(p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(0),
              rc,
              isActivation = true,
            ),
            AcsUpdate(
              stakeholders = Map(bob -> Set(p2.toLf, p3.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(0),
              rc,
              isActivation = true,
            ),
            AcsUpdate(
              stakeholders = Map(charlie -> Set(p1.toLf, p3.toLf, p4.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(0),
              rc,
              isActivation = true,
            ),

            // cid1
            AcsUpdate(
              stakeholders = Map(alice -> Set(p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(1),
              rc,
              isActivation = true,
            ),
            AcsUpdate(
              stakeholders = Map(charlie -> Set(p1.toLf, p3.toLf, p4.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(1),
              rc,
              isActivation = true,
            ),

            // cid2
            AcsUpdate(
              stakeholders = Map(alice -> Set(p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(2),
              rc,
              isActivation = true,
            ),
            AcsUpdate(
              stakeholders = Map(bob -> Set(p2.toLf, p3.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(2),
              rc,
              isActivation = true,
            ),

            // cid3
            AcsUpdate(
              stakeholders = Map(alice -> Set(p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(3),
              rc,
              isActivation = true,
            ),

            // finally the classification that triggers p1's digest update
            PartyAddedToParticipant(alice, p1.toLf),
          )
        }

        withClue("counterparty batch size 5") {
          val classifications = classifyWithBatchSize(5)
          classifications should contain theSameElementsAs Seq(
            // cid0
            AcsUpdate(
              stakeholders = Map(
                alice -> Set(p2.toLf),
                bob -> Set(p2.toLf, p3.toLf),
                charlie -> Set(p1.toLf, p3.toLf, p4.toLf),
              ),
              locallyHostedStakeholders = Seq(alice),
              cid(0),
              rc,
              isActivation = true,
            ),

            // cid1
            AcsUpdate(
              stakeholders = Map(alice -> Set(p2.toLf), charlie -> Set(p1.toLf, p3.toLf, p4.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(1),
              rc,
              isActivation = true,
            ),

            // cid2
            AcsUpdate(
              stakeholders = Map(alice -> Set(p2.toLf), bob -> Set(p2.toLf, p3.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(2),
              rc,
              isActivation = true,
            ),

            // cid3
            AcsUpdate(
              stakeholders = Map(alice -> Set(p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(3),
              rc,
              isActivation = true,
            ),

            // finally the classification that triggers p1's digest update
            PartyAddedToParticipant(alice, p1.toLf),
          )
        }
      }
    }
  }
}
