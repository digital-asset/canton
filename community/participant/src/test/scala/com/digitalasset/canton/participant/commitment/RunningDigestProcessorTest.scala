// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.HashOps
import com.digitalasset.canton.data.{CantonTimestamp, Counter, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  InternalIndexService,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.commitment.RunningDigestProcessor.{
  AcsUpdate,
  CheckpointFence,
  NotCheckpointFence,
  PartyAddedToParticipant,
  PartyOnboardingToParticipant,
  PartyRemovedFromParticipant,
  ProcessingContext,
}
import com.digitalasset.canton.participant.config.AcsDigestTracingMode
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.SynchronizerParameters.WithValidity
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  ExampleTransactionFactory,
  LfContractId,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.TopologyTransactionTestFactory
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ParticipantId,
  SynchronizerId,
  TestingTopology,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  HasActorSystem,
  HasExecutionContext,
  LfPartyId,
  ReassignmentCounter,
  ReassignmentDiscriminator,
}
import com.digitalasset.daml.lf.data.Ref.Party
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable
import scala.concurrent.duration.*
import scala.language.implicitConversions

class RunningDigestProcessorTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with HasActorSystem {

  import RunningDigestProcessorTest.*

  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

  val alice = party("alice::aaa")
  val bob = party("bob::bbb")
  val charlie = party("charlie::ccc")

  val p1 = ParticipantId.tryFromProtoPrimitive("PAR::p1::zzz")
  val p2 = ParticipantId.tryFromProtoPrimitive("PAR::p2::yyy")
  val p3 = ParticipantId.tryFromProtoPrimitive("PAR::p3::xxx")
  val p4 = ParticipantId.tryFromProtoPrimitive("PAR::p4::www")
  val thisParticipant = p1

  val ts0 = ts(0)
  val off1 = off(1)

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
      maxNumUpdatesBetweenCheckpoints = maxNumUpdatesBetweenCheckpoints,
      indexService,
      getTopologySnapshot = ts =>
        FutureUnlessShutdown.pure(testingTopology.topologySnapshot(timestampOfSnapshot = ts)),
      mock[AcsDigestStore],
      mock[StringInterning],
      mock[HashOps],
      PositiveInt.tryCreate(counterpartyBatchSize),
      AcsDigestTracingMode.Disabled,
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
            ProcessingContext(ts((2)), off(2), dummyAcsChange),
            ProcessingContext(ts((3)), off(3), dummyAcsChange),
            ProcessingContext(ts((5)), off(5), dummyAcsChange),
            ProcessingContext(ts((6)), off(6), dummyAcsChange),
            ProcessingContext(ts((7)), off(7), dummyAcsChange),
          )
        ).via(rdp.checkpointing).runWith(Sink.seq).futureValue

        result.map(_.map(_.toOption)) should contain theSameElementsInOrderAs Seq(
          // TODO(#33084) Remove initial CheckpointFence once crash recovery is implemented
          ProcessingContext(ts(0), off(1), None),
          ProcessingContext(ts(2), off(2), Some(dummyAcsChange)),
          ProcessingContext(ts(3), off(3), Some(dummyAcsChange)),
          ProcessingContext(ts(5), off(5), Some(dummyAcsChange)),
          ProcessingContext(ts(5), off(5), None),
          ProcessingContext(ts(6), off(6), Some(dummyAcsChange)),
          ProcessingContext(ts(7), off(7), Some(dummyAcsChange)),
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
            ProcessingContext(ts(2), off(2), dummyAcsChange),
            ProcessingContext(ts(2), off(3), dummyAcsChange),
            ProcessingContext(ts(2), off(4), dummyAcsChange),
            ProcessingContext(ts(3), off(5), dummyAcsChange),
            ProcessingContext(ts(5), off(8), dummyAcsChange),
            ProcessingContext(ts(6), off(9), dummyAcsChange),
          )
        ).via(rdp.checkpointing).runWith(Sink.seq).futureValue

        result.map(_.map(_.toOption)) should contain theSameElementsInOrderAs Seq(
          // TODO(#33084) Remove initial CheckpointFence once crash recovery is implemented
          ProcessingContext(ts(0), off(1), None),
          ProcessingContext(ts(2), off(2), Some(dummyAcsChange)),
          ProcessingContext(ts(2), off(3), Some(dummyAcsChange)),
          ProcessingContext(ts(2), off(4), Some(dummyAcsChange)),
          ProcessingContext(ts(3).immediatePredecessor, off(4), None),
          ProcessingContext(ts(3), off(5), Some(dummyAcsChange)),
          ProcessingContext(ts(5), off(8), Some(dummyAcsChange)),
          ProcessingContext(ts(6).immediatePredecessor, off(8), None),
          ProcessingContext(ts(6), off(9), Some(dummyAcsChange)),
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
          ProcessingContext(ts(timeOffset), off(timeOffset), tte(event))
        }

        val rdp = mkRunningDigestProcessor(
          // make sure that the checkpoint intervals do not come into play
          maxNumUpdatesBetweenCheckpoints = PositiveInt.tryCreate(100),
          reconciliationInterval = 1.hour,
        )

        val result = Source(inputEvents).via(rdp.checkpointing).runWith(Sink.seq).futureValue

        // match on CheckpointFenceOr[InputEvent].toOption, so we don't have to match on the topology snapshot
        val expectedResult =
          // add a fence AFTER each input event with the same timestamp as the input event
          inputEvents.flatMap { input =>
            Seq(
              ProcessingContext(input.recordTime, input.offset, Some(input.value)),
              ProcessingContext(input.recordTime, input.offset, None),
            )
          }

        result.map(_.map(_.toOption)) should contain theSameElementsInOrderAs (expectedResult)
      }
    }

    // test cases for the classification stage
    "classifying" should {
      "pass checkpoint fences through" in {
        val rdp = mkRunningDigestProcessor()

        val fence = ProcessingContext(ts0, off1, CheckpointFence)

        val result = Source
          .single(fence)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue
          .loneElement

        result shouldBe ProcessingContext(ts0, off1, CheckpointFence)
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
            ts0,
            off1,
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
          ProcessingContext(ts0, off1, NotCheckpointFence(topologySnapshot, tte(event)))
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
          ProcessingContext(ts0, off1, NotCheckpointFence(topologySnapshot, tte(event)))

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
          ProcessingContext(ts0, off1, NotCheckpointFence(topologySnapshot, tte(event)))

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
            ts(2),
            off(2),
            NotCheckpointFence(
              testingTopology.topologySnapshot(),
              tte(p1_unhosts_alice, p2_hosts_alice, p1_hosts_bob),
            ),
          )

        def processTopologyEventsWithParticipant(
            participant: ParticipantId
        ): Seq[RunningDigestProcessor.Classification] = {

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
              ts(3),
              off(3),
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

    "reinitializing" should {
      val topologySnapshot = TestingTopology(topology =
        Map(
          partyHosting(alice)(p1, p2),
          partyHosting(bob)(p2, p3),
          partyHosting(charlie)(p1, p3, p4),
        )
      ).build().topologySnapshot()

      "handle a simple case of a few ACS updates" in {
        val rdp = mkRunningDigestProcessor(
          indexService = mkIndexService(
            (off(1), cid(1), Seq(party(alice), party(bob))),
            (off(2), cid(2), Seq(party(alice), party(bob), party(charlie))),
          )
        )

        val acsUpdates = rdp
          .reinitializationAcsUpdates(ts(3), off(3), topologySnapshot)
          .runWith(Sink.seq)
          .futureValue

        acsUpdates shouldBe Seq(
          ProcessingContext(
            ts(3),
            off(3),
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                ),
                locallyHostedStakeholders = Seq(alice),
                cid = cid(1),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            ts(3),
            off(3),
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                  charlie -> Set(p1.toLf, p3.toLf, p4.toLf),
                ),
                locallyHostedStakeholders = Seq(alice, charlie),
                cid = cid(2),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            ts(3),
            off(3),
            CheckpointFence,
          ),
        )
      }

      "handle grouping and filtering by record time" in {
        val before = off(1)
        val requestedOffset = off(2)
        val after = off(3)

        val rdp = mkRunningDigestProcessor(
          indexService = mkIndexService(
            (before, cid(1), Seq(party(alice), party(bob))),
            (before, cid(2), Seq(party(alice), party(bob), party(charlie))),
            (requestedOffset, cid(3), Seq(party(alice), party(bob))),
            (after, cid(4), Seq(party(alice), party(charlie))), // Should be filtered out
          ),
          counterpartyBatchSize = 2,
        )

        val acsUpdates = rdp
          .reinitializationAcsUpdates(
            requestedOffset.toEpochTimestamp,
            requestedOffset,
            topologySnapshot,
          )
          .runWith(Sink.seq)
          .futureValue

        acsUpdates shouldBe Seq(
          ProcessingContext(
            requestedOffset.toEpochTimestamp,
            requestedOffset,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                ),
                locallyHostedStakeholders = Seq(alice),
                cid = cid(1),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            requestedOffset.toEpochTimestamp,
            requestedOffset,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                ),
                locallyHostedStakeholders = Seq(alice, charlie),
                cid = cid(2),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            requestedOffset.toEpochTimestamp,
            requestedOffset,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                ),
                locallyHostedStakeholders = Seq(alice),
                cid = cid(3),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            requestedOffset.toEpochTimestamp,
            requestedOffset,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  charlie -> Set(p1.toLf, p3.toLf, p4.toLf)
                ),
                locallyHostedStakeholders = Seq(alice, charlie),
                cid = cid(2),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            requestedOffset.toEpochTimestamp,
            requestedOffset,
            CheckpointFence,
          ),
        )
      }
    }
  }
}
object RunningDigestProcessorTest {
  private val rc: Counter[ReassignmentDiscriminator] = ReassignmentCounter.Genesis

  implicit def toAcsChangeData(
      parties: Set[LfPartyId]
  ): ContractStakeholdersAndReassignmentCounter =
    ContractStakeholdersAndReassignmentCounter(parties, rc)
  implicit class RichOffset(off: Offset) {
    def toEpochTimestamp: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(off.positive)
  }

  def cid(i: Int): LfContractId = ExampleTransactionFactory.suffixedId(i, i)
  def ts(i: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(i.toLong)
  def off(i: Int): Offset = Offset.tryFromLong(i.toLong)
  def tte(events: PartyToParticipantAuthorization*) =
    InternalIndexService.AcsUpdate.EffectivePartyToParticipantMappings(events.toSet)
  def party(s: String): LfPartyId = LfPartyId.assertFromString(s)

  def partyHosting(party: LfPartyId)(participants: ParticipantId*): (LfPartyId, PartyInfo) =
    (
      party,
      PartyInfo(PositiveInt.one, participants.map(_ -> ParticipantAttributes(Submission)).toMap),
    )

  def mkIndexService(
      contractsWithStakeholders: (Offset, LfContractId, Seq[LfPartyId])*
  ): InternalIndexService = {
    val acs = for {
      (offset, cid, rawStakeholders) <- contractsWithStakeholders
      stakeholders = rawStakeholders.map(LfPartyId.assertFromString)
      activeContract = InternalIndexService.ActiveContract(cid, stakeholders.toSet, rc)
      stakeholder <- stakeholders
    } yield {
      stakeholder -> (offset, activeContract)
    }
    val partyToContracts = immutable.MultiDict.from(acs)

    new InternalIndexService {
      override def activeContracts(partyIds: Set[LfPartyId], validAt: Option[Offset])(implicit
          traceContext: TraceContext
      ): Source[GetActiveContractsResponse, NotUsed] = ???

      override def topologyTransactions(partyId: LfPartyId, fromExclusive: Offset)(implicit
          traceContext: TraceContext
      ): Source[TopologyTransaction, NotUsed] = ???

      override def acsUpdates(synchronizerId: SynchronizerId, fromExclusive: Option[Offset])(
          implicit traceContext: TraceContext
      ): Source[InternalIndexService.AcsUpdateContainer, NotUsed] = ???

      override def acs(
          synchronizerId: SynchronizerId,
          activeAt: Offset,
          stakeholders1: Set[Party],
          stakeholders2: Set[Party],
      )(implicit
          traceContext: TraceContext
      ): Source[InternalIndexService.ActiveContract, NotUsed] = {
        val result =
          partyToContracts.values.collect {
            case (rt, contract)
                if rt <= activeAt &&
                  (stakeholders1.isEmpty || contract.stakeholders.exists(
                    stakeholders1
                  )) && (stakeholders2.isEmpty || contract.stakeholders.exists(stakeholders2)) =>
              contract
          }.toSet

        Source(result)
      }

      override def counterParties(
          synchronizerId: SynchronizerId,
          activeAt: Offset,
          party: Option[Party],
      )(implicit traceContext: TraceContext): Source[LfPartyId, NotUsed] = Source(
        party
          .flatMap(partyToContracts.sets.get(_))
          .getOrElse(partyToContracts.sets.values.flatten)
          .flatMap { case (offset, c) => if (offset <= activeAt) c.stakeholders else Set.empty }
          .toSet
      )
    }
  }

}
