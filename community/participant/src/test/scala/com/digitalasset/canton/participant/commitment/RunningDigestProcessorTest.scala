// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, Counter}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  Onboarding,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationLevel,
  TopologyEvent,
}
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.commitment.AcsLookup.AcsActiveContract
import com.digitalasset.canton.participant.commitment.RunningDigestProcessor.{
  AcsUpdate,
  CheckpointFence,
  InputEvent,
  NotCheckpointFence,
  PartyAddedToRemoteParticipant,
  PartyOnboardingToRemoteParticipant,
  PartyRemovedFromRemoteParticipant,
  ProcessingContext,
}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.protocol.SynchronizerParameters.WithValidity
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  ExampleTransactionFactory,
  LfContractId,
  UpdateId,
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
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable
import scala.concurrent.duration.*
import scala.language.implicitConversions
import scala.math.Ordering.Implicits.*

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

  val ts0 = rt(0)

  def mkRunningDigestProcessor(
      participant: ParticipantId = thisParticipant,
      acsLookup: AcsLookup = mkAcsLookup(),
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
      acsLookup,
      getTopologySnapshot = ts =>
        FutureUnlessShutdown.pure(testingTopology.topologySnapshot(timestampOfSnapshot = ts)),
      PositiveInt.tryCreate(counterpartyBatchSize),
      loggerFactory,
    )
  }

  "RunningDigestProcessor" when {

    "checkpointing" should {
      "emit a checkpoint fence for AcsChanges that cross a reconciliation invertval boundary" in {
        val rdp = mkRunningDigestProcessor(
          reconciliationInterval = 5.seconds
        )

        val dummyAcsChange = InputEvent(AcsChange(Map.empty, Map.empty))
        val result = Source(
          Seq(
            ProcessingContext(rt(1), dummyAcsChange),
            ProcessingContext(rt(2), dummyAcsChange),
            ProcessingContext(rt(3), dummyAcsChange),
            ProcessingContext(rt(5), dummyAcsChange),
            ProcessingContext(rt(6), dummyAcsChange),
          )
        ).via(rdp.checkpointing).runWith(Sink.seq).futureValue

        result.map(_.map(_.toOption)) should contain theSameElementsInOrderAs Seq(
          // TODO(#33084) Remove initial CheckpointFence once crash recovery is implemented
          ProcessingContext(rt(0), None),
          ProcessingContext(rt(1), Some(dummyAcsChange)),
          ProcessingContext(rt(2), Some(dummyAcsChange)),
          ProcessingContext(rt(3), Some(dummyAcsChange)),
          ProcessingContext(rt(5), Some(dummyAcsChange)),
          ProcessingContext(rt(5), None),
          ProcessingContext(rt(6), Some(dummyAcsChange)),
        )
      }

      "emit a checkpoint fence every n events without checkpoints" in {
        val rdp = mkRunningDigestProcessor(
          maxNumUpdatesBetweenCheckpoints = PositiveInt.two,
          // make sure that the checkpoint intervals do not come into play
          reconciliationInterval = 1.hour,
        )

        val dummyAcsChange = InputEvent(AcsChange(Map.empty, Map.empty))
        val result = Source(
          Seq(
            ProcessingContext(rt(1), dummyAcsChange),
            ProcessingContext(rt(2), dummyAcsChange),
            ProcessingContext(rt(2), dummyAcsChange),
            ProcessingContext(rt(2), dummyAcsChange),
            ProcessingContext(rt(3), dummyAcsChange),
            ProcessingContext(rt(5), dummyAcsChange),
            ProcessingContext(rt(6), dummyAcsChange),
          )
        ).via(rdp.checkpointing).runWith(Sink.seq).futureValue

        result.map(_.map(_.toOption)) should contain theSameElementsInOrderAs Seq(
          // TODO(#33084) Remove initial CheckpointFence once crash recovery is implemented
          ProcessingContext(rt(0), None),
          ProcessingContext(rt(1), Some(dummyAcsChange)),
          ProcessingContext(rt(2), Some(dummyAcsChange)),
          ProcessingContext(rt(2), Some(dummyAcsChange)),
          ProcessingContext(rt(2), Some(dummyAcsChange)),
          ProcessingContext(rt(3).immediatePredecessor, None),
          ProcessingContext(rt(3), Some(dummyAcsChange)),
          ProcessingContext(rt(5), Some(dummyAcsChange)),
          ProcessingContext(rt(6).immediatePredecessor, None),
          ProcessingContext(rt(6), Some(dummyAcsChange)),
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
          ProcessingContext(rt(timeOffset), InputEvent(tte(timeOffset, event)))
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
              ProcessingContext(input.recordTime, Some(input.value)),
              ProcessingContext(input.recordTime, None),
            )
          }

        result.map(_.map(_.toOption)) should contain theSameElementsInOrderAs (expectedResult)
      }
    }

    // test cases for the classification stage
    "classifying" should {
      "pass checkpoint fences through" in {
        val rdp = mkRunningDigestProcessor()

        val fence = ProcessingContext(ts0, CheckpointFence)

        val result = Source
          .single(fence)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue
          .loneElement

        result shouldBe ProcessingContext(ts0, CheckpointFence)
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
          // one of the stakeholders is hosted by thisParticipant
          activations = Map(cid(0) -> Set(alice, bob)),
          // both stakeholders of the contract are hosted by thisParticipant
          deactivations = Map(cid(1) -> Set(alice, charlie)),
        )

        val toProcess =
          ProcessingContext(ts0, NotCheckpointFence(topologySnapshot, InputEvent(event)))

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
          ProcessingContext(rt(0), NotCheckpointFence(topologySnapshot, InputEvent(tte(0, event))))
        val rdp = mkRunningDigestProcessor()

        val result = Source
          .single(toProcess)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue
          .loneElement

        result.value.tryValue shouldBe PartyOnboardingToRemoteParticipant(bob, p3.toLf)
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
          ProcessingContext(rt(0), NotCheckpointFence(topologySnapshot, InputEvent(tte(0, event))))

        val rdp = mkRunningDigestProcessor()

        val result = Source
          .single(toProcess)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue
          .loneElement

        result.value.tryValue shouldBe PartyAddedToRemoteParticipant(bob, p3.toLf)
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
          ProcessingContext(rt(0), NotCheckpointFence(topologySnapshot, InputEvent(tte(0, event))))

        val rdp = mkRunningDigestProcessor()

        val result = Source
          .single(toProcess)
          .via(rdp.classification)
          .runWith(Sink.seq)
          .futureValue
          .loneElement

        result.value.tryValue shouldBe PartyRemovedFromRemoteParticipant(bob, p3.toLf)
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
            rt(2),
            NotCheckpointFence(
              testingTopology.topologySnapshot(),
              InputEvent(tte(2, p1_unhosts_alice, p2_hosts_alice, p1_hosts_bob)),
            ),
          )

        def processTopologyEventsWithParticipant(
            participant: ParticipantId
        ): Seq[RunningDigestProcessor.Classification] = {

          val rdp_p1 = mkRunningDigestProcessor(
            participant = participant,
            acsLookup = mkAcsLookup(
              (rt(0), cid(1), Seq(alice, bob, charlie))
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
          AcsUpdate(
            Map(
              alice -> Set(p1.toLf),
              bob -> Set(p2.toLf),
              charlie -> Set(p1.toLf, p2.toLf),
            ),
            Seq(alice),
            cid(1),
            rc,
            isActivation = false,
          ),
          PartyAddedToRemoteParticipant(alice, p2.toLf),
          AcsUpdate(
            Map(
              alice -> Set(p2.toLf),
              bob -> Set(p1.toLf, p2.toLf),
              charlie -> Set(p1.toLf, p2.toLf),
            ),
            Seq(bob),
            cid(1),
            rc,
            isActivation = true,
          ),
        )

        val resultP2 = processTopologyEventsWithParticipant(p2)

        resultP2 should contain theSameElementsInOrderAs Seq(
          PartyRemovedFromRemoteParticipant(alice, p1.toLf),
          AcsUpdate(
            Map(
              alice -> Set(p2.toLf),
              bob -> Set(p2.toLf),
              charlie -> Set(p1.toLf, p2.toLf),
            ),
            Seq(alice),
            cid(1),
            rc,
            isActivation = true,
          ),
          PartyAddedToRemoteParticipant(bob, p1.toLf),
        )

      }

      "handle the completion of a party onboarding to the local participant" in {
        // simulates that completion of onboarding alice to p1
        // TODO(#33084) decide which topology snapshot to take for topology events
        val testingTopology = TestingTopology(topology =
          Map(
            partyHosting(alice)(p1, p2),
            partyHosting(bob)(p2, p3),
            partyHosting(charlie)(p1, p3, p4),
          )
        ).build()

        def classifyWithBatchSize(batchSize: Int) = {
          val rdp = mkRunningDigestProcessor(
            acsLookup = mkAcsLookup(
              (rt(0), cid(0), Seq(alice, bob, charlie)),
              (rt(1), cid(1), Seq(alice, charlie)),
              (rt(1), cid(2), Seq(alice, bob)),
              (rt(1), cid(3), Seq(alice)),
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
              rt(2),
              NotCheckpointFence(testingTopology.topologySnapshot(), InputEvent(tte(2, event))),
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
              stakeholders = Map(alice -> Set(p1.toLf, p2.toLf)),
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
              stakeholders = Map(alice -> Set(p1.toLf, p2.toLf)),
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
              stakeholders = Map(alice -> Set(p1.toLf, p2.toLf)),
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
              stakeholders = Map(alice -> Set(p1.toLf, p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(3),
              rc,
              isActivation = true,
            ),
          )
        }

        withClue("counterparty batch size 5") {
          val classifications = classifyWithBatchSize(5)
          classifications should contain theSameElementsAs Seq(
            // cid0
            AcsUpdate(
              stakeholders = Map(
                alice -> Set(p1.toLf, p2.toLf),
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
              stakeholders =
                Map(alice -> Set(p1.toLf, p2.toLf), charlie -> Set(p1.toLf, p3.toLf, p4.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(1),
              rc,
              isActivation = true,
            ),

            // cid2
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1.toLf, p2.toLf), bob -> Set(p2.toLf, p3.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(2),
              rc,
              isActivation = true,
            ),

            // cid3
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1.toLf, p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(3),
              rc,
              isActivation = true,
            ),
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
          acsLookup = mkAcsLookup(
            (rt(1), cid(1), Seq(party(alice), party(bob))),
            (rt(2), cid(2), Seq(party(alice), party(bob), party(charlie))),
          )
        )

        val acsUpdates = rdp
          .reinitializationAcsUpdates(rt(3), topologySnapshot)
          .runWith(Sink.seq)
          .futureValue

        acsUpdates shouldBe Seq(
          ProcessingContext(
            rt(3),
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
            rt(3),
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
            rt(3),
            CheckpointFence,
          ),
        )
      }

      "handle grouping and filtering by record time" in {
        val before = rt(1)
        val requestedTime = rt(2)
        val after = rt(3)

        val rdp = mkRunningDigestProcessor(
          acsLookup = mkAcsLookup(
            (before, cid(1), Seq(party(alice), party(bob))),
            (before, cid(2), Seq(party(alice), party(bob), party(charlie))),
            (requestedTime, cid(3), Seq(party(alice), party(bob))),
            (after, cid(4), Seq(party(alice), party(charlie))), // Should be filtered out
          ),
          counterpartyBatchSize = 2,
        )

        val acsUpdates = rdp
          .reinitializationAcsUpdates(requestedTime, topologySnapshot)
          .runWith(Sink.seq)
          .futureValue

        acsUpdates shouldBe Seq(
          ProcessingContext(
            requestedTime,
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
            requestedTime,
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
            requestedTime,
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
            requestedTime,
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
            requestedTime,
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
  def cid(i: Int): LfContractId = ExampleTransactionFactory.suffixedId(i, i)
  def ts(i: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(i.toLong)
  def rt(i: Int): RecordTime = RecordTime(ts(i), 0)
  def tte(i: Int, events: TopologyEvent*) = Update.TopologyTransactionEffective(
    UpdateId.zero,
    events.toSet,
    DefaultTestIdentities.synchronizerId,
    ts(i),
  )(TraceContext.empty)
  def party(s: String): LfPartyId = LfPartyId.assertFromString(s)

  def partyHosting(party: LfPartyId)(participants: ParticipantId*): (LfPartyId, PartyInfo) =
    (
      party,
      PartyInfo(PositiveInt.one, participants.map(_ -> ParticipantAttributes(Submission)).toMap),
    )

  implicit class RichRecordtime(rt: RecordTime) {
    def immediatePredecessor: RecordTime =
      RunningDigestProcessor.immediatePredecessor(rt)
  }

  def mkAcsLookup(
      contractsWithStakeholders: (RecordTime, LfContractId, Seq[LfPartyId])*
  ): AcsLookup = {
    val acs = for {
      (recordTime, cid, rawStakeholders) <- contractsWithStakeholders
      stakeholders = rawStakeholders.map(LfPartyId.assertFromString)
      activeContract = AcsActiveContract(cid, rc, stakeholders.toSet)
      stakeholder <- stakeholders
    } yield {
      stakeholder -> (recordTime, activeContract)
    }
    val partyToContracts = immutable.MultiDict.from(acs)

    new AcsLookup {
      override def contractMetadata(
          anyOf: Set[LfPartyId],
          anyOf2: Set[LfPartyId],
          synchronizerId: SynchronizerId,
          asOfInclusive: RecordTime,
      )(implicit traceContext: TraceContext): Source[AcsActiveContract, NotUsed] = {
        val result =
          partyToContracts.values.collect {
            case (rt, contract)
                if rt <= asOfInclusive &&
                  (anyOf.isEmpty || contract.stakeholders.exists(
                    anyOf
                  )) && (anyOf2.isEmpty || contract.stakeholders.exists(anyOf2)) =>
              contract
          }.toSet

        Source(result)
      }
    }
  }

}
