// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
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
}
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
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.processing.TopologyTransactionTestFactory
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{ParticipantId, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  HasActorSystem,
  HasExecutionContext,
  LfPartyId,
  ReassignmentCounter,
}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable
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
      acsLookup: AcsLookup = mkAcsLookup(),
      counterpartyBatchSize: Int = 10,
  ): RunningDigestProcessor =
    new RunningDigestProcessor(
      thisParticipant,
      acsLookup,
      PositiveInt.tryCreate(counterpartyBatchSize),
      loggerFactory,
    )

  "RunningDigestProcessor" when {

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
            stakeholders = Map(alice -> Seq(p1.toLf, p2.toLf), bob -> Seq(p2.toLf, p3.toLf)),
            locallyHostedStakeholders = Seq(alice),
            cid(0),
            rc,
            isActivation = true,
          ),
          AcsUpdate(
            stakeholders =
              Map(alice -> Seq(p1.toLf, p2.toLf), charlie -> Seq(p1.toLf, p3.toLf, p4.toLf)),
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
          ProcessingContext(ts0, NotCheckpointFence(topologySnapshot, InputEvent(event)))
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
          ProcessingContext(ts0, NotCheckpointFence(topologySnapshot, InputEvent(event)))

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
          ProcessingContext(ts0, NotCheckpointFence(topologySnapshot, InputEvent(event)))

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
            mkAcsLookup(
              (rt(0), cid(0), Seq(alice, bob, charlie)),
              (rt(1), cid(1), Seq(alice, charlie)),
              (rt(1), cid(2), Seq(alice, bob)),
              (rt(1), cid(3), Seq(alice)),
            ),
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
              NotCheckpointFence(testingTopology.topologySnapshot(), InputEvent(event)),
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
              stakeholders = Map(alice -> Seq(p1.toLf, p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(0),
              rc,
              isActivation = true,
            ),
            AcsUpdate(
              stakeholders = Map(bob -> Seq(p2.toLf, p3.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(0),
              rc,
              isActivation = true,
            ),
            AcsUpdate(
              stakeholders = Map(charlie -> Seq(p1.toLf, p3.toLf, p4.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(0),
              rc,
              isActivation = true,
            ),

            // cid1
            AcsUpdate(
              stakeholders = Map(alice -> Seq(p1.toLf, p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(1),
              rc,
              isActivation = true,
            ),
            AcsUpdate(
              stakeholders = Map(charlie -> Seq(p1.toLf, p3.toLf, p4.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(1),
              rc,
              isActivation = true,
            ),

            // cid2
            AcsUpdate(
              stakeholders = Map(alice -> Seq(p1.toLf, p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(2),
              rc,
              isActivation = true,
            ),
            AcsUpdate(
              stakeholders = Map(bob -> Seq(p2.toLf, p3.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(2),
              rc,
              isActivation = true,
            ),

            // cid3
            AcsUpdate(
              stakeholders = Map(alice -> Seq(p1.toLf, p2.toLf)),
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
                alice -> Seq(p1.toLf, p2.toLf),
                bob -> Seq(p2.toLf, p3.toLf),
                charlie -> Seq(p1.toLf, p3.toLf, p4.toLf),
              ),
              locallyHostedStakeholders = Seq(alice),
              cid(0),
              rc,
              isActivation = true,
            ),

            // cid1
            AcsUpdate(
              stakeholders =
                Map(alice -> Seq(p1.toLf, p2.toLf), charlie -> Seq(p1.toLf, p3.toLf, p4.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(1),
              rc,
              isActivation = true,
            ),

            // cid2
            AcsUpdate(
              stakeholders = Map(alice -> Seq(p1.toLf, p2.toLf), bob -> Seq(p2.toLf, p3.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(2),
              rc,
              isActivation = true,
            ),

            // cid3
            AcsUpdate(
              stakeholders = Map(alice -> Seq(p1.toLf, p2.toLf)),
              locallyHostedStakeholders = Seq(alice),
              cid(3),
              rc,
              isActivation = true,
            ),
          )
        }
      }
    }
  }
}
object RunningDigestProcessorTest {
  val rc = ReassignmentCounter.MinValue

  implicit def toAcsChangeData(
      parties: Set[LfPartyId]
  ): ContractStakeholdersAndReassignmentCounter =
    ContractStakeholdersAndReassignmentCounter(parties, rc)
  def cid(i: Int): LfContractId = ExampleTransactionFactory.suffixedId(i, i)
  def rt(i: Int): RecordTime = RecordTime(CantonTimestamp.Epoch.plusSeconds(i.toLong), 0)
  def party(s: String): LfPartyId = LfPartyId.assertFromString(s)

  def partyHosting(party: LfPartyId)(participants: ParticipantId*): (LfPartyId, PartyInfo) =
    (
      party,
      PartyInfo(PositiveInt.one, participants.map(_ -> ParticipantAttributes(Submission)).toMap),
    )

  def mkAcsLookup(
      contractsWithStakeholders: (RecordTime, LfContractId, Seq[LfPartyId])*
  ): AcsLookup = {
    val acs = for {
      (recordTime, cid, rawStakeholders) <- contractsWithStakeholders
      stakeholders = rawStakeholders.map(LfPartyId.assertFromString)
      activeContract = AcsActiveContract(cid, ReassignmentCounter.MinValue, stakeholders)
      stakeholder <- stakeholders
    } yield {
      stakeholder -> (recordTime, activeContract)
    }
    val partyToContracts = immutable.MultiDict.from(acs)

    new AcsLookup {
      override def activeContracts(parties: Set[LfPartyId], asOfInclusive: RecordTime)(implicit
          traceContext: TraceContext
      ): Source[AcsActiveContract, NotUsed] = {
        val result = parties
          .flatMap(party =>
            partyToContracts.get(party).collect {
              case (rt, contract) if rt <= asOfInclusive => contract
            }
          )
        Source(result)
      }
    }
  }

}
