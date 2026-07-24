// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.PromiseUnlessShutdown
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  AcsUpdate,
  CheckpointFence,
  CheckpointFenceOr,
  CheckpointWritten,
  Classification,
  NotCheckpointFence,
  PartyAddedToParticipant,
  PartyRemovedFromParticipant,
  ProcessingContext,
}
import com.digitalasset.canton.participant.commitment.InMemoryDigestAccumulator.{
  ParticipantDigestIdentifier,
  PartyDigestIdentifier,
}
import com.digitalasset.canton.participant.config.AcsDigestTracingMode
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.ReconciliationIntervalBoundary
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  Checkpoint,
  HashedDigest,
  LocalPartyFirst,
  ParticipantAcsDigestUpdate,
  PartyAcsDigestUpdate,
  PartyAndOrder,
  PartyOrder,
  RawDigest,
  RemotePartyFirst,
}
import com.digitalasset.canton.participant.store.memory.InMemoryAcsDigestStore
import com.digitalasset.canton.participant.store.{AcsDigestStore, BlockingAcsDigestStore}
import com.digitalasset.canton.platform.store.interning.{MockStringInterning, StringInterning}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.{
  BaseTestWordSpec,
  HasExecutionContext,
  LedgerParticipantId,
  LfPartyId,
  ReassignmentCounter,
}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.testkit.scaladsl.{TestSink, TestSource}
import org.apache.pekko.stream.testkit.{TestPublisher, TestSubscriber}
import org.apache.pekko.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.unused
import scala.collection.immutable.SortedMap
import scala.concurrent.{Future, Promise}

@AcsCommitmentTest
class InMemoryDigestAccumulatorTest
    extends TestKit(ActorSystem(classOf[InMemoryDigestAccumulatorTest].getSimpleName))
    with BaseTestWordSpec
    with BeforeAndAfterAll
    with HasExecutionContext {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  class Fixture(
      participant: LedgerParticipantId,
      acsUpdateBatchSize: Int = 1,
      digestLoadParallelism: Int = 1,
      digestStoreParallelism: Int = 1,
  ) {
    val stringInterning: StringInterning = new MockStringInterning()
    val digestStore: AcsDigestStore = InMemoryAcsDigestStore.create(
      Eval.now(stringInterning),
      loggerFactory,
    )
    val accumulator: InMemoryDigestAccumulator = new InMemoryDigestAccumulator(
      participant,
      digestStore,
      loggerFactory,
      Eval.now(stringInterning),
      acsUpdateBatchSize,
      digestLoadParallelism,
      digestStoreParallelism,
      tracingMode = AcsDigestTracingMode.Disabled,
      enableConsistencyChecks = true,
    )

    def lookupPartyDigest(
        party: LfPartyId,
        order: PartyOrder,
        offset: Offset = Offset.MaxValue,
    ): Option[AcsDigestStore.AcsDigestUpdate[PartyAndOrder[LfPartyId], RawDigest]] =
      Fixture.lookupPartyDigest(digestStore, stringInterning, party, order, offset)

    def lookupParticipantDigest(
        participant: LedgerParticipantId,
        offset: Offset = Offset.MaxValue,
    ): Option[AcsDigestStore.AcsDigestUpdate[LedgerParticipantId, (RawDigest, HashedDigest)]] =
      Fixture.lookupParticipantDigest(digestStore, stringInterning, participant, offset)
  }
  object Fixture {
    def lookupPartyDigest(
        digestStore: AcsDigestStore,
        stringInterning: StringInterning,
        party: LfPartyId,
        order: PartyOrder,
        offset: Offset = Offset.MaxValue,
    ): Option[AcsDigestStore.AcsDigestUpdate[PartyAndOrder[LfPartyId], RawDigest]] =
      digestStore.party
        .lookup(
          PartyAndOrder(stringInterning.party.internalize(party), order),
          offset,
        )
        .futureValueUS
        .map(PartyAcsDigestUpdate.externalize(stringInterning, _))

    def lookupParticipantDigest(
        digestStore: AcsDigestStore,
        stringInterning: StringInterning,
        participant: LedgerParticipantId,
        offset: Offset = Offset.MaxValue,
    ): Option[AcsDigestStore.AcsDigestUpdate[LedgerParticipantId, (RawDigest, HashedDigest)]] =
      digestStore.participant
        .lookup(
          stringInterning.participantId.internalize(participant),
          offset,
        )
        .futureValueUS
        .map(ParticipantAcsDigestUpdate.externalize(stringInterning, _))
  }

  private val p1 = DefaultTestIdentities.participant1.toLf
  private val p2 = DefaultTestIdentities.participant2.toLf
  private val p3 = DefaultTestIdentities.participant3.toLf
  private val p4 = DefaultTestIdentities.participant4.toLf
  private val p5 = DefaultTestIdentities.participant5.toLf
  private val alice = DefaultTestIdentities.party1.toLf
  private val bob = DefaultTestIdentities.party2.toLf
  private val carol = DefaultTestIdentities.party3.toLf
  private val dave = DefaultTestIdentities.party4.toLf
  private val evelyn = DefaultTestIdentities.party5.toLf
  private val rc0 = ReassignmentCounter.Genesis
  private val cid0 = cid(0)
  private val cid1 = cid(1)
  private val cid2 = cid(2)
  private val cid3 = cid(3)

  private def ts(offsetFromEpoch: Long): CantonTimestamp =
    CantonTimestamp.Epoch.plusSeconds(offsetFromEpoch)

  private def off(offset: Long): Offset = Offset.tryFromLong(offset)

  def tp(offset: Long): Timepoint = tp(offset, offset)

  def tp(offset: Long, offsetFromEpoch: Long): Timepoint =
    Timepoint(off(offset))(ts(offsetFromEpoch))

  private def cid(i: Int): LfContractId = ExampleTransactionFactory.suffixedId(i, i)

  private def from(timepoint: Timepoint)(
      classification: Classification
  ): ProcessingContext[CheckpointFenceOr[Classification]] =
    ProcessingContext(
      timepoint,
      NotCheckpointFence(mock[TopologySnapshot], classification),
    )

  private def checkpoint(timepoint: Timepoint): ProcessingContext[CheckpointFenceOr[Nothing]] =
    // checkpoint type was picked arbitrarily, as it doesn't change the behavior of InMemoryDigestAccumulator
    ProcessingContext(timepoint, CheckpointFence(ReconciliationIntervalBoundary))

  @unused
  private def testSource: Source[
    ProcessingContext[CheckpointFenceOr[Classification]],
    TestPublisher.Probe[ProcessingContext[CheckpointFenceOr[Classification]]],
  ] = TestSource.probe[ProcessingContext[CheckpointFenceOr[Classification]]]

  @unused
  private def testSink: Sink[CheckpointWritten, TestSubscriber.Probe[CheckpointWritten]] =
    TestSink.probe[CheckpointWritten]

  "InMemoryDigestAccumulator" should {
    "process a simple AcsUpdate" in {
      val fixture = new Fixture(p1)
      import fixture.*

      val result = Source(
        Seq(
          from(tp(1))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1)),
              locallyHostedStakeholders = Seq(alice),
              cid0,
              rc0,
              isActivation = true,
            )
          ),
          checkpoint(tp(1)),
        )
      ).via(accumulator.flow()).runWith(Sink.seq).futureValue

      result shouldBe Seq(CheckpointWritten(ts(1), off(1), ReconciliationIntervalBoundary))

      digestStore.latestCheckpointUpTo(Offset.MaxValue).futureValueUS.value shouldBe
        Checkpoint(tp(1), ReconciliationIntervalBoundary)

      val partyDigest = lookupPartyDigest(alice, RemotePartyFirst).value
      val participantDigest = lookupParticipantDigest(p1).value
      partyDigest.digestUpdate.digestO.value shouldBe participantDigest.digestUpdate.digestO.value._1

      // All in-memory state has been evicted
      accumulator.digestsUsageCounters shouldBe empty
    }

    // given cid0 with stakeholders alice and bob,
    // this test checks that the digests are the same in the following two scenarios:
    // 1. p1 hosts only alice, receives cid0, and then onboards bob
    // 2. p1 hosts alice and bob, and then receives cid0
    "process a party onboarding scenario" in {
      val fixtureBobOnboarded = new Fixture(p1)

      Source(
        Seq(
          from(tp(1))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(alice),
              cid0,
              rc0,
              isActivation = true,
            )
          ),
          // simulate onboarding of bob
          from(tp(2))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(bob),
              cid0,
              rc0,
              isActivation = true,
            )
          ),
          from(tp(2))(PartyAddedToParticipant(bob, p1)),
          checkpoint(tp(3)),
        )
      ).via(fixtureBobOnboarded.accumulator.flow()).runWith(Sink.seq).futureValue shouldBe
        Seq(CheckpointWritten(ts(3), off(3), ReconciliationIntervalBoundary))

      val fixtureBobAlreadyHosted = new Fixture(p1)
      Source(
        Seq(
          from(tp(1))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set(p1)),
              locallyHostedStakeholders = Seq(alice, bob),
              cid0,
              rc0,
              isActivation = true,
            )
          ),
          checkpoint(tp(3)),
        )
      ).via(fixtureBobAlreadyHosted.accumulator.flow()).runWith(Sink.seq).futureValue shouldBe
        Seq(CheckpointWritten(ts(3), off(3), ReconciliationIntervalBoundary))

      // All in-memory state has been evicted
      fixtureBobOnboarded.accumulator.digestsUsageCounters shouldBe empty
      fixtureBobAlreadyHosted.accumulator.digestsUsageCounters shouldBe empty

      fixtureBobAlreadyHosted.lookupParticipantDigest(p1).value.digestUpdate.digestO shouldBe
        fixtureBobOnboarded.lookupParticipantDigest(p1).value.digestUpdate.digestO

      for {
        party <- Seq(alice, bob)
        order <- Seq(LocalPartyFirst, RemotePartyFirst)
      } yield {
        val digestAlreadyHosted =
          fixtureBobAlreadyHosted.lookupPartyDigest(party, order).value.digestUpdate.digestO
        val digestOnboarded =
          fixtureBobOnboarded.lookupPartyDigest(party, order).value.digestUpdate.digestO
        digestAlreadyHosted shouldBe digestOnboarded
      }
    }

    // given cid0 has stakeholders alice and bob,
    // this test checks that the digests are the same in the following two scenarios:
    // 1. p1 hosts alice and bob, receives cid0, and then offboards bob
    // 2. p1 hosts only alice and receives cid0
    "process a party offboarding scenario" in {
      val fixtureBobOffboarded = new Fixture(p1)

      Source(
        Seq(
          from(tp(1))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set(p1)),
              locallyHostedStakeholders = Seq(alice, bob),
              cid0,
              rc0,
              isActivation = true,
            )
          ),
          // simulate offboarding of bob
          from(tp(2))(PartyRemovedFromParticipant(bob, p1)),
          from(tp(2))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(bob),
              cid0,
              rc0,
              isActivation = false,
            )
          ),
          checkpoint(tp(3)),
        )
      ).via(fixtureBobOffboarded.accumulator.flow()).runWith(Sink.seq).futureValue shouldBe
        Seq(CheckpointWritten(ts(3), off(3), ReconciliationIntervalBoundary))

      val fixtureBobNotHosted = new Fixture(p1)
      Source(
        Seq(
          from(tp(2))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(alice),
              cid0,
              rc0,
              isActivation = true,
            )
          ),
          checkpoint(tp(3)),
        )
      ).via(fixtureBobNotHosted.accumulator.flow()).runWith(Sink.seq).futureValue shouldBe
        Seq(CheckpointWritten(ts(3), off(3), ReconciliationIntervalBoundary))

      // All in-memory state has been evicted
      fixtureBobNotHosted.accumulator.digestsUsageCounters shouldBe empty
      fixtureBobOffboarded.accumulator.digestsUsageCounters shouldBe empty

      fixtureBobNotHosted.lookupParticipantDigest(p1).value.digestUpdate.digestO shouldBe
        fixtureBobOffboarded.lookupParticipantDigest(p1).value.digestUpdate.digestO

      for {
        party <- Seq(alice, bob)
        order <- Seq(LocalPartyFirst, RemotePartyFirst)
      } yield {
        val digestNotHosted =
          fixtureBobNotHosted.lookupPartyDigest(party, order).value.digestUpdate.digestO
        val digestOffboarded =
          fixtureBobOffboarded.lookupPartyDigest(party, order).value.digestUpdate.digestO
        digestNotHosted shouldBe digestOffboarded
      }
    }

    "not store empty initial digests" in {
      val fixture = new Fixture(p1)
      import fixture.*

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty

      Source(
        Seq(
          from(tp(2, 1))(PartyAddedToParticipant(alice, p2)),
          from(tp(3, 2))(PartyRemovedFromParticipant(alice, p2)),
          checkpoint(tp(4)),
        )
      ).via(accumulator.flow()).runWith(Sink.seq).futureValue shouldBe
        Seq(CheckpointWritten(ts(4), off(4), ReconciliationIntervalBoundary))

      // All in-memory state has been evicted
      accumulator.digestsUsageCounters shouldBe empty

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty
    }

    "store empty digests after a non-empty initial digest" in {
      val fixture = new Fixture(p1)
      import fixture.*

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty

      val activation = AcsUpdate(
        stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
        locallyHostedStakeholders = Seq(alice),
        cid0,
        rc0,
        isActivation = true,
      )
      val deactivation = activation.copy(isActivation = false)

      Source(
        Seq(
          from(tp(1))(activation),
          // Don't write a checkpoint in between so that these updates could in theory be conflated
          from(tp(2))(deactivation),
          checkpoint(tp(3)),
        )
      ).via(accumulator.flow()).runWith(Sink.seq).futureValue shouldBe
        Seq(CheckpointWritten(ts(3), off(3), ReconciliationIntervalBoundary))

      // All in-memory state has been evicted
      accumulator.digestsUsageCounters shouldBe empty

      // check digests for participants
      Seq(p1, p2).foreach { participant =>
        val digest = lookupParticipantDigest(participant).value
        digest.digestUpdate.offset shouldBe off(2)
        digest.replacesOffset shouldBe Some(off(1))
        digest.digestUpdate.digestO shouldBe empty
      }

      // check digests for parties
      for {
        party <- Seq(alice, bob)
        order <- Seq(RemotePartyFirst, LocalPartyFirst)
      } yield {
        val digest = lookupPartyDigest(party, order).value
        digest.digestUpdate.offset shouldBe off(2)
        digest.replacesOffset shouldBe Some(off(1))
        digest.digestUpdate.digestO shouldBe empty
      }
    }

    "not create cycles in the replacement chain" in {
      val fixture = new Fixture(p1)
      import fixture.*

      val (source, sink) = testSource.via(accumulator.flow()).toMat(testSink)(Keep.both).run()

      sink.request(1)
      source
        .sendNext(
          from(tp(1))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set(p1)),
              locallyHostedStakeholders = Seq(alice, bob),
              cid0,
              rc0,
              isActivation = true,
            )
          )
        )
        .sendNext(checkpoint(tp(2)))
      sink.expectNext() shouldBe CheckpointWritten(ts(2), off(2), ReconciliationIntervalBoundary)

      // the first entry doesn't replace anything
      for {
        party <- Seq(alice, bob)
        order <- Seq(RemotePartyFirst, LocalPartyFirst)
      } {
        lookupPartyDigest(party, order).value.replacesOffset shouldBe empty
      }
      lookupParticipantDigest(p1).value.replacesOffset shouldBe empty

      // now process two acs updates of cid1 and cid2 at the same offset
      sink.request(1)
      source
        .sendNext(
          from(tp(3))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set(p1)),
              locallyHostedStakeholders = Seq(alice, bob),
              cid1,
              rc0,
              isActivation = true,
            )
          )
        )
        // Force an intermediate checkpoint to be written so that we persist an intermediate result.
        // Such checkpoints should not happen in practice because they would corrupt crash recovery.
        .sendNext(checkpoint(tp(3)))
      sink.expectNext() shouldBe CheckpointWritten(ts(3), off(3), ReconciliationIntervalBoundary)
      sink.request(1)

      // processing an update on a later offset should properly set replacesOffset
      for {
        party <- Seq(alice, bob)
        order <- Seq(RemotePartyFirst, LocalPartyFirst)
      } {
        lookupPartyDigest(party, order).value.replacesOffset.value shouldBe off(1)
      }
      lookupParticipantDigest(p1).value.replacesOffset.value shouldBe off(1)

      source
        .sendNext(
          from(tp(3))(
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(bob),
              cid2,
              rc0,
              isActivation = false,
            )
          )
        )
        .sendNext(checkpoint(tp(4)))
      sink.expectNext() shouldBe CheckpointWritten(ts(4), off(4), ReconciliationIntervalBoundary)

      // since the first persisted update at off(2) was just an intermediate result that got persisted,
      // another update for the same offset should retain the original replacesOffset
      for {
        party <- Seq(alice, bob)
        order <- Seq(RemotePartyFirst, LocalPartyFirst)
      } {
        lookupPartyDigest(party, order).value.replacesOffset.value shouldBe off(1)
      }
      lookupParticipantDigest(p1).value.replacesOffset.value shouldBe off(1)

      source.sendComplete()
      sink.expectComplete()
    }

    // send a checkpoint and 19 ACS updates and then another checkpoint through the flow while the store is blocked.
    // this should conflate all 19 ACS updates into one store update
    "conflate multiple updates when the writing to the store is slow" in {
      val fixture = new Fixture(p1, acsUpdateBatchSize = 10)
      import fixture.*

      val storeBlockPromise = Promise[Unit]()
      val storeBlocker: ProcessingContext[CheckpointFenceOr[Classification]] => Future[Unit] =
        _ => storeBlockPromise.future

      val loadedCounter = new AtomicInteger()
      val loadedBlocker: ProcessingContext[CheckpointFenceOr[Classification]] => Future[Unit] = {
        _ =>
          loadedCounter.incrementAndGet().discard
          Future.unit
      }

      val flow = accumulator.flowInternal(
        sequentialBlocker = Some(loadedBlocker),
        storeBlocker = Some(storeBlocker),
      )
      val (source, sink) = testSource.via(flow).toMat(testSink)(Keep.both).run()

      sink.request(2)
      // Send a checkpoint first because the first element is never going to be conflated when
      // there is demand.
      source.sendNext(checkpoint(tp(1)))
      (2 to 20).foreach { i =>
        val stakeholders = Map(alice -> Set(p1)) ++
          (if (i % 2 == 0) Map(bob -> Set(p2)) else Map.empty) ++
          (if (i % 3 == 0) Map(carol -> Set(p3)) else Map.empty)
        source.sendNext(
          from(tp(i.toLong))(
            AcsUpdate(
              stakeholders,
              locallyHostedStakeholders = Seq(alice),
              cid(i),
              rc0,
              isActivation = true,
            )
          )
        )
      }
      source.sendNext(checkpoint(tp(20)))

      eventually() {
        loadedCounter.get should be >= 20
      }

      // Check that the usage counters are as expected
      val expectedDigestUsage =
        Seq(p1 -> 19, p2 -> 10, p3 -> 6).map { case (participant, count) =>
          ParticipantDigestIdentifier(
            stringInterning.participantId.internalize(participant)
          ) -> count
        } ++ Seq(LocalPartyFirst, RemotePartyFirst).flatMap(order =>
          Seq(alice -> 19, bob -> 10, carol -> 6)
            .map { case (party, count) =>
              PartyDigestIdentifier(stringInterning.party.internalize(party), order) -> count
            }
        )
      SortedMap.from(accumulator.digestsUsageCounters) shouldBe
        SortedMap.from(expectedDigestUsage)

      storeBlockPromise.success(())
      sink.expectNext() shouldBe CheckpointWritten(ts(1), off(1), ReconciliationIntervalBoundary)
      sink.expectNext() shouldBe CheckpointWritten(ts(20), off(20), ReconciliationIntervalBoundary)

      // All in-memory state has been evicted
      // accumulator.digestsUsageCounters shouldBe empty

      val p1DU = lookupParticipantDigest(p1, off(20)).value
      p1DU.digestUpdate.offset shouldBe off(20)
      p1DU.replacesOffset shouldBe None

      forAll(Seq(alice, bob)) { party =>
        forAll(Seq(LocalPartyFirst, RemotePartyFirst)) { order =>
          val partyDU = lookupPartyDigest(party, order, off(20)).value
          partyDU.digestUpdate.offset shouldBe off(20)
          partyDU.replacesOffset shouldBe None
        }
      }

      source.sendComplete()
      sink.expectComplete()
    }

    // Send more updates than fit into the acsUpdateBatch size and expect that backpressure kicks in.
    "conflation backpressures" in {
      val fixture = new Fixture(
        p1,
        acsUpdateBatchSize = 9,
        // Make room for elements to end up in the `async` buffers so that we can measure backpressure.
        digestLoadParallelism = 8,
      )
      import fixture.*

      val storeBlockPromise = Promise[Unit]()
      val storeBlocker: ProcessingContext[CheckpointFenceOr[Classification]] => Future[Unit] =
        _ => storeBlockPromise.future

      val loadedCounter = new AtomicInteger()
      val loadedBlocker: ProcessingContext[CheckpointFenceOr[Classification]] => Future[Unit] = {
        _ =>
          loadedCounter.incrementAndGet().discard
          Future.unit
      }

      val flow = accumulator.flowInternal(
        sequentialBlocker = Some(loadedBlocker),
        storeBlocker = Some(storeBlocker),
      )
      val (source, sink) = testSource.via(flow).toMat(testSink)(Keep.both).run()

      sink.request(2)
      // Send a checkpoint first because the first element is never going to be conflated when
      // there is demand.
      source.sendNext(checkpoint(tp(1)))
      eventually() {
        loadedCounter.get should be >= 1
      }
      source.sendNext(
        from(tp(2))(
          // 6 digest identifiers (2 parties * 2 orders + 2 participants)
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
            locallyHostedStakeholders = Seq(alice),
            cid0,
            rc0,
            isActivation = true,
          )
        )
      )
      source.sendNext(
        from(tp(3))(
          // 3 more digest identifiers (1 party * 2 orders + 1 participant)
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1), carol -> Set(p3)),
            locallyHostedStakeholders = Seq(alice),
            cid1,
            rc0,
            isActivation = true,
          )
        )
      )
      source.sendNext(
        from(tp(4))(
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1), carol -> Set(p3), dave -> Set(p4)),
            locallyHostedStakeholders = Seq(alice),
            cid2,
            rc0,
            isActivation = true,
          )
        )
      )
      source.sendNext(checkpoint(tp(5)))

      always() {
        loadedCounter.get should be <= 4
      }
      eventually() {
        loadedCounter.get shouldBe 4
      }

      // Check that the usage counters are as expected
      val expectedUsages =
        Seq(p1 -> 3, p2 -> 1, p3 -> 2, p4 -> 1).map { case (participant, count) =>
          ParticipantDigestIdentifier(
            stringInterning.participantId.internalize(participant)
          ) -> count
        } ++ Seq(LocalPartyFirst, RemotePartyFirst).flatMap(order =>
          Seq(alice -> 3, bob -> 1, carol -> 2, dave -> 1).map { case (party, count) =>
            PartyDigestIdentifier(stringInterning.party.internalize(party), order) -> count
          }
        )
      SortedMap.from(accumulator.digestsUsageCounters) shouldBe
        SortedMap.from(expectedUsages)

      storeBlockPromise.success(())
      sink.expectNext() shouldBe CheckpointWritten(ts(1), off(1), ReconciliationIntervalBoundary)
      sink.expectNext() shouldBe CheckpointWritten(ts(5), off(5), ReconciliationIntervalBoundary)

      // All in-memory state has been evicted
      accumulator.digestsUsageCounters shouldBe empty

      val partyDigestUpdates = Table(
        ("party", "queried offset", "valid since and replaces"),
        // No entry for alice at 2 because this was conflated with 3
        (alice, off(2), None),
        (alice, off(3), Some(off(3) -> None)),
        (alice, off(5), Some(off(4) -> Some(off(3)))),
        // Bob has an entry at 2 because it was not affected by 3
        (bob, off(2), Some(off(2) -> None)),
        (bob, off(5), Some(off(2) -> None)),
        (carol, off(3), Some(off(3) -> None)),
        (carol, off(5), Some(off(4) -> Some(off(3)))),
        (dave, off(3), None),
        (dave, off(5), Some(off(4) -> None)),
      )

      forAll(partyDigestUpdates) { (party, offset, expected) =>
        forAll(Seq(LocalPartyFirst, RemotePartyFirst)) { order =>
          val partyDUO = lookupPartyDigest(party, order, offset)
          expected match {
            case None => partyDUO shouldBe empty
            case Some((expectedOffset, replaces)) =>
              val partyDU = partyDUO.value
              partyDU.digestUpdate.offset shouldBe expectedOffset
              partyDU.replacesOffset shouldBe replaces
          }
        }
      }

      val participantDigestUpdates = Table(
        ("participant", "queried offset", "valid since and replaces"),
        (p1, off(2), None),
        (p1, off(3), Some(off(3) -> None)),
        (p2, off(2), Some(off(2) -> None)),
        (p2, off(5), Some(off(2) -> None)),
        (p3, off(3), Some(off(3) -> None)),
        (p3, off(5), Some(off(4) -> Some(off(3)))),
        (p4, off(3), None),
        (p4, off(5), Some(off(4) -> None)),
      )
      forAll(participantDigestUpdates) { (participant, offset, expected) =>
        val participantDUO = lookupParticipantDigest(participant, offset)
        expected match {
          case None => participantDUO shouldBe empty
          case Some((expectedOffset, replaces)) =>
            val participantDU = participantDUO.value
            participantDU.digestUpdate.offset shouldBe expectedOffset
            participantDU.replacesOffset shouldBe replaces
        }
      }

      source.sendComplete()
      sink.expectComplete()
    }

    "slow digest loading backpressures" in {
      val stringInterning = new MockStringInterning()
      val digestStore = new BlockingAcsDigestStore(stringInterning, loggerFactory)
      val accumulator = new InMemoryDigestAccumulator(
        p1,
        digestStore,
        loggerFactory,
        Eval.now(stringInterning),
        acsUpdateBatchSize = 1,
        digestLoadParallelism = 2,
        digestStoreParallelism = 1,
        tracingMode = AcsDigestTracingMode.Disabled,
        enableConsistencyChecks = true,
      )

      val releaseP = PromiseUnlessShutdown.unsupervised[Unit]()
      digestStore.setBlocking(releaseP.futureUS)

      val loadingCounter = new AtomicInteger()
      val loadingBlocker: ProcessingContext[CheckpointFenceOr[Classification]] => Future[Unit] = {
        _ =>
          loadingCounter.incrementAndGet().discard
          Future.unit
      }

      val flow = accumulator.flowInternal(computeDigestBlocker = Some(loadingBlocker))
      val (source, sink) = testSource.via(flow).toMat(testSink)(Keep.both).run()

      sink.request(1)

      // This request goes through and gets stuck when the loading future is being awaited on.
      // This simulates backpressure from downstream.
      source.sendNext(
        from(tp(1))(
          // 3 digest identifiers (1 party * 2 orders + 2 participant)
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1)),
            locallyHostedStakeholders = Seq(alice),
            cid0,
            rc0,
            isActivation = true,
          )
        )
      )
      // This request ends up in the buffer.
      source.sendNext(
        from(tp(2))(
          // 6 digest identifiers (2 party * 2 orders + 2 participant)
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
            locallyHostedStakeholders = Seq(alice),
            cid1,
            rc0,
            isActivation = true,
          )
        )
      )
      // This request also ends up in the buffer
      source.sendNext(
        from(tp(3))(
          // 3 more digest identifiers (1 party * 2 orders + 1 participant)
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1), bob -> Set(p2), carol -> Set(p3)),
            locallyHostedStakeholders = Seq(alice),
            cid2,
            rc0,
            isActivation = true,
          )
        )
      )
      // This request should not go into the stream, but it ends up in the buffer of the `loadingBlocker`.
      source.sendNext(
        from(tp(4))(
          // 3 more digest identifiers (1 party * 2 orders + 1 participant)
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1), bob -> Set(p2), carol -> Set(p3), dave -> Set(p4)),
            locallyHostedStakeholders = Seq(alice),
            cid3,
            rc0,
            isActivation = true,
          )
        )
      )
      // This request should not go into the stream.
      // We can still send it to the source because the flow has a non-trivial input buffer of its own.
      source.sendNext(
        from(tp(5))(
          // 3 more digest identifiers (1 party * 2 orders + 1 participant)
          AcsUpdate(
            stakeholders = Map(
              alice -> Set(p1),
              bob -> Set(p2),
              carol -> Set(p3),
              dave -> Set(p4),
              evelyn -> Set(p5),
            ),
            locallyHostedStakeholders = Seq(alice),
            cid3,
            rc0,
            isActivation = true,
          )
        )
      )
      source.sendNext(checkpoint(tp(6)))
      always() {
        loadingCounter.get should be <= 4
      }
      eventually() {
        loadingCounter.get shouldBe 4
      }

      // Check that the usage counters are as expected
      val expectedDigestUsage =
        Seq(p1 -> 4, p2 -> 3, p3 -> 2, p4 -> 1).map { case (participant, count) =>
          ParticipantDigestIdentifier(
            stringInterning.participantId.internalize(participant)
          ) -> count
        } ++ Seq(LocalPartyFirst, RemotePartyFirst).flatMap(order =>
          Seq(alice -> 4, bob -> 3, carol -> 2, dave -> 1)
            .map { case (party, count) =>
              PartyDigestIdentifier(stringInterning.party.internalize(party), order) -> count
            }
        )
      SortedMap.from(accumulator.digestsUsageCounters) shouldBe
        SortedMap.from(expectedDigestUsage)

      releaseP.outcome_(())
      sink.expectNext() shouldBe CheckpointWritten(ts(6), off(6), ReconciliationIntervalBoundary)

      source.sendComplete()
      sink.expectComplete()
    }
  }
}
