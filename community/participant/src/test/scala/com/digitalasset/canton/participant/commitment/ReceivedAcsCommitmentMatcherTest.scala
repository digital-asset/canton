// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.{
  CommitmentMatchPeriod,
  MatchedCommitmentMatchPeriod,
  MismatchedCommitmentMatchPeriod,
  MismatchedOrUnexpectedCommitmentMatchPeriod,
  OutstandingCommitmentMatchPeriod,
}
import com.digitalasset.canton.participant.store.AcsDigestStore.InternedParticipantId
import com.digitalasset.canton.participant.store.memory.InMemoryAcsCommitmentPeriodStore
import com.digitalasset.canton.participant.store.{
  AcsCommitmentPeriodStore,
  DelegatingAcsCommitmentPeriodStore,
}
import com.digitalasset.canton.platform.store.interning.{MockStringInterning, StringInterning}
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  AcsCommitmentProtocolMessage,
  CommitmentPeriod,
  Digest,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTestWordSpec,
  HasExecutionContext,
  LedgerParticipantId,
  ProtocolVersionChecksAnyWordSpec,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.testkit.TestKit
import org.scalatest.BeforeAndAfterAll

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable
import scala.language.implicitConversions

@AcsCommitmentTest
class ReceivedAcsCommitmentMatcherTest
    extends TestKit(ActorSystem(classOf[ReceivedAcsCommitmentMatcherTest].getSimpleName))
    with BaseTestWordSpec
    with ProtocolVersionChecksAnyWordSpec
    with BeforeAndAfterAll
    with HasExecutionContext {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private def synchronizerId: SynchronizerId = DefaultTestIdentities.synchronizerId

  private val p1: LedgerParticipantId = DefaultTestIdentities.participant1.toLf
  private val p2: LedgerParticipantId = DefaultTestIdentities.participant2.toLf
  private val p3: LedgerParticipantId = DefaultTestIdentities.participant3.toLf
  private val p4: LedgerParticipantId = DefaultTestIdentities.participant4.toLf

  class Fixture(
      participant: LedgerParticipantId
  ) {
    val stringInterning: StringInterning = new MockStringInterning()
    val store: AcsCommitmentPeriodStore = new InMemoryAcsCommitmentPeriodStore(
      Eval.now(stringInterning),
      loggerFactory,
      enableConsistencyChecks = true,
    )
    val matcher: ReceivedAcsCommitmentMatcher =
      new ReceivedAcsCommitmentMatcher(
        store,
        Eval.now(stringInterning),
        loggerFactory,
        PositiveInt.tryCreate(1000),
      )

    def intern(participantId: LedgerParticipantId): InternedParticipantId =
      stringInterning.participantId.internalize(participantId)

    def mkAcsUpdateContainer(
        sender: LedgerParticipantId,
        fromExclusive: CantonTimestamp,
        toInclusive: CantonTimestamp,
        digest: String,
        offset: Offset,
        recordTime: CantonTimestamp,
    )(implicit traceContext: TraceContext): InternalIndexService.AcsUpdateContainer =
      Fixture.mkAcsUpdateContainer(
        sender,
        participant,
        fromExclusive,
        toInclusive,
        digest,
        offset,
        recordTime,
      )
  }

  object Fixture {
    def mkAcsUpdateContainer(
        sender: LedgerParticipantId,
        participant: LedgerParticipantId,
        fromExclusive: CantonTimestamp,
        toInclusive: CantonTimestamp,
        digest: String,
        offset: Offset,
        recordTime: CantonTimestamp,
    )(implicit traceContext: TraceContext): InternalIndexService.AcsUpdateContainer = {
      val commitment = AcsCommitment.create(
        synchronizerId.toPhysical,
        sender,
        participant,
        CommitmentPeriod.tryCreate(fromExclusive, toInclusive),
        Digest.hashDigest(ByteString.copyFromUtf8(digest)).getCryptographicEvidence,
        testedProtocolVersion,
      )
      val message = AcsCommitmentProtocolMessage(commitment, Signature.noSignature)
      val receivedAcsCommitments = ReceivedAcsCommitments(NonEmpty(Seq, message))
      val payload = receivedAcsCommitments.toByteString(testedProtocolVersion)
      InternalIndexService.AcsUpdateContainer(
        InternalIndexService.AcsUpdate.AcsCommitment(payload),
        recordTime,
        offset,
        traceContext,
      )
    }
  }

  private def ts(offsetFromEpoch: Long): CantonTimestamp =
    CantonTimestamp.Epoch.plusSeconds(offsetFromEpoch)

  private def off(offset: Long): Offset = Offset.tryFromLong(offset)

  private def cp(from: CantonTimestamp, to: CantonTimestamp): CommitmentPeriod =
    CommitmentPeriod.tryCreate(from, to)

  "ReceivedAcsCommitmentMatcher" should {
    "process a received ACS commitment" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val fixture = new Fixture(p1)
      import fixture.*

      val outstanding = Seq(
        CommitmentMatchPeriod.outstanding(intern(p2), ts(1), ts(10), "P2:1-10"),
        CommitmentMatchPeriod.outstanding(intern(p2), ts(12), ts(20), "P2:12-20"),
        CommitmentMatchPeriod.outstanding(intern(p2), ts(20), ts(30), "P2:20-30"),
      )
      store.markOutstanding(outstanding).futureValueUS
      store.increaseInsertionWatermark(ts(30), affirmationOnly = false).futureValueUS

      val updateContainer = mkAcsUpdateContainer(p2, ts(2), ts(25), "P2:12-20", off(17), ts(42))

      Source(Seq(updateContainer)).via(matcher.flow).runWith(Sink.ignore).futureValue

      store
        .lookupOutstanding(Seq(intern(p2) -> cp(ts(1), ts(30))))
        .futureValueUS should contain theSameElementsAs
        Seq(
          CommitmentMatchPeriod.outstanding(intern(p2), ts(1), ts(2), "P2:1-10"),
          CommitmentMatchPeriod.outstanding(intern(p2), ts(25), ts(30), "P2:20-30"),
        )
      store
        .lookupMismatchedOrUnexpected(Seq(intern(p2) -> cp(ts(1), ts(30))))
        .futureValueUS should contain theSameElementsAs
        Seq(
          CommitmentMatchPeriod.mismatched(intern(p2), ts(2), ts(10), "P2:1-10", off(17)),
          CommitmentMatchPeriod.mismatched(intern(p2), ts(20), ts(25), "P2:20-30", off(17)),
        )
      store
        .lookupMatched(Seq(intern(p2) -> cp(ts(1), ts(30))))
        .futureValueUS should contain theSameElementsAs
        Seq(CommitmentMatchPeriod.matched(intern(p2), ts(12), ts(20), off(17)))
      store.watermarks().futureValueUS.matching shouldBe Some(off(17))
    }

    "process matching after mismatching commitments" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val fixture = new Fixture(p1)
      import fixture.*

      val outstanding = Seq(CommitmentMatchPeriod.outstanding(intern(p2), ts(1), ts(10), "P2:1-10"))
      store.markOutstanding(outstanding).futureValueUS
      store.increaseInsertionWatermark(ts(10), affirmationOnly = false).futureValueUS

      val mismatch = mkAcsUpdateContainer(p2, ts(1), ts(8), "P2:1-8", off(13), ts(11))
      val `match` = mkAcsUpdateContainer(p2, ts(2), ts(8), "P2:1-10", off(14), ts(12))
      Source(Seq(mismatch, `match`)).via(matcher.flow).runWith(Sink.ignore).futureValue
      store
        .lookupMismatchedOrUnexpected(Seq(intern(p2) -> cp(ts(1), ts(10))))
        .futureValueUS should contain theSameElementsAs
        Seq(CommitmentMatchPeriod.mismatched(intern(p2), ts(1), ts(2), "P2:1-10", off(13)))
      store
        .lookupMatched(Seq(intern(p2) -> cp(ts(1), ts(10))))
        .futureValueUS should contain theSameElementsAs
        Seq(CommitmentMatchPeriod.matched(intern(p2), ts(2), ts(8), off(14)))
      store.watermarks().futureValueUS.matching shouldBe Some(off(14))
    }

    "ignore parse errors" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val fixture = new Fixture(p1)
      import fixture.*

      val outstanding = Seq(CommitmentMatchPeriod.outstanding(intern(p2), ts(2), ts(3), "P2:2-3"))
      store.markOutstanding(outstanding).futureValueUS
      store.increaseInsertionWatermark(ts(30), affirmationOnly = false).futureValueUS

      val badPayload = ByteString.copyFromUtf8("this is garbage")
      val badUpdateContainer = InternalIndexService.AcsUpdateContainer(
        InternalIndexService.AcsUpdate.AcsCommitment(badPayload),
        ts(10),
        off(12),
        traceContext,
      )
      val goodUpdateContainer = mkAcsUpdateContainer(p2, ts(2), ts(3), "P2:2-3", off(13), ts(11))
      loggerFactory.assertLogs(
        Source(Seq(badUpdateContainer, goodUpdateContainer))
          .via(matcher.flow)
          .runWith(Sink.ignore)
          .futureValue,
        _.warningMessage should include(
          s"Failed to parse received ACS commitment at offset ${off(12)}. Discarding the update."
        ),
      )
      store
        .lookupMatched(Seq(intern(p2) -> cp(ts(2), ts(3))))
        .futureValueUS should contain theSameElementsAs
        Seq(CommitmentMatchPeriod.matched(intern(p2), ts(2), ts(3), off(13)))
      store.watermarks().futureValueUS.matching shouldBe Some(off(13))
    }

    // TODO(#34324) Change this so that they are not ignored
    "ignore unexpected commitments" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val fixture = new Fixture(p1)
      import fixture.*

      store.increaseInsertionWatermark(ts(10), affirmationOnly = false).futureValueUS

      val unexpected = mkAcsUpdateContainer(p2, ts(2), ts(3), "P2:2-3", off(1), ts(4))
      Source(Seq(unexpected)).via(matcher.flow).runWith(Sink.ignore).futureValue
      store.watermarks().futureValueUS.matching shouldBe Some(off(1))
    }

    "handle multiple envelopes in the same container" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val fixture = new Fixture(p1)
      import fixture.*

      val outstanding = Seq(
        CommitmentMatchPeriod.outstanding(intern(p2), ts(1), ts(10), "P2:1-10"),
        CommitmentMatchPeriod.outstanding(intern(p3), ts(10), ts(20), "P3:10-20"),
      )
      store.markOutstanding(outstanding).futureValueUS
      store.increaseInsertionWatermark(ts(20), affirmationOnly = false).futureValueUS

      val mismatchP2 = AcsCommitment.create(
        synchronizerId.toPhysical,
        p2,
        p1,
        cp(ts(4), ts(8)),
        Digest.hashDigest(ByteString.copyFromUtf8("mismatch")).getCryptographicEvidence,
        testedProtocolVersion,
      )
      val commitmentsP2 =
        mismatchP2 +-: NonEmpty(Seq, cp(ts(1), ts(5)), cp(ts(5), ts(10))).map(cp =>
          AcsCommitment.create(
            synchronizerId.toPhysical,
            p2,
            p1,
            cp,
            Digest.hashDigest(ByteString.copyFromUtf8("P2:1-10")).getCryptographicEvidence,
            testedProtocolVersion,
          )
        )
      val commitmentsP3 = NonEmpty(Seq, cp(ts(10), ts(15)), cp(ts(15), ts(20))).map(cp =>
        AcsCommitment.create(
          synchronizerId.toPhysical,
          p3,
          p1,
          cp,
          Digest.hashDigest(ByteString.copyFromUtf8("P3:10-20")).getCryptographicEvidence,
          testedProtocolVersion,
        )
      )
      val messages = (commitmentsP2 ++ commitmentsP3).map(cmt =>
        AcsCommitmentProtocolMessage(cmt, Signature.noSignature)
      )
      val receivedAcsCommitments = ReceivedAcsCommitments(messages)
      val payload = receivedAcsCommitments.toByteString(testedProtocolVersion)
      val container = InternalIndexService.AcsUpdateContainer(
        InternalIndexService.AcsUpdate.AcsCommitment(payload),
        ts(20),
        off(23),
        traceContext,
      )
      Source(Seq(container)).via(matcher.flow).runWith(Sink.ignore).futureValue

      store.watermarks().futureValueUS.matching shouldBe Some(off(23))
      store
        .lookupMatched(Seq(intern(p2) -> cp(ts(1), ts(10)), intern(p3) -> cp(ts(10), ts(20))))
        .futureValueUS should contain theSameElementsAs
        Seq(
          CommitmentMatchPeriod.matched(intern(p2), ts(1), ts(4), off(23)),
          CommitmentMatchPeriod.matched(intern(p2), ts(4), ts(5), off(23)),
          CommitmentMatchPeriod.matched(intern(p2), ts(5), ts(8), off(23)),
          CommitmentMatchPeriod.matched(intern(p2), ts(8), ts(10), off(23)),
          CommitmentMatchPeriod.matched(intern(p3), ts(10), ts(15), off(23)),
          CommitmentMatchPeriod.matched(intern(p3), ts(15), ts(20), off(23)),
        )
    }

    "tolerate overlapping and duplicate commitments" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val fixture = new Fixture(p1)
      import fixture.*

      val outstanding = Seq(CommitmentMatchPeriod.outstanding(intern(p2), ts(1), ts(20), "P2:1-20"))
      store.markOutstanding(outstanding).futureValueUS
      store.increaseInsertionWatermark(ts(20), affirmationOnly = false).futureValueUS

      val updates = Seq(
        mkAcsUpdateContainer(p2, ts(1), ts(10), "P2:1-20", off(10), ts(11)),
        mkAcsUpdateContainer(p2, ts(1), ts(10), "P2:1-20", off(11), ts(12)),
        mkAcsUpdateContainer(p2, ts(1), ts(20), "P2:1-20", off(12), ts(13)),
        mkAcsUpdateContainer(p2, ts(1), ts(20), "P2:1-20", off(13), ts(14)),
      )
      Source(updates).via(matcher.flow).runWith(Sink.ignore).futureValue

      store.lookupOutstanding(Seq(intern(p2) -> cp(ts(1), ts(20)))).futureValueUS shouldBe empty
      store
        .lookupMatched(Seq(intern(p2) -> cp(ts(1), ts(20))))
        .futureValueUS should contain theSameElementsAs
        Seq(
          CommitmentMatchPeriod.matched(intern(p2), ts(1), ts(10), off(10)),
          CommitmentMatchPeriod.matched(intern(p2), ts(10), ts(20), off(12)),
        )
      store.watermarks().futureValueUS.matching shouldBe Some(off(13))
    }

    "correctly process many queued commitments" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val fixture = new Fixture(p1)
      import fixture.*

      val count = 1000L

      val outstanding =
        Seq(CommitmentMatchPeriod.outstanding(intern(p2), ts(0), ts(count), "P2:all"))
      store.markOutstanding(outstanding).futureValueUS
      store.increaseInsertionWatermark(ts(count), affirmationOnly = false).futureValueUS

      val updates =
        (1L to count).map(i => mkAcsUpdateContainer(p2, ts(0), ts(i), "P2:all", off(i), ts(i + 1)))
      Source(updates).via(matcher.flow).runWith(Sink.ignore).futureValue
      store
        .lookupMatched(Seq(intern(p2) -> cp(ts(0), ts(count))))
        .futureValueUS should contain theSameElementsAs
        (1L to count).map(i => CommitmentMatchPeriod.matched(intern(p2), ts(i - 1), ts(i), off(i)))
      store.watermarks().futureValueUS.matching shouldBe Some(off(count))
    }

    "process commitments from different participants concurrently and sequentially per participant" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val stringInterning = new MockStringInterning()
      val store = new InMemoryAcsCommitmentPeriodStore(
        Eval.now(stringInterning),
        loggerFactory,
        enableConsistencyChecks = true,
      )

      val participants = Seq(p2, p3, p4).zipWithIndex
      val outstanding = participants.map { case (participant, i) =>
        CommitmentMatchPeriod.outstanding(
          stringInterning.participantId.internalize(participant),
          ts(0),
          ts(10),
          s"P$i:0-10",
        )
      }
      store.markOutstanding(outstanding).futureValueUS
      store.increaseInsertionWatermark(ts(10), affirmationOnly = false).futureValueUS

      val updates = participants.flatMap { case (sender, i) =>
        val first = Fixture.mkAcsUpdateContainer(
          sender,
          p1,
          ts(0),
          ts(5),
          s"P$i:0-10",
          off(2 * i.toLong + 1),
          ts(i.toLong + 11),
        )
        val second = Fixture.mkAcsUpdateContainer(
          sender,
          p1,
          ts(5),
          ts(10),
          s"P$i:0-10",
          off(2 * i.toLong + 2),
          ts(i.toLong + 20),
        )
        Seq(first, second)
      }

      // Delay completion of the writes until writes have been triggered for all participants.
      val promise = PromiseUnlessShutdown.unsupervised[Unit]()
      val calls = new AtomicInteger(0)

      val slowStore = new DelegatingAcsCommitmentPeriodStore(store) {
        override def persistMatchingOutcome(
            deleteOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
            deleteMismatched: immutable.Iterable[MismatchedCommitmentMatchPeriod],
            insertOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
            insertMismatchedOrUnexpected: immutable.Iterable[
              MismatchedOrUnexpectedCommitmentMatchPeriod
            ],
            insertMatched: immutable.Iterable[MatchedCommitmentMatchPeriod],
        )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
          calls.incrementAndGet().discard
          promise.futureUS.flatMap(_ =>
            super.persistMatchingOutcome(
              deleteOutstanding,
              deleteMismatched,
              insertOutstanding,
              insertMismatchedOrUnexpected,
              insertMatched,
            )
          )(ec)
        }
      }
      val matcher =
        new ReceivedAcsCommitmentMatcher(
          slowStore,
          Eval.now(stringInterning),
          loggerFactory,
          PositiveInt.tryCreate(10),
        )

      val fut = Source(updates).via(matcher.flow).runWith(Sink.ignore)
      always() {
        fut.isCompleted shouldBe false
        calls.get should be <= participants.size
      }
      eventually() {
        calls.get shouldBe participants.size
      }
      promise.outcome_(())
      fut.futureValue

      val periods = participants.map { case (p, _) =>
        stringInterning.participantId.internalize(p) -> cp(ts(0), ts(10))
      }
      val expected = participants.flatMap { case (p, i) =>
        val first = CommitmentMatchPeriod.matched(
          stringInterning.participantId.internalize(p),
          ts(0),
          ts(5),
          off(2 * i.toLong + 1),
        )
        val second = CommitmentMatchPeriod.matched(
          stringInterning.participantId.internalize(p),
          ts(5),
          ts(10),
          off(2 * i.toLong + 2),
        )
        Seq(first, second)
      }
      store.lookupMatched(periods).futureValueUS should contain theSameElementsAs expected
      store.watermarks().futureValueUS.matching shouldBe Some(off(updates.size.toLong))
    }
  }

  implicit def stringToByteString(s: String): ByteString =
    Digest.hashDigest(ByteString.copyFromUtf8(s)).getCryptographicEvidence
}
