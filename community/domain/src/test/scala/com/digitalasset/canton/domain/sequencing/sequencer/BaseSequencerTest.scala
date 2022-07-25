// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream.scaladsl.{Keep, Source}
import akka.stream.{KillSwitches, Materializer}
import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors.{
  CreateSubscriptionError,
  RegisterMemberError,
  SequencerWriteError,
}
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  domainManager,
  participant1,
  participant2,
}
import com.digitalasset.canton.topology.{Member, UnauthenticatedMemberId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable
import scala.concurrent.Future

class BaseSequencerTest extends AsyncWordSpec with BaseTest {
  val messageId = MessageId.tryCreate("test-message-id")
  def mkBatch(recipients: Set[Member]): Batch[ClosedEnvelope] =
    Batch[ClosedEnvelope](
      ClosedEnvelope(ByteString.EMPTY, Recipients.ofSet(recipients).value) :: Nil,
      testedProtocolVersion,
    )
  def submission(from: Member, to: Set[Member]) =
    SubmissionRequest(
      from,
      messageId,
      isRequest = true,
      mkBatch(to),
      CantonTimestamp.MaxValue,
      None,
      testedProtocolVersion,
    )

  private implicit val materializer = mock[Materializer] // not used

  private val unauthenticatedMemberId =
    UniqueIdentifier.fromProtoPrimitive_("unm1::default").map(new UnauthenticatedMemberId(_)).value

  class StubSequencer(existingMembers: Set[Member])
      extends BaseSequencer(domainManager, loggerFactory) {
    val newlyRegisteredMembers =
      mutable
        .Set[Member]() // we're using the scalatest serial execution context so don't need a concurrent collection
    override protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncError, Unit] =
      EitherT.pure[Future, SendAsyncError](())
    override def isRegistered(member: Member)(implicit
        traceContext: TraceContext
    ): Future[Boolean] =
      Future.successful(existingMembers.contains(member))
    override def registerMember(member: Member)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SequencerWriteError[RegisterMemberError], Unit] = {
      newlyRegisteredMembers.add(member)
      EitherT.pure(())
    }
    override def readInternal(member: Member, offset: SequencerCounter)(implicit
        traceContext: TraceContext
    ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource] =
      EitherT.rightT[Future, CreateSubscriptionError](
        Source.empty.viaMat(KillSwitches.single)(Keep.right)
      )
    override def acknowledge(member: Member, timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): Future[Unit] = ???

    override def pruningStatus(implicit
        traceContext: TraceContext
    ): Future[SequencerPruningStatus] = ???
    override def prune(requestedTimestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): EitherT[Future, PruningError, String] = ???
    override def snapshot(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, SequencerSnapshot] =
      ???
    override def disableMember(member: Member)(implicit traceContext: TraceContext): Future[Unit] =
      ???
    override def close(): Unit = ()

    override def isLedgerIdentityRegistered(identity: LedgerIdentity)(implicit
        traceContext: TraceContext
    ): Future[Boolean] = ???

    override def authorizeLedgerIdentity(identity: LedgerIdentity)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, Unit] = ???

    // making this public on purpose so we can test
    override def healthChanged(health: SequencerHealthStatus): Unit = super.healthChanged(health)

    override protected def healthInternal(implicit
        traceContext: TraceContext
    ): Future[SequencerHealthStatus] = Future.successful(SequencerHealthStatus(isActive = true))

  }

  "sendAsync" should {
    "topology manager sends should automatically register all unknown members" in {
      val sequencer = new StubSequencer(existingMembers = Set(participant1))
      val request = submission(from = domainManager, to = Set(participant1, participant2))

      for {
        _ <- sequencer.sendAsync(request).value
      } yield sequencer.newlyRegisteredMembers should contain only participant2
    }

    "sends from an unauthenticated member should auto register this member" in {
      val sequencer = new StubSequencer(existingMembers = Set(participant1))
      val request = submission(from = unauthenticatedMemberId, to = Set(participant1, participant2))
      for {
        _ <- sequencer.sendAsync(request).value
      } yield sequencer.newlyRegisteredMembers should contain only unauthenticatedMemberId
    }

    "sends from anyone else should not auto register" in {
      val sequencer = new StubSequencer(existingMembers = Set(participant1))
      val request = submission(from = participant1, to = Set(participant1, participant2))

      for {
        _ <- sequencer.sendAsync(request).value
      } yield sequencer.newlyRegisteredMembers shouldBe empty
    }
  }

  "read" should {
    "read from an unauthenticated member should auto register this member" in {
      val sequencer = new StubSequencer(existingMembers = Set(participant1))
      for {
        _ <- sequencer
          .read(unauthenticatedMemberId, 0L)
          .value
      } yield sequencer.newlyRegisteredMembers should contain only unauthenticatedMemberId
    }
  }

  "health" should {
    "onHealthChange should register listener and immediately call it with current status" in {
      val sequencer = new StubSequencer(Set())
      var status = SequencerHealthStatus(false)
      sequencer.onHealthChange { newHealth =>
        status = newHealth
      }

      status shouldBe SequencerHealthStatus(true)
    }

    "health status change should trigger registered health listener" in {
      val sequencer = new StubSequencer(Set())
      var status = SequencerHealthStatus(true)
      sequencer.onHealthChange { newHealth =>
        status = newHealth
      }

      val badHealth = SequencerHealthStatus(false, Some("something bad happened"))
      sequencer.healthChanged(badHealth)

      status shouldBe badHealth

    }
  }
}
