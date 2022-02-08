// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import cats.syntax.traverse._
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.domain.sequencing.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.protocol.messages.ProtocolMessage
import com.digitalasset.canton.protocol.v0.EnvelopeContent
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology._
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, DomainId, HasExecutionContext, SequencerCounter}
import com.google.protobuf.ByteString
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration._

class SequencerTest extends FixtureAsyncWordSpec with BaseTest with HasExecutionContext {

  private val domainId = DefaultTestIdentities.domainId
  private val alice: Member = ParticipantId("alice")
  private val bob: Member = ParticipantId("bob")
  private val carole: Member = ParticipantId("carole")

  class Env extends FlagCloseableAsync {
    override val timeouts = SequencerTest.this.timeouts
    protected val logger = SequencerTest.this.logger
    private implicit val actorSystem = ActorSystem(classOf[SequencerTest].getSimpleName)
    private implicit val materializer = implicitly[Materializer]
    val store = new InMemorySequencerStore(loggerFactory)
    val clock = new WallClock(timeouts, loggerFactory = loggerFactory)
    val crypto = valueOrFail(
      TestingTopology()
        .build(loggerFactory)
        .forOwner(SequencerId(domainId))
        .forDomain(domainId)
        .toRight("crypto error")
    )("building crypto")

    val sequencer =
      new DatabaseSequencer(
        SequencerWriterStoreFactory.singleInstance,
        CommunitySequencerConfig.Database(),
        DefaultProcessingTimeouts.testing,
        new MemoryStorage(),
        clock,
        domainId,
        crypto,
        loggerFactory,
      )(parallelExecutionContext, tracer, materializer)

    def readAsSeq(
        member: Member,
        limit: Int,
        offset: SequencerCounter = 0L,
    ): Future[Seq[OrdinarySerializedEvent]] =
      valueOrFail(sequencer.readInternal(member, offset))(
        s"read for $member"
      ) flatMap {
        _.take(limit.toLong)
          .idleTimeout(30.seconds)
          .runWith(Sink.seq)
      }

    def asDeliverEvent(event: SequencedEvent[ClosedEnvelope]): Deliver[ClosedEnvelope] =
      event match {
        case deliver: Deliver[ClosedEnvelope] => deliver
        case other => fail(s"Expected deliver event but got $other")
      }

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
      SyncCloseable("sequencer", sequencer.close()),
      AsyncCloseable("actorSystem", actorSystem.terminate(), 10.seconds),
    )
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  class TestProtocolMessage(text: String) extends ProtocolMessage {
    override def toEnvelopeContentByteString(version: ProtocolVersion): ByteString =
      ByteString.copyFromUtf8(text)
    override def domainId: DomainId = ???
    override def toProtoEnvelopeContentV0(version: ProtocolVersion): EnvelopeContent = ???
    override def productElement(n: Int): Any = ???
    override def productArity: Int = ???
    override def canEqual(that: Any): Boolean = ???
  }

  "send" should {
    "correctly deliver event to each recipient" in { env =>
      import env._

      val messageId = MessageId.tryCreate("test-message")
      val message1 = new TestProtocolMessage("message1")
      val message2 = new TestProtocolMessage("message2")

      val submission = SubmissionRequest(
        alice,
        messageId,
        true,
        Batch
          .of(
            (message1, Recipients.cc(bob)),
            (message2, Recipients.cc(carole)),
          )
          .closeEnvelopes,
        clock.now.plusSeconds(10),
        None,
      )

      for {
        _ <- valueOrFail(List(alice, bob, carole).traverse(sequencer.registerMember))(
          "member registration"
        )
        _ <- valueOrFail(sequencer.sendAsync(submission))("send")
        aliceDeliverEvent <- readAsSeq(alice, 1)
          .map(_.loneElement.signedEvent.content)
          .map(asDeliverEvent)
        bobDeliverEvent <- readAsSeq(bob, 1)
          .map(_.loneElement.signedEvent.content)
          .map(asDeliverEvent)
        caroleDeliverEvent <- readAsSeq(carole, 1)
          .map(_.loneElement.signedEvent.content)
          .map(asDeliverEvent)
      } yield {
        aliceDeliverEvent.messageId.value shouldBe messageId // as alice is the sender
        aliceDeliverEvent.batch.envelopes should have size (0) // as we didn't send a message to ourself

        bobDeliverEvent.messageId shouldBe None
        bobDeliverEvent.batch.envelopes.map(_.bytes) should contain only message1
          .toEnvelopeContentByteString(ProtocolVersion.default)

        caroleDeliverEvent.messageId shouldBe None
        caroleDeliverEvent.batch.envelopes.map(_.bytes) should contain only message2
          .toEnvelopeContentByteString(ProtocolVersion.default)
      }
    }
  }
}
