// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.AkkaUtil
import com.digitalasset.canton.util.FutureInstances.*
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

abstract class SequencerApiTest
    extends FixtureAsyncWordSpec
    with HasExecutionContext
    with BaseTest {

  import RecipientsTest.*

  class Env extends AutoCloseable with NamedLogging {
    protected val loggerFactory: NamedLoggerFactory = SequencerApiTest.this.loggerFactory

    implicit val actorSystem: ActorSystem =
      AkkaUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

    val sequencer: CantonSequencer = SequencerApiTest.this.createSequencer

    override def close(): Unit = {
      sequencer.close()
      Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts).close()
    }
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()
    complete {
      super.withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  def domainId: DomainId = DefaultTestIdentities.domainId
  def mediatorId: MediatorId = DefaultTestIdentities.mediator
  def topologyClientMember: Member = DefaultTestIdentities.sequencer

  def createSequencer(implicit materializer: Materializer): CantonSequencer

  "The sequencers should" should {
    "send a batch to one recipient" in { env =>
      import env.*
      val messageContent = "hello"
      val sender: MediatorId = mediatorId
      val recipients = Recipients.cc(sender)

      val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

      for {
        _ <- valueOrFail(sequencer.registerMember(topologyClientMember))("Register topology client")
        _ <- valueOrFail(sequencer.registerMember(sender))("Register mediator")
        _ <- valueOrFail(sequencer.sendAsync(request))("Sent async")
        messages <- readForMembers(List(sender), sequencer)
      } yield {
        val details =
          EventDetails(SequencerCounter(0), sender, EnvelopeDetails(messageContent, recipients))
        checkMessages(List(details), messages)
      }
    }

    "not fail when a block is empty due to suppressed events" in { env =>
      import env.*

      val suppressedMessageContent = "suppressed message"
      // TODO(i10412): The sequencer implementations for tests currently do not all behave in the same way.
      // Until this is fixed, we are currently sidestepping the issue by using a different set of recipients
      // for each test to ensure "isolation".
      val sender = p7
      val recipients = Recipients.cc(sender)

      val tsInThePast = CantonTimestamp.MinValue

      val request = createSendRequest(
        sender,
        suppressedMessageContent,
        recipients,
        maxSequencingTime = tsInThePast,
      )

      for {
        _ <- valueOrFail(sequencer.registerMember(topologyClientMember))("Register topology client")
        _ <- valueOrFail(sequencer.registerMember(sender))("Register mediator")
        messages <- loggerFactory.assertLogs(
          valueOrFail(sequencer.sendAsync(request))("Sent async")
            .flatMap(_ =>
              readForMembers(
                List(sender),
                sequencer,
                timeout = 5.seconds, // We don't need the full timeout here
              )
            ),
          entry => {
            entry.warningMessage should include("has exceeded the max-sequencing-time")
            entry.warningMessage should include(suppressedMessageContent)
          },
        )
      } yield {
        checkMessages(List(), messages)
      }
    }

    "not fail when some events in a block are suppressed" in { env =>
      import env.*

      val normalMessageContent = "normal message"
      val suppressedMessageContent = "suppressed message"
      // See note from previous test
      val sender = p8
      val recipients = Recipients.cc(sender)

      val tsInThePast = CantonTimestamp.MinValue

      val request1 = createSendRequest(sender, normalMessageContent, recipients)
      val request2 = createSendRequest(
        sender,
        suppressedMessageContent,
        recipients,
        maxSequencingTime = tsInThePast,
      )

      for {
        _ <- valueOrFail(sequencer.registerMember(topologyClientMember))("Register topology client")
        _ <- valueOrFail(sequencer.registerMember(sender))("Register mediator")
        _ <- valueOrFail(sequencer.sendAsync(request1))("Sent async #1")
        messages <- loggerFactory.assertLogs(
          valueOrFail(sequencer.sendAsync(request2))("Sent async #2")
            .flatMap(_ => readForMembers(List(sender), sequencer)),
          entry => {
            entry.warningMessage should include("has exceeded the max-sequencing-time")
            entry.warningMessage should include(suppressedMessageContent)
          },
        )
      } yield {
        val details = EventDetails(
          SequencerCounter(0),
          sender,
          EnvelopeDetails(normalMessageContent, recipients),
        )
        checkMessages(List(details), messages)
      }
    }

    "send recipients only the subtrees that they should see" in { env =>
      import env.*
      val messageContent = "msg1"
      val sender: MediatorId = mediatorId
      val recipients = Recipients(NonEmpty(Seq, t5, t3))
      val readFor: List[Member] = recipients.allRecipients.toList

      val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

      val expectedDetailsForMembers = readFor.map { member =>
        EventDetails(
          SequencerCounter(0),
          member,
          EnvelopeDetails(messageContent, recipients.forMember(member).value),
        )
      }

      for {
        _ <- registerMembers(recipients.allRecipients + sender + topologyClientMember, sequencer)
        _ <- valueOrFail(sequencer.sendAsync(request))(s"Sent async")
        reads <- readForMembers(readFor, sequencer)
      } yield {
        checkMessages(expectedDetailsForMembers, reads)
      }
    }
  }

  private def readForMembers(
      members: List[Member],
      sequencer: CantonSequencer,
      // up to 60 seconds needed because Besu is very slow on CI
      timeout: FiniteDuration = 60.seconds,
  )(implicit materializer: Materializer): Future[List[(Member, OrdinarySerializedEvent)]] = {
    members
      .parTraverseFilter { member =>
        for {
          source <- valueOrFail(sequencer.read(member, SequencerCounter(0)))(
            s"Read for $member"
          )
          events <- source
            // hard-coding that we only expect 1 event per member
            .take(1)
            .takeWithin(timeout)
            .runWith(Sink.seq)
            .map {
              case Seq(e) => Some((member, e))
              case _ =>
                // We read no messages for a member when we expected some
                None
            }
        } yield events
      }
  }

  case class EnvelopeDetails(content: String, recipients: Recipients)

  case class EventDetails(counter: SequencerCounter, to: Member, envs: EnvelopeDetails*)

  private def registerMembers(members: Set[Member], sequencer: CantonSequencer): Future[Unit] =
    members.toList.parTraverse_ { member =>
      val registerE = sequencer.registerMember(member)
      valueOrFail(registerE)(s"Register member $member")
    }

  private def createSendRequest(
      sender: Member,
      messageContent: String,
      recipients: Recipients,
      maxSequencingTime: CantonTimestamp = CantonTimestamp.MaxValue,
  ) = {
    val envelope1 = TestingEnvelope(messageContent, recipients)
    val batch = Batch(List(envelope1.closeEnvelope), testedProtocolVersion)
    val messageId = MessageId.tryCreate(s"thisisamessage: $messageContent")
    val request = SubmissionRequest(
      sender,
      messageId,
      isRequest = false,
      batch,
      maxSequencingTime,
      timestampOfSigningKey = None,
      testedProtocolVersion,
    )
    request
  }

  private def checkMessages(
      expectedMessages: List[EventDetails],
      receivedMessages: List[(Member, OrdinarySerializedEvent)],
  ): Assertion = {

    receivedMessages.length shouldBe expectedMessages.length

    val sortExpected = expectedMessages.sortBy(e => e.to)
    val sortReceived = receivedMessages.sortBy { case (member, _) => member }

    forAll(sortReceived.zip(sortExpected)) { case ((member, message), expectedMessage) =>
      withClue(s"Member mismatch") { member shouldBe expectedMessage.to }

      withClue(s"Sequencer counter is wrong") {
        message.counter shouldBe expectedMessage.counter
      }

      val event = message.signedEvent.content

      event match {
        case Deliver(_, _, _, _, batch) =>
          batch.allRecipients

          withClue(s"Received the wrong number of envelopes for recipient $member") {
            batch.envelopes.length shouldBe expectedMessage.envs.length
          }

          forAll(batch.envelopes.zip(expectedMessage.envs)) { case (got, wanted) =>
            got.recipients shouldBe wanted.recipients
            got.bytes shouldBe DeterministicEncoding.encodeString(wanted.content)
          }

        case _ => fail(s"Event $event is not a deliver")
      }
    }
  }

  case class TestingEnvelope(content: String, override val recipients: Recipients)
      extends Envelope[String] {

    /** Closes the envelope by serializing the contents */
    def closeEnvelope: ClosedEnvelope =
      ClosedEnvelope(
        DeterministicEncoding.encodeString(content),
        recipients,
        Seq.empty,
        testedProtocolVersion,
      )

    override def forRecipient(member: Member): Option[Envelope[String]] = {
      recipients.forMember(member).map(recipients => TestingEnvelope(content, recipients))
    }

    override def pretty: Pretty[TestingEnvelope] = adHocPrettyInstance
  }
}
