// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.DeliverErrorReason.{BatchInvalid, BatchRefused}
import com.digitalasset.canton.sequencing.protocol.{AggregationRule, *}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.AkkaUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

abstract class SequencerApiTest
    extends FixtureAsyncWordSpec
    with HasExecutionContext
    with BaseTest
    with ProtocolVersionChecksFixtureAsyncWordSpec {

  import RecipientsTest.*

  class Env extends AutoCloseable with NamedLogging {
    protected val loggerFactory: NamedLoggerFactory = SequencerApiTest.this.loggerFactory

    implicit val actorSystem: ActorSystem =
      AkkaUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

    val topologyFactory =
      new TestingIdentityFactory(
        topology = TestingTopology(additionalParticipants = Set(p11, p12, p13, p14, p15)),
        loggerFactory,
        List.empty,
      )
    val sequencer: CantonSequencer = SequencerApiTest.this.createSequencer(
      topologyFactory.forOwnerAndDomain(owner = mediatorId, domainId)
    )

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
  def topologyClientMember: Member = DefaultTestIdentities.sequencerId

  def createSequencer(crypto: DomainSyncCryptoClient)(implicit
      materializer: Materializer
  ): CantonSequencer

  protected def supportAggregation: Boolean

  "The sequencers" should {
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
        val details = EventDetails(
          SequencerCounter(0),
          sender,
          Some(request.messageId),
          EnvelopeDetails(messageContent, recipients),
        )
        checkMessages(List(details), messages)
      }
    }

    "not fail when a block is empty due to suppressed events" in { env =>
      import env.*

      val suppressedMessageContent = "suppressed message"
      // TODO(i10412): The sequencer implementations for tests currently do not all behave in the same way.
      // Until this is fixed, we are currently sidestepping the issue by using a different set of recipients
      // for each test to ensure "isolation".
      val sender = p7.member
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
      // TODO(i10412): See above
      val sender = p8.member
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
          SequencerCounter.Genesis,
          sender,
          Some(request1.messageId),
          EnvelopeDetails(normalMessageContent, recipients),
        )
        checkMessages(List(details), messages)
      }
    }

    "send recipients only the subtrees that they should see" in { env =>
      import env.*
      val messageContent = "msg1"
      val sender: MediatorId = mediatorId
      // TODO(i10412): See above
      val recipients = Recipients(NonEmpty(Seq, t5, t3))
      val readFor: List[Member] = recipients.allRecipients.collect { case MemberRecipient(member) =>
        member
      }.toList

      val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

      val expectedDetailsForMembers = readFor.map { member =>
        EventDetails(
          SequencerCounter.Genesis,
          member,
          Option.when(member == sender)(request.messageId),
          EnvelopeDetails(messageContent, recipients.forMember(member, Set.empty).value),
        )
      }

      for {
        _ <- registerMembers(
          recipients.allRecipients.collect { case MemberRecipient(member) =>
            member
          } + sender + topologyClientMember,
          sequencer,
        )
        _ <- valueOrFail(sequencer.sendAsync(request))("Sent async")
        reads <- readForMembers(readFor, sequencer)
      } yield {
        checkMessages(expectedDetailsForMembers, reads)
      }
    }

    def testAggregation: Boolean =
      // TODO(#12373) Adapt when releasing BFT
      testedProtocolVersion >= ProtocolVersion.dev && supportAggregation

    "aggregate submission requests" onlyRunWhen testAggregation in { env =>
      import env.*

      val messageContent = "aggregatable-message"
      // TODO(i10412): See above
      val aggregationRule =
        AggregationRule(NonEmpty(Seq, p6, p9), PositiveInt.tryCreate(2), testedProtocolVersion)
      val request1 = createSendRequest(
        p6,
        messageContent,
        Recipients.cc(p10),
        aggregationRule = Some(aggregationRule),
      )
      val request2 = request1.copy(sender = p9, messageId = MessageId.fromUuid(new UUID(1, 2)))

      for {
        _ <- registerMembers(Set(p6, p9, p10, topologyClientMember), sequencer)
        _ <- valueOrFail(sequencer.sendAsync(request1))("Sent async for participant1")
        reads1 <- readForMembers(Seq(p6), sequencer)
        _ <- valueOrFail(sequencer.sendAsync(request2))("Sent async for participant2")
        reads2 <- readForMembers(Seq(p9), sequencer)
        reads3 <- readForMembers(Seq(p10), sequencer)
      } yield {
        // p6 gets the receipt immediately
        checkMessages(
          Seq(EventDetails(SequencerCounter.Genesis, p6, Some(request1.messageId))),
          reads1,
        )
        // p9 gets the receipt only
        checkMessages(
          Seq(EventDetails(SequencerCounter.Genesis, p9, Some(request2.messageId))),
          reads2,
        )
        // p10 gets the message
        checkMessages(
          Seq(
            EventDetails(
              SequencerCounter.Genesis,
              p10,
              None,
              EnvelopeDetails(messageContent, Recipients.cc(p10)),
            )
          ),
          reads3,
        )
      }
    }

    "aggregate signatures" onlyRunWhen testAggregation in { env =>
      import env.*

      // TODO(i10412): See above
      val aggregationRule =
        AggregationRule(
          NonEmpty(Seq, p11, p12, p13),
          PositiveInt.tryCreate(2),
          testedProtocolVersion,
        )

      val content1 = "message1-to-sign"
      val content2 = "message2-to-sign"
      val recipients1 = Recipients.cc(p11, p13)
      val envelope1 = ClosedEnvelope.tryCreate(
        ByteString.copyFromUtf8(content1),
        recipients1,
        Seq.empty,
        testedProtocolVersion,
      )
      val recipients2 = Recipients.cc(p12, p13)
      val envelope2 = ClosedEnvelope.tryCreate(
        ByteString.copyFromUtf8(content2),
        recipients2,
        Seq.empty,
        testedProtocolVersion,
      )
      val envelopes = List(envelope1, envelope2)
      val messageId1 = MessageId.tryCreate(s"request1")
      val messageId2 = MessageId.tryCreate(s"request2")
      val messageId3 = MessageId.tryCreate(s"request3")
      val p11Crypto = topologyFactory.forOwnerAndDomain(p11, domainId)
      val p12Crypto = topologyFactory.forOwnerAndDomain(p12, domainId)
      val p13Crypto = topologyFactory.forOwnerAndDomain(p13, domainId)

      def mkRequest(
          sender: Member,
          messageId: MessageId,
          envelopes: List[ClosedEnvelope],
      ): SubmissionRequest =
        SubmissionRequest.tryCreate(
          sender,
          messageId,
          isRequest = false,
          Batch(envelopes, testedProtocolVersion),
          CantonTimestamp.MaxValue,
          timestampOfSigningKey = None,
          Some(aggregationRule),
          testedProtocolVersion,
        )

      for {
        _ <- registerMembers(Set(p11, p12, p13, topologyClientMember), sequencer)
        envs1 <- envelopes.parTraverse(signEnvelope(p11Crypto, _))
        request1 = mkRequest(p11, messageId1, envs1)
        envs2 <- envelopes.parTraverse(signEnvelope(p12Crypto, _))
        request2 = mkRequest(p12, messageId2, envs2)
        _ <- valueOrFail(sequencer.sendAsync(request1))("Sent async for participant11")
        reads11 <- readForMembers(Seq(p11), sequencer)
        _ <- valueOrFail(sequencer.sendAsync(request2))("Sent async for participant13")
        reads12 <- readForMembers(Seq(p12, p13), sequencer)
        reads12a <- readForMembers(
          Seq(p11),
          sequencer,
          firstSequencerCounter = SequencerCounter.Genesis + 1,
        )

        // participant13 is late to the party and its request is refused
        envs3 <- envelopes.parTraverse(signEnvelope(p13Crypto, _))
        request3 = mkRequest(p13, messageId3, envs3)
        _ <- valueOrFail(sequencer.sendAsync(request3))("Sent async for participant13")
        reads13 <- readForMembers(
          Seq(p13),
          sequencer,
          firstSequencerCounter = SequencerCounter.Genesis + 1,
        )
      } yield {
        checkMessages(
          Seq(EventDetails(SequencerCounter.Genesis, p11, Some(request1.messageId))),
          reads11,
        )
        checkMessages(
          Seq(
            EventDetails(
              SequencerCounter.Genesis,
              p12,
              Some(request1.messageId),
              EnvelopeDetails(content2, recipients2, envs1(1).signatures ++ envs2(1).signatures),
            ),
            EventDetails(
              SequencerCounter.Genesis,
              p13,
              None,
              EnvelopeDetails(content1, recipients1, envs1(0).signatures ++ envs2(0).signatures),
              EnvelopeDetails(content2, recipients2, envs1(1).signatures ++ envs2(1).signatures),
            ),
          ),
          reads12,
        )
        checkMessages(
          Seq(
            EventDetails(
              SequencerCounter.Genesis + 1,
              p11,
              None,
              EnvelopeDetails(content1, recipients1, envs1(0).signatures ++ envs2(0).signatures),
            )
          ),
          reads12a,
        )

        checkRejection(reads13, p13, messageId3) { case BatchRefused(reason) =>
          reason should (
            include(s"The aggregatable request with aggregation ID") and
              include("was previously delivered at")
          )
        }
      }
    }

    "prevent aggregation stuffing" onlyRunWhen testAggregation in { env =>
      import env.*

      val messageContent = "aggregatable-message-stuffing"
      // TODO(i10412): See above
      val aggregationRule =
        AggregationRule(NonEmpty(Seq, p14, p15), PositiveInt.tryCreate(2), testedProtocolVersion)
      val recipients = Recipients.cc(p14, p15)
      val envelope = ClosedEnvelope.tryCreate(
        ByteString.copyFromUtf8(messageContent),
        recipients,
        Seq.empty,
        testedProtocolVersion,
      )
      val messageId1 = MessageId.tryCreate(s"request1")
      val messageId2 = MessageId.tryCreate(s"request2")
      val messageId3 = MessageId.tryCreate(s"request3")
      val p14Crypto = topologyFactory.forOwnerAndDomain(p14, domainId)
      val p15Crypto = topologyFactory.forOwnerAndDomain(p15, domainId)

      def mkRequest(
          sender: Member,
          messageId: MessageId,
          envelope: ClosedEnvelope,
      ): SubmissionRequest =
        SubmissionRequest.tryCreate(
          sender,
          messageId,
          isRequest = false,
          Batch(List(envelope), testedProtocolVersion),
          CantonTimestamp.MaxValue,
          timestampOfSigningKey = None,
          Some(aggregationRule),
          testedProtocolVersion,
        )

      for {
        _ <- registerMembers(Set(p14, p15, topologyClientMember), sequencer)
        env1 <- signEnvelope(p14Crypto, envelope)
        request1 = mkRequest(p14, messageId1, env1)
        env2 <- signEnvelope(p14Crypto, envelope)
        request2 = mkRequest(p14, messageId2, env2)
        env3 <- signEnvelope(p15Crypto, envelope)
        request3 = mkRequest(p15, messageId3, env3)
        _ <- valueOrFail(sequencer.sendAsync(request1))("Sent async for participant14")
        reads14 <- readForMembers(Seq(p14), sequencer)
        _ <- valueOrFail(sequencer.sendAsync(request2))("Sent async stuffing for participant14")
        reads14a <- readForMembers(
          Seq(p14),
          sequencer,
          firstSequencerCounter = SequencerCounter.Genesis + 1,
        )
        // p15 can still continue and finish the aggregation
        _ <- valueOrFail(sequencer.sendAsync(request3))("Sent async for participant15")
        reads14b <- readForMembers(
          Seq(p14),
          sequencer,
          firstSequencerCounter = SequencerCounter.Genesis + 2,
        )
        reads15 <- readForMembers(Seq(p15), sequencer)
      } yield {
        checkMessages(
          Seq(EventDetails(SequencerCounter.Genesis, p14, Some(request1.messageId))),
          reads14,
        )
        checkRejection(reads14a, p14, messageId2) { case BatchRefused(reason) =>
          reason should include(
            s"The sender ${p14} previously contributed to the aggregatable submission with ID"
          )
        }
        val deliveredEnvelopeDetails = EnvelopeDetails(
          messageContent,
          recipients,
          // Only the first signature from p1 is included
          env1.signatures ++ env3.signatures,
        )

        checkMessages(
          Seq(EventDetails(SequencerCounter.Genesis + 2, p14, None, deliveredEnvelopeDetails)),
          reads14b,
        )
        checkMessages(
          Seq(
            EventDetails(SequencerCounter.Genesis, p15, Some(messageId3), deliveredEnvelopeDetails)
          ),
          reads15,
        )
      }
    }

    "require eligible senders be registered" onlyRunWhen testAggregation in { env =>
      import env.*

      // We expect synchronous rejections and can therefore reuse participant1.
      // But we need a fresh unregistered participant16
      // TODO(i10412): remove this comment
      val aggregationRule =
        AggregationRule(NonEmpty(Seq, p1, p16), PositiveInt.tryCreate(1), testedProtocolVersion)

      val request = createSendRequest(
        p1,
        "unregistered-eligible-sender",
        Recipients.cc(p1),
        aggregationRule = Some(aggregationRule),
      )

      for {
        _ <- registerMembers(Set(p1, topologyClientMember), sequencer)
        error <- leftOrFail(sequencer.sendAsync(request))("Sent async")
      } yield {
        error shouldBe a[SendAsyncError.SenderUnknown]
        error.message should (
          include("The following senders in the aggregation rule are unknown") and
            include(p16.toString)
        )
      }
    }

    "require the threshold to be reachable" onlyRunWhen testAggregation in { env =>
      import env.*

      // TODO(i10412): See above
      val aggregationRule =
        AggregationRule(NonEmpty(Seq, p16, p16), PositiveInt.tryCreate(2), testedProtocolVersion)

      val messageId = MessageId.tryCreate("unreachable-threshold")
      val request = SubmissionRequest.tryCreate(
        p16,
        messageId,
        isRequest = false,
        Batch.empty(testedProtocolVersion),
        maxSequencingTime = CantonTimestamp.MaxValue,
        timestampOfSigningKey = None,
        aggregationRule = Some(aggregationRule),
        testedProtocolVersion,
      )

      for {
        _ <- registerMembers(Set(p16, topologyClientMember), sequencer)
        _ <- valueOrFail(sequencer.sendAsync(request))("Sent async")
        reads <- readForMembers(Seq(p16), sequencer)
      } yield {
        checkRejection(reads, p16, messageId) { case BatchInvalid(reason) =>
          reason should include("Threshold 2 cannot be reached")
        }
      }
    }

    "require the sender to be eligible" onlyRunWhen testAggregation in { env =>
      import env.*

      // TODO(i10412): See above
      val aggregationRule =
        AggregationRule(NonEmpty(Seq, p16), PositiveInt.tryCreate(1), testedProtocolVersion)

      val messageId = MessageId.tryCreate("unreachable-threshold")
      val request = SubmissionRequest.tryCreate(
        p17,
        messageId,
        isRequest = false,
        Batch.empty(testedProtocolVersion),
        maxSequencingTime = CantonTimestamp.MaxValue,
        timestampOfSigningKey = None,
        aggregationRule = Some(aggregationRule),
        testedProtocolVersion,
      )

      for {
        _ <- registerMembers(Set(p16, p17, topologyClientMember), sequencer)
        _ <- valueOrFail(sequencer.sendAsync(request))("Sent async")
        reads <- readForMembers(Seq(p17), sequencer)
      } yield {
        checkRejection(reads, p17, messageId) { case BatchInvalid(reason) =>
          reason should include("Sender is not eligible according to the aggregation rule")
        }
      }
    }

    "require all eligible senders be authenticated" onlyRunWhen testAggregation in { env =>
      import env.*

      val unauthenticatedMember =
        UnauthenticatedMemberId(UniqueIdentifier.tryCreate("unauthenticated", "member"))
      // TODO(i10412): See above
      val aggregationRule = AggregationRule(
        NonEmpty(Seq, p18, unauthenticatedMember),
        PositiveInt.tryCreate(1),
        testedProtocolVersion,
      )

      val messageId = MessageId.tryCreate("unreachable-threshold")
      val request = SubmissionRequest.tryCreate(
        p18,
        messageId,
        isRequest = false,
        Batch.empty(testedProtocolVersion),
        maxSequencingTime = CantonTimestamp.MaxValue,
        timestampOfSigningKey = None,
        aggregationRule = Some(aggregationRule),
        testedProtocolVersion,
      )

      for {
        _ <- registerMembers(Set(p18, unauthenticatedMember, topologyClientMember), sequencer)
        _ <- valueOrFail(sequencer.sendAsync(request))("Sent async")
        reads <- readForMembers(Seq(p18), sequencer)
      } yield {
        checkRejection(reads, p18, messageId) { case BatchInvalid(reason) =>
          reason should include(
            "Eligible senders in aggregation rule must be authenticated, but found unauthenticated members"
          )
        }
      }
    }
  }

  private def readForMembers(
      members: Seq[Member],
      sequencer: CantonSequencer,
      // up to 60 seconds needed because Besu is very slow on CI
      timeout: FiniteDuration = 60.seconds,
      firstSequencerCounter: SequencerCounter = SequencerCounter.Genesis,
  )(implicit materializer: Materializer): Future[Seq[(Member, OrdinarySerializedEvent)]] = {
    members
      .parTraverseFilter { member =>
        for {
          source <- valueOrFail(sequencer.read(member, firstSequencerCounter))(
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

  case class EnvelopeDetails(
      content: String,
      recipients: Recipients,
      signatures: Seq[Signature] = Seq.empty,
  )

  case class EventDetails(
      counter: SequencerCounter,
      to: Member,
      messageId: Option[MessageId],
      envs: EnvelopeDetails*
  )

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
      aggregationRule: Option[AggregationRule] = None,
  ): SubmissionRequest = {
    val envelope1 = TestingEnvelope(messageContent, recipients)
    val batch = Batch(List(envelope1.closeEnvelope), testedProtocolVersion)
    val messageId = MessageId.tryCreate(s"thisisamessage: $messageContent")
    SubmissionRequest.tryCreate(
      sender,
      messageId,
      isRequest = false,
      batch,
      maxSequencingTime,
      timestampOfSigningKey = None,
      aggregationRule,
      testedProtocolVersion,
    )
  }

  private def checkMessages(
      expectedMessages: Seq[EventDetails],
      receivedMessages: Seq[(Member, OrdinarySerializedEvent)],
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
          withClue(s"Received the wrong number of envelopes for recipient $member") {
            batch.envelopes.length shouldBe expectedMessage.envs.length
          }

          forAll(batch.envelopes.zip(expectedMessage.envs)) { case (got, wanted) =>
            got.recipients shouldBe wanted.recipients
            got.bytes shouldBe ByteString.copyFromUtf8(wanted.content)
            got.signatures shouldBe wanted.signatures
          }

        case _ => fail(s"Event $event is not a deliver")
      }
    }
  }

  def checkRejection(
      got: Seq[(Member, OrdinarySerializedEvent)],
      sender: Member,
      expectedMessageId: MessageId,
  )(assertReason: PartialFunction[DeliverErrorReason, Assertion]): Assertion = {
    got match {
      case Seq((`sender`, event)) =>
        event.signedEvent.content match {
          case DeliverError(_counter, _timestamp, _domainId, messageId, reason) =>
            messageId shouldBe expectedMessageId
            assertReason(reason)

          case _ => fail(s"Expected a deliver error, but got $event")
        }
      case _ => fail(s"Read wrong events for $sender: $got")
    }
  }

  def signEnvelope(
      crypto: DomainSyncCryptoClient,
      envelope: ClosedEnvelope,
  ): Future[ClosedEnvelope] = {
    val hash = crypto.pureCrypto.digest(HashPurpose.SignedProtocolMessageSignature, envelope.bytes)
    crypto.currentSnapshotApproximation
      .sign(hash)
      .valueOrFail(s"Failed to sign $envelope")
      .map(sig => envelope.copy(signatures = Seq(sig)))
  }

  case class TestingEnvelope(content: String, override val recipients: Recipients)
      extends Envelope[String] {

    /** Closes the envelope by serializing the contents */
    def closeEnvelope: ClosedEnvelope =
      ClosedEnvelope.tryCreate(
        ByteString.copyFromUtf8(content),
        recipients,
        Seq.empty,
        testedProtocolVersion,
      )

    override def forRecipient(
        member: Member,
        groupAddresses: Set[GroupRecipient],
    ): Option[Envelope[String]] = {
      recipients
        .forMember(member, groupAddresses)
        .map(recipients => TestingEnvelope(content, recipients))
    }

    override def pretty: Pretty[TestingEnvelope] = adHocPrettyInstance
  }
}
