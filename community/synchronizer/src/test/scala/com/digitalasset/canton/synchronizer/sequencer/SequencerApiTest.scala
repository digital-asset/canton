// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{
  HashPurpose,
  Signature,
  SigningKeyUsage,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.sequencing.SequencedSerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.SubmissionRequestMalformed
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.block.update.BlockChunkProcessor
import com.digitalasset.canton.synchronizer.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.synchronizer.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.ExceededMaxSequencingTime
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil, PekkoUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}
import org.slf4j.event.Level

import java.time.Duration
import java.util.UUID
import scala.annotation.nowarn
import scala.concurrent.Promise
import scala.concurrent.duration.{DurationInt, FiniteDuration}

abstract class SequencerApiTest
    extends SequencerApiTestUtils
    with ProtocolVersionChecksFixtureAsyncWordSpec
    with FailOnShutdown {

  import RecipientsTest.*

  private lazy val m1 = MediatorId(UniqueIdentifier.tryCreate("mediator1", "abc"))
  private lazy val m2 = MediatorId(UniqueIdentifier.tryCreate("mediator2", "abc"))
  private lazy val m3 = MediatorId(UniqueIdentifier.tryCreate("mediator3", "abc"))

  protected class Env extends AutoCloseable {

    implicit lazy val actorSystem: ActorSystem =
      PekkoUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

    lazy val sequencer: CantonSequencer = {
      val sequencer = SequencerApiTest.this.createSequencer(
        topologyFactory.forOwnerAndSynchronizer(owner = sequencerId, psid)
      )
      registerAllTopologyMembers(topologyFactory.topologySnapshot(), sequencer)
      sequencer
    }

    val topologyFactory: TestingIdentityFactory =
      TestingTopology(
        synchronizerParameters = List.empty,
        mediatorGroups = Set(
          MediatorGroup(
            index = MediatorGroupIndex.zero,
            active = NonEmpty.mk(Seq, m1, m2, m3),
            passive = Seq.empty,
            threshold = PositiveInt.two,
          )
        ),
      )
        .withSimpleParticipants(
          p1,
          p2,
          p3,
          p4,
          p5,
          p6,
          p7,
          p8,
          p9,
          p10,
          p11,
          p12,
          p13,
          p14,
          p15,
          p17,
          p18,
          p19,
        )
        .withDynamicSynchronizerParameters(
          DynamicSynchronizerParameters.initialValues(testedProtocolVersion),
          validFrom = CantonTimestamp.MinValue,
        )
        .build(loggerFactory)

    def sign(
        request: SubmissionRequest
    ): SignedContent[SubmissionRequest] = {
      val cryptoSnapshot =
        topologyFactory
          .forOwnerAndSynchronizer(request.sender)
          .currentSnapshotApproximation
          .futureValueUS
      SignedContent
        .create(
          cryptoSnapshot.pureCrypto,
          cryptoSnapshot,
          request,
          timestampOfSigningKey = Some(cryptoSnapshot.ipsSnapshot.timestamp),
          signingTimestampOverrides =
            None, // not needed for unit tests; session signing keys disabled
          HashPurpose.SubmissionRequestSignature,
          testedProtocolVersion,
        )
        .futureValueUS
        .value
    }

    def close(): Unit = {
      sequencer.close()
      LifeCycle.toCloseableActorSystem(actorSystem, logger, timeouts).close()
    }
  }

  override protected type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env
    complete {
      super.withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  protected var clock: Clock = _

  protected def createClock(): Clock = new SimClock(loggerFactory = loggerFactory)

  protected def simClockOrFail(clock: Clock): SimClock =
    clock match {
      case simClock: SimClock => simClock
      case _ =>
        fail(
          "This test case is only compatible with SimClock for `clock` and `driverClock` fields"
        )
    }

  protected def psid: PhysicalSynchronizerId = DefaultTestIdentities.physicalSynchronizerId
  protected def mediatorId: MediatorId = m1
  protected def sequencerId: SequencerId = DefaultTestIdentities.sequencerId

  protected def createSequencer(crypto: SynchronizerCryptoClient)(implicit
      materializer: Materializer
  ): CantonSequencer

  protected def supportAggregation: Boolean

  protected def defaultExpectedTrafficReceipt: Option[TrafficReceipt]

  private def sendWithElapsedMaxSequencingTime(
      sequencer: Sequencer,
      request: SignedContent[SubmissionRequest],
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] =
    // Only block orderers implement the max-sequencing-time check on the write side
    // Since this method is only used for testing the read side, we bypass the write-side check for block sequencers.
    sequencer.orderer match {
      case None => sequencer.sendAsyncSigned(request)
      case Some(orderer) =>
        orderer.send(request).mapK(FutureUnlessShutdown.outcomeK).leftWiden[CantonBaseError]
    }

  private def createMediatorRequest(
      members: Seq[(Member, MessageId)],
      envelopeContent: Seq[(String, Seq[Member])],
      maxSequencingTime: CantonTimestamp,
  )(implicit env: Env) = {
    import env.*
    val aggregationRule =
      AggregationRule.activeMediators(
        MediatorGroupIndex.zero,
        testedProtocolVersion,
      )

    val envelopes = envelopeContent.map {
      case (content, first +: rest) =>
        ClosedUncompressedEnvelope.create(
          ByteString.copyFromUtf8(content),
          Recipients.cc(first, rest*),
          Seq.empty,
          testedProtocolVersion,
        )
      case _ => fail("need at least one recipient for each envelope")
    }

    def mkRequest(
        sender: Member,
        messageId: MessageId,
        envelopes: List[ClosedUncompressedEnvelope],
    ): SubmissionRequest =
      SubmissionRequest.tryCreate(
        sender,
        messageId,
        Batch(envelopes, testedProtocolVersion),
        maxSequencingTime,
        topologyTimestamp = None,
        Some(aggregationRule),
        Option.empty[SequencingSubmissionCost],
        testedProtocolVersion,
      )

    (
      aggregationRule,
      MonadUtil.sequentialTraverse(members) { case (member, messageId) =>
        val client = topologyFactory.forOwnerAndSynchronizer(member, psid)
        for {
          signedEnvelopes <- envelopes.parTraverse(signEnvelope(client, _))
        } yield (sign(mkRequest(member, messageId, signedEnvelopes.toList)), signedEnvelopes)
      },
    )

  }

  protected def runSequencerApiTests(): Unit = {
    "The sequencers" should {
      "send a batch to one recipient" in { env =>
        import env.*
        val messageContent = "hello"
        val sender = p7.member
        val recipients = Recipients.cc(sender)

        val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

        for {
          _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFail("Sent async")
          messages <- readForMembers(List(sender), sequencer)
        } yield {
          val details = EventDetails(
            previousTimestamp = None,
            to = sender,
            messageId = Some(request.messageId),
            trafficReceipt = defaultExpectedTrafficReceipt,
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
          messages <- loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.INFO))(
            sendWithElapsedMaxSequencingTime(sequencer, sign(request))
              .valueOrFail("sent async")
              .flatMap(_ =>
                readForMembers(
                  List(sender),
                  sequencer,
                  timeout = 5.seconds, // We don't need the full timeout here
                )
              ),
            // TODO(#25250): was `forAll`; tighten these log checks back once the BFT sequencer logs are more stable
            forAtLeast(1, _) { entry =>
              entry.message should ((include(suppressedMessageContent) and {
                include(ExceededMaxSequencingTime.id) or include regex "Send of .* at"
              }) or include("Detected new members without sequencer counter") or
                include regex "Creating .* at block height None" or
                include("Received `Start` message") or
                include("Completing init") or
                include("Subscribing to block source from") or
                include("Re-using the existing sequencer storage for BFT ordering") or
                include("Advancing sim clock") or
                (include("Creating ForkJoinPool with parallelism") and include(
                  "to avoid starvation"
                )) or
                include("Started gathering segment status") or
                include("Broadcasting epoch status") or
                include("Scheduling pruning in 1 hour") or
                include("Got a retransmission request from"))
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
          _ <- sequencer.sendAsyncSigned(sign(request1)).valueOrFail("Sent async #1")
          messages <- loggerFactory.assertLogsSeq(
            SuppressionRule.LevelAndAbove(Level.INFO) &&
              SuppressionRule.forLogger[BlockChunkProcessor]
          )(
            sendWithElapsedMaxSequencingTime(sequencer, sign(request2))
              .valueOrFail("sent async")
              .flatMap(_ => readForMembers(List(sender), sequencer)),
            forAll(_) { entry =>
              // block update generator will log every send
              entry.message should (include("Detected new members without sequencer counter") or
                include(ExceededMaxSequencingTime.id) or
                include regex s"Send of .*$suppressedMessageContent.* at " or
                include regex s"Send of .*$normalMessageContent.* at")
            },
          )
        } yield {
          val details = EventDetails(
            previousTimestamp = None,
            to = sender,
            messageId = Some(request1.messageId),
            trafficReceipt = defaultExpectedTrafficReceipt,
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
        val readFor: List[Member] = recipients.allRecipients.collect {
          case MemberRecipient(member) =>
            member
        }.toList

        val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

        val expectedDetailsForMembers = readFor.map { member =>
          EventDetails(
            previousTimestamp = None,
            to = member,
            messageId = Option.when(member == sender)(request.messageId),
            if (member == sender) defaultExpectedTrafficReceipt else None,
            EnvelopeDetails(messageContent, recipients.forMember(member, Set.empty).value),
          )
        }

        for {
          _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFail("Sent async")
          reads <- readForMembers(readFor, sequencer)
        } yield {
          checkMessages(expectedDetailsForMembers, reads)
        }
      }

      def testAggregation: Boolean =
        supportAggregation && testedProtocolVersion > ProtocolVersion.v34 // 3.7 is no longer supporting pv34, so tests won't run on it

      "aggregate submission requests" onlyRunWhen testAggregation in { env =>
        import env.*

        val messageContent = "aggregatable-message"
        val messageId1 = MessageId.tryCreate(messageContent + "-m1")
        val messageId2 = MessageId.tryCreate(messageContent + "-m2")

        val (_, requestsF) = createMediatorRequest(
          Seq(
            (m1, messageId1),
            (m2, messageId2),
          ),
          // TODO(i10412): See above
          Seq((messageContent, Seq(p10))),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
        )(env)

        for {
          requests <- requestsF
          (request1, envelopes1) = requests.headOption.value
          (request2, envelopes2) = requests(1)
          _ <- sequencer.sendAsyncSigned(request1).valueOrFail("Sent async for mediator1")
          reads1 <- readForMembers(Seq(m1), sequencer)
          _ <- sequencer.sendAsyncSigned(request2).valueOrFail("Sent async for mediator2")
          reads2 <- readForMembers(Seq(m2), sequencer)
          reads3 <- readForMembers(Seq(p10), sequencer)
        } yield {
          // m1 gets the receipt immediately
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = m1,
                messageId = Some(messageId1),
                defaultExpectedTrafficReceipt,
              )
            ),
            reads1,
          )
          // p9 gets the receipt only
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = m2,
                messageId = Some(messageId2),
                defaultExpectedTrafficReceipt,
              )
            ),
            reads2,
          )
          // p10 gets the message
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = p10,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(
                  messageContent,
                  Recipients.cc(p10),
                  signatures = envelopes1.flatMap(_.signatures) ++ envelopes2.flatMap(_.signatures),
                ),
              )
            ),
            reads3,
          )
        }
      }

      "bounce on write path aggregate submissions with maxSequencingTime exceeding bound" onlyRunWhen testAggregation in {
        env =>
          import env.*

          val messageContent = "bounce-write-path-message"
          val aggregationRule =
            AggregationRule.activeMediators(
              MediatorGroupIndex.zero,
              testedProtocolVersion,
            )
          val request1 = createSendRequest(
            m1,
            messageContent,
            Recipients.cc(p10),
            maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofMinutes(10)),
            aggregationRule = Some(aggregationRule),
          )
          val request2 = request1.copy(
            sender = m2,
            messageId = MessageId.fromUuid(new UUID(1, 2)),
            maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofMinutes(-10)),
          )

          for {
            tooFarInTheFuture <- sequencer
              .sendAsyncSigned(sign(request1))
              .leftOrFailShutdown(
                "A sendAsync of submission with maxSequencingTime too far in the future"
              )
            inThePast <- sequencer
              .sendAsyncSigned(sign(request2))
              .leftOrFailShutdown(
                "A sendAsync of submission with maxSequencingTime in the past"
              )
          } yield {
            tooFarInTheFuture.code.id shouldBe SequencerErrors.MaxSequencingTimeTooFar.id
            tooFarInTheFuture.cause should (
              include("is too far in the future") and
                include("Max sequencing time")
            )

            inThePast.code.id shouldBe ExceededMaxSequencingTime.id
            inThePast.cause should (
              include("The sequencer time") and
                include("has exceeded by") and
                include("the max-sequencing-time of the send request") and
                include("Estimation for message id")
            )
          }
      }

      "bounce on write path aggregate submissions dedup" onlyRunWhen testAggregation in { env =>
        import env.*

        val messageContent = "bounce-sender-dedup-message"
        // TODO(i10412): See above
        val aggregationRule =
          AggregationRule.senderDedup(testedProtocolVersion)

        val request1 = createSendRequest(
          p6,
          messageContent,
          Recipients.cc(p10),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofMinutes(1)),
          aggregationRule = Some(aggregationRule),
        )

        val signed = sign(request1)
        for {
          // first succeeds
          _ <- sequencer
            .sendAsyncSigned(signed)
            .valueOrFail("Sent async for participant1")
          _ <- readForMembers(Seq(p6), sequencer)
          // second bounces
          deduped <- sequencer
            .sendAsyncSigned(signed)
            .leftOrFail("A sendAsync of duplicate aggregation submission")
        } yield {
          deduped.code.id shouldBe SequencerErrors.AggregateSubmissionAlreadySent.id
          succeed
        }
      }

      "bounce on read path aggregate submissions with maxSequencingTime exceeding bound" onlyRunWhen testAggregation in {
        env =>
          import env.*
          sequencer.discard // This is necessary to init the lazy val in the Env before manipulating the clocks

          val messageContent = "bounce-read-path-message"
          val aggregationRule =
            AggregationRule.activeMediators(
              MediatorGroupIndex.zero,
              testedProtocolVersion,
            )

          val request1 = createSendRequest(
            p6,
            messageContent,
            Recipients.cc(p10),
            // Note:  write side clock is at 100s, which lets the request pass,
            //        read side clock is at 0s, which should produce an error due to the MST bound at 6m(=360s)
            maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(370)),
            aggregationRule = Some(aggregationRule),
          )

          // Only block orderers implement aggregation, so this should always be defined.
          val orderer = sequencer.orderer.value

          for {
            // We're checking the read path, so we can skip the write path of the sequencer and submit directly to the orderer
            _ <- orderer.send(sign(request1)).valueOrFail("Sent async for participant1")
            reads3 <- readForMembers(Seq(p6), sequencer)
          } yield {
            checkRejection(reads3, p6, request1.messageId, defaultExpectedTrafficReceipt) {
              case SequencerErrors.MaxSequencingTimeTooFar(reason) =>
                reason should (
                  include(s"Max sequencing time") and
                    include("is too far in the future")
                )
            }
          }
      }

      "aggregate signatures" onlyRunWhen testAggregation in { env =>
        import env.*

        val messageId1 = MessageId.tryCreate(s"request1")
        val messageId2 = MessageId.tryCreate(s"request2")
        val messageId3 = MessageId.tryCreate(s"request3")
        val content1 = "envelope1-to-sign"
        val recipients1 = Seq(p1, p3)
        val recipients1CC = Recipients.cc(p1, p3)
        val recipients2 = Seq(p2, p3)
        val recipients2CC = Recipients.cc(p2, p3)
        val content2 = "envelope2-to-sign"
        val (_, requestsF) = createMediatorRequest(
          Seq((m1, messageId1), (m2, messageId2), (m3, messageId3)),
          Seq((content1, recipients1), (content2, recipients2)),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
        )(env)

        val lockP = Promise[Unit]()

        for {
          requests <- requestsF
          (request1, envs1) = requests.headOption.value
          (request2, envs2) = requests(1)
          (request3, envs3) = requests(2)
          _ <- sequencer
            .sendAsyncSigned(request1)
            .valueOrFail("Sent async for m1")
          read1 <- readForMembers(Seq(m1), sequencer)
          _ = sequencer.applyPostProcessingLockForTesting(lockP.future)
          _ <- sequencer
            .sendAsyncSigned(request2)
            .valueOrFail("Sent async for m2")
          _ <- sequencer
            .sendAsyncSigned(request3)
            .valueOrFail("Sent async for m3")
          _ = lockP.success(())
          readP <- readForMembers(Seq(p1, p2, p3), sequencer)
          read2 <- readForMembers(Seq(m2), sequencer)
          read3 <- readForMembers(Seq(m3), sequencer)

          // if m3 sends again after processing, he'll see the already sent error
          _ <-
            if (testAggregation)
              sequencer
                .sendAsyncSigned(request3)
                .leftOrFail("Send async should fail with already sent")
            else FutureUnlessShutdown.unit
        } yield {
          // expect receipt for m1 and m2
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = m1,
                messageId = Some(messageId1),
                trafficReceipt = defaultExpectedTrafficReceipt,
              )
            ),
            read1,
          )
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = m2,
                messageId = Some(messageId2),
                trafficReceipt = defaultExpectedTrafficReceipt,
              )
            ),
            read2,
          )
          // expect envelopes for p1, p2, p3
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = p1,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(content1, recipients1CC, envs1(0).signatures ++ envs2(0).signatures),
              ),
              EventDetails(
                previousTimestamp = None,
                to = p2,
                messageId = None,
                trafficReceipt = defaultExpectedTrafficReceipt,
                EnvelopeDetails(content2, recipients2CC, envs1(1).signatures ++ envs2(1).signatures),
              ),
              EventDetails(
                previousTimestamp = None,
                to = p3,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(
                  content1,
                  recipients1CC,
                  envs1(0).signatures ++ envs2(0).signatures,
                ),
                EnvelopeDetails(content2, recipients2CC, envs1(1).signatures ++ envs2(1).signatures),
              ),
            ),
            readP,
          )

          checkRejection(read3, m3, messageId3, defaultExpectedTrafficReceipt) {
            case SequencerErrors.AggregateSubmissionAlreadySent(reason) =>
              reason should (
                include(s"The aggregatable request with aggregation ID") and
                  include("was previously delivered at")
              )
            case SequencerErrors.AggregateSubmissionAlreadySentV2(reason) =>
              reason should (
                include(s"The aggregatable request with aggregation ID") and
                  include("was previously delivered at")
              )
          }
        }
      }

      "prevent aggregation stuffing" onlyRunWhen testAggregation in { env =>
        import env.*

        val messageId1 = MessageId.tryCreate(s"request1")
        val messageId2 = MessageId.tryCreate(s"request2")
        val messageId3 = MessageId.tryCreate(s"request3")
        val messageContent = "aggregatable-message-stuffing"
        val recipients = Seq(p1)
        val (_, requestsF) = createMediatorRequest(
          Seq((m1, messageId1), (m1, messageId2), (m2, messageId3)),
          Seq((messageContent, recipients)),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
        )(env)

        for {
          requests <- requestsF
          (request1, envelopes1) = requests.headOption.value
          (request2, _) = requests(1)
          (request3, envelopes3) = requests(2)
          _ <- sequencer.sendAsyncSigned(request1).valueOrFail("Sent async for mediator1")
          reads1 <- readForMembers(Seq(m1), sequencer)
          _ <- sequencer.sendAsyncSigned(request2).valueOrFail("Sent async stuffing for mediator1")
          reads1a <- readForMembers(
            Seq(m1),
            sequencer,
            startTimestamp = firstEventTimestamp(m1)(reads1).map(_.immediateSuccessor),
          )
          // m2 can still continue and finish the aggregation
          _ <- sequencer
            .sendAsyncSigned(request3)
            .valueOrFail("Sent async for mediator2")
          reads2 <- readForMembers(Seq(m2), sequencer)
          readsp1 <- readForMembers(Seq(p1), sequencer)
        } yield {
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = m1,
                messageId = Some(messageId1),
                trafficReceipt = defaultExpectedTrafficReceipt,
              )
            ),
            reads1,
          )
          checkRejection(reads1a, m1, messageId2, defaultExpectedTrafficReceipt) {
            case SequencerErrors.AggregateSubmissionStuffing(reason) =>
              reason should include(
                s"The sender $m1 previously contributed to the aggregatable submission with ID"
              )
          }
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = m2,
                messageId = Some(messageId3),
                trafficReceipt = defaultExpectedTrafficReceipt,
              )
            ),
            reads2,
          )
          val deliveredEnvelopeDetails = EnvelopeDetails(
            messageContent,
            Recipients.ofSet(recipients.toSet).value,
            // Only the first signature from m1 is included
            envelopes1.flatMap(_.signatures) ++ envelopes3.flatMap(_.signatures),
          )
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = p1,
                messageId = None,
                trafficReceipt = defaultExpectedTrafficReceipt,
                deliveredEnvelopeDetails,
              )
            ),
            readsp1,
          )
        }
      }

      "require the sender to be registered" onlyRunWhen testAggregation in { env =>
        import env.*

        val aggregationRule = AggregationRule.senderDedup(testedProtocolVersion)

        val request = sign(
          createSendRequest(
            p1,
            "unregistered-sender",
            Recipients.cc(p16),
            aggregationRule = Some(aggregationRule),
            maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
          )
        )
        val spoofed = request.copy(content = request.content.copy(sender = p16))
        for {
          error <- sequencer.sendAsyncSigned(spoofed).leftOrFailShutdown("Sent async")
        } yield {
          error.code.id shouldBe SequencerErrors.SubmissionRequestRefused.id
          error.cause should (
            include("There are no valid keys for") and
              include(p16.toString)
          )
        }
      }

      "prevent non-eligible senders from contributing" onlyRunWhen testAggregation in { env =>
        import env.*

        val messageId1 = MessageId.tryCreate(s"request1")
        val messageId2 = MessageId.tryCreate(s"request2")
        val messageId3 = MessageId.tryCreate(s"request3")
        val messageContent = "aggregatable-message"
        val recipients = Seq(p2)
        val (_, requestsF) = createMediatorRequest(
          Seq((m1, messageId1), (p1, messageId2), (m2, messageId3)),
          Seq((messageContent, recipients)),
          maxSequencingTime = CantonTimestamp.Epoch.add(Duration.ofSeconds(60)),
        )(env)

        for {
          requests <- requestsF
          (requestFromM1, envelopes1) = requests.headOption.value
          (requestFromP1, _) = requests(1)
          (requestFromM2, envelopes3) = requests(2)
          _ <- sequencer
            .sendAsyncSigned(requestFromM1)
            .valueOrFail("Sent async for mediator1")
          readsForM1 <- readForMembers(Seq(m1), sequencer)
          readsForP1 <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            for {
              _ <- sequencer
                .sendAsyncSigned(requestFromP1)
                .valueOrFail("Sent async for non-eligible participant4")
              reads <- readForMembers(Seq(p1), sequencer, timeout = 5.seconds)
            } yield reads,
            LogEntry.assertLogSeq(
              Seq(
                (
                  // just emitted as a warning message with details, while the rejection
                  // omits details
                  _.warningMessage should include(
                    if (testedProtocolVersion > ProtocolVersion.v36)
                      SequencerErrors.SubmissionRequestMalformedAndRejected.id
                    else SubmissionRequestMalformed.id
                  ),
                  "p1's submission generates an alarm and a rejection",
                )
              )
            ),
          )

          _ <- sequencer
            .sendAsyncSigned(requestFromM2)
            .valueOrFail("Sent async for mediator2")
          readsForM2 <- readForMembers(Seq(m2), sequencer)
          readsForP2 <- readForMembers(Seq(p2), sequencer)
        } yield {
          // m1 gets the receipt immediately
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = m1,
                messageId = Some(messageId1),
                trafficReceipt = defaultExpectedTrafficReceipt,
              )
            ),
            readsForM1,
          )

          // m2 gets the receipt only
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = m2,
                messageId = Some(messageId3),
                trafficReceipt = defaultExpectedTrafficReceipt,
              )
            ),
            readsForM2,
          )

          // p2 gets the message
          checkMessages(
            Seq(
              EventDetails(
                previousTimestamp = None,
                to = p2,
                messageId = None,
                trafficReceipt = None,
                EnvelopeDetails(
                  messageContent,
                  Recipients.cc(p2),
                  envelopes1.flatMap(_.signatures) ++ envelopes3.flatMap(_.signatures),
                ),
              )
            ),
            readsForP2,
          )

          // p1 gets a rejection
          if (testedProtocolVersion > ProtocolVersion.v36)
            checkRejection(readsForP1, p1, messageId2, defaultExpectedTrafficReceipt) {
              case SequencerErrors.SubmissionRequestMalformedAndRejected(reason)
                  if testedProtocolVersion > ProtocolVersion.v36 =>
                reason should include(s"Validation of aggregation rule")
            }
          else {
            // p1 gets nothing (message is discarded before pv37)
            checkMessages(Seq(), readsForP1)
          }

        }

      }

      "require the member to be enabled to send/read" in { env =>
        import env.*

        val messageContent = "message-from-disabled-member"
        val sender = p7.member
        val recipients = Recipients.cc(sender)

        val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)

        for {
          // Need to send first request and wait for it to be processed to get the member registered in BS
          _ <- sequencer.sendAsyncSigned(sign(request)).valueOrFail("Send async failed")
          _ <- readForMembers(Seq(p7), sequencer)
          _ <- sequencer.disableMember(sender).valueOrFail("Disabling member failed")
          sendError <- sequencer
            .sendAsyncSigned(sign(request))
            .leftOrFail("Send successful, expected error")
          subscribeError <- sequencer
            .read(sender, timestampInclusive = None)
            .leftOrFail("Read successful, expected error")
        } yield {
          sendError.code.id shouldBe SequencerErrors.SubmissionRequestRefused.id
          sendError.cause should (
            include("is disabled at the sequencer") and
              include(p7.toString)
          )
          subscribeError should matchPattern {
            case CreateSubscriptionError.MemberDisabled(member) if member == sender =>
          }
        }
      }
    }
  }
}

trait SequencerApiTestUtils
    extends FixtureAsyncWordSpec
    with ProtocolVersionChecksFixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext {
  protected def readForMembers(
      members: Seq[Member],
      sequencer: CantonSequencer,
      // up to 60 seconds needed because Besu is very slow on CI
      timeout: FiniteDuration = 60.seconds,
      startTimestamp: Option[CantonTimestamp] = None,
  )(implicit
      materializer: Materializer
  ): FutureUnlessShutdown[Seq[(Member, SequencedSerializedEvent)]] =
    // TODO(#33650) – replace with unboundedTraverseFilter; OK because members are statically bounded to a small number
    members
      .parTraverseFilter { member =>
        for {
          source <- valueOrFail(
            sequencer.read(member, startTimestamp)
          )(
            s"Read for $member"
          )
          events <- FutureUnlessShutdown.outcomeF(
            source
              // hard-coding that we only expect 1 event per member
              .take(1)
              .takeWithin(timeout)
              .runWith(Sink.seq)
              .map {
                case Seq(Right(e)) => Some((member, e))
                case Seq(Left(err)) => fail(s"Test does not expect tombstones: $err")
                case _ =>
                  // We read no messages for a member when we expected some
                  None
              }
          )
        } yield events
      }

  protected def firstEventTimestamp(forMember: Member)(
      reads: Seq[(Member, SequencedSerializedEvent)]
  ): Option[CantonTimestamp] =
    reads.collectFirst { case (`forMember`, event) => event.timestamp }

  case class EnvelopeDetails(
      content: String,
      recipients: Recipients,
      signatures: Seq[Signature] = Seq.empty,
  )

  case class EventDetails(
      previousTimestamp: Option[CantonTimestamp],
      to: Member,
      messageId: Option[MessageId],
      trafficReceipt: Option[TrafficReceipt],
      envs: EnvelopeDetails*
  )

  protected def createSendRequest(
      sender: Member,
      messageContent: String,
      recipients: Recipients,
      maxSequencingTime: CantonTimestamp = CantonTimestamp.MaxValue,
      aggregationRule: Option[AggregationRule] = None,
      sequencingSubmissionCost: Batch[ClosedUncompressedEnvelope] => Option[
        SequencingSubmissionCost
      ] = _ => None,
      signatures: Seq[Signature] = Seq.empty,
  ): SubmissionRequest = {
    val envelope1 = TestingEnvelope(messageContent, recipients, signatures)
    val batch = Batch(List(envelope1.toClosedUncompressedEnvelope), testedProtocolVersion)
    val messageId = MessageId.tryCreate(s"thisisamessage: $messageContent")
    SubmissionRequest.tryCreate(
      sender,
      messageId,
      batch,
      maxSequencingTime,
      None,
      aggregationRule,
      sequencingSubmissionCost(batch),
      testedProtocolVersion,
    )
  }

  protected def checkMessages(
      expectedMessages: Seq[EventDetails],
      receivedMessages: Seq[(Member, SequencedSerializedEvent)],
      expectEnvelopes: Boolean = true,
  ): Assertion = {

    receivedMessages.length shouldBe expectedMessages.length

    val sortExpected = expectedMessages.sortBy(e => e.to)
    val sortReceived = receivedMessages.sortBy { case (member, _) => member }

    forAll(sortReceived.zip(sortExpected)) { case ((member, message), expectedMessage) =>
      withClue(s"Member mismatch")(member shouldBe expectedMessage.to)

      withClue(s"Message id is wrong") {
        expectedMessage.messageId.foreach(_ =>
          message.signedEvent.content match {
            case Deliver(_, _, _, messageId, _, _, _) =>
              messageId shouldBe expectedMessage.messageId
            case _ => fail(s"Expected a deliver $expectedMessage, received error $message")
          }
        )
      }

      val event = message.signedEvent.content

      event match {
        case Deliver(_, _, _, messageIdO, batch, _, trafficReceipt) =>
          if (expectEnvelopes) {
            withClue(s"Received the wrong number of envelopes for recipient $member") {
              batch.envelopes.length shouldBe expectedMessage.envs.length
            }
          }

          if (messageIdO.isDefined) {
            withClue(s"Received incorrect traffic receipt $member") {
              trafficReceipt shouldBe expectedMessage.trafficReceipt
            }
          } else {
            withClue(s"Received a traffic receipt for $member in an event without messageId") {
              trafficReceipt shouldBe empty
            }
          }

          forAll(batch.envelopes.zip(expectedMessage.envs)) { case (got, wanted) =>
            val gotUncompressed = got.toClosedUncompressedEnvelopeUnsafe

            gotUncompressed.recipients shouldBe wanted.recipients
            gotUncompressed.bytes shouldBe ByteString.copyFromUtf8(wanted.content)
            gotUncompressed.signatures shouldBe wanted.signatures
          }

        case _ => fail(s"Event $event is not a deliver")
      }
    }
  }

  def checkRejection(
      got: Seq[(Member, SequencedSerializedEvent)],
      sender: Member,
      expectedMessageId: MessageId,
      expectedTrafficReceipt: Option[TrafficReceipt],
  )(assertReason: PartialFunction[Status, Assertion]): Assertion =
    got match {
      case Seq((`sender`, event)) =>
        event.signedEvent.content match {
          case DeliverError(
                _previousTimestamp,
                _timestamp,
                _synchronizerId,
                messageId,
                reason,
                trafficReceipt,
              ) =>
            messageId shouldBe expectedMessageId
            assertReason(reason)
            trafficReceipt shouldBe expectedTrafficReceipt

          case _ => fail(s"Expected a deliver error, but got $event")
        }
      case _ => fail(s"Read wrong events for $sender: $got")
    }

  def signEnvelope(
      crypto: SynchronizerCryptoClient,
      envelope: ClosedUncompressedEnvelope,
  ): FutureUnlessShutdown[ClosedUncompressedEnvelope] = {
    val hash = crypto.pureCrypto.digest(HashPurpose.SignedProtocolMessageSignature, envelope.bytes)
    crypto.currentSnapshotApproximation.futureValueUS
      .sign(
        hash,
        SigningKeyUsage.ProtocolOnly,
        None,
      )
      .map(sig => envelope.copy(signatures = Seq(sig)))
      .valueOrFail(s"Failed to sign $envelope")
  }

  case class TestingEnvelope(
      content: String,
      override val recipients: Recipients,
      signatures: Seq[Signature] = Seq.empty,
  ) extends Envelope[String] {

    /** Closes the envelope by serializing the contents */
    def toClosedUncompressedEnvelope: ClosedUncompressedEnvelope =
      ClosedUncompressedEnvelope.create(
        ByteString.copyFromUtf8(content),
        recipients,
        signatures,
        testedProtocolVersion,
      )

    override def toClosedUncompressedEnvelopeResult: ParsingResult[ClosedUncompressedEnvelope] =
      toClosedUncompressedEnvelope.asRight

    override def toClosedCompressedEnvelope(
        algo: com.digitalasset.canton.util.CompressionAlgo
    ): ClosedCompressedEnvelope =
      toClosedUncompressedEnvelope.toClosedCompressedEnvelope(algo)

    override def forRecipient(
        member: Member,
        groupAddresses: Set[GroupRecipient],
    ): Option[Envelope[String]] =
      recipients
        .forMember(member, groupAddresses)
        .map(recipients => TestingEnvelope(content, recipients))

    override protected def pretty: Pretty[TestingEnvelope] = adHocPrettyInstance
  }

  /** Registers all the members present in the topology snapshot with the sequencer. Used for unit
    * testing sequencers. During the normal sequencer operation members are registered via topology
    * subscription or sequencer startup in SequencerRuntime.
    */
  @nowarn("cat=deprecation")
  def registerAllTopologyMembers(headSnapshot: TopologySnapshot, sequencer: Sequencer): Unit =
    (for {
      allMembers <- EitherT
        .right[Sequencer.RegisterError](headSnapshot.knownMembers())
      _ <- allMembers.toSeq
        .parTraverse_ { member =>
          for {
            firstKnownAtO <- EitherT
              .right(headSnapshot.memberFirstKnownAt(member))
            res <- firstKnownAtO match {
              case Some((_, firstKnownAtEffectiveTime)) =>
                sequencer
                  .registerMemberInternal(member, firstKnownAtEffectiveTime.value)
              case None =>
                ErrorUtil.invalidState(
                  s"Member $member has no first known at time, despite being in the topology"
                )
            }
          } yield res
        }
    } yield ()).futureValueUS
}
