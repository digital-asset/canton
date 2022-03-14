// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.{EitherT, NonEmptyList, NonEmptySet}
import cats.syntax.foldable._
import cats.syntax.option._
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.governance.ParticipantAuditor
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.service.SubscriptionPool.PoolClosed
import com.digitalasset.canton.topology._
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, GenesisSequencerCounter}
import com.google.protobuf.ByteString
import io.grpc.Status.Code._
import io.grpc.StatusException
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import monocle.macros.syntax.lens._
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}

import scala.collection.mutable
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Null"))
class GrpcSequencerServiceTest extends FixtureAsyncWordSpec with BaseTest {
  type Subscription = GrpcManagedSubscription

  sealed trait StreamItem
  case class StreamNext[A](value: A) extends StreamItem
  case class StreamError(t: Throwable) extends StreamItem
  object StreamComplete extends StreamItem

  class MockStreamObserver
      extends StreamObserver[v0.SubscriptionResponse]
      with RecordStreamObserverItems
  class MockServerStreamObserver
      extends ServerCallStreamObserver[v0.SubscriptionResponse]
      with RecordStreamObserverItems {
    override def isCancelled: Boolean = ???
    override def setOnCancelHandler(onCancelHandler: Runnable): Unit = ???
    override def setCompression(compression: String): Unit = ???
    override def isReady: Boolean = ???
    override def setOnReadyHandler(onReadyHandler: Runnable): Unit = ???
    override def disableAutoInboundFlowControl(): Unit = ???
    override def request(count: Int): Unit = ???
    override def setMessageCompression(enable: Boolean): Unit = ???
  }

  trait RecordStreamObserverItems {
    this: StreamObserver[v0.SubscriptionResponse] =>

    val items: mutable.Buffer[StreamItem] = mutable.Buffer[StreamItem]()

    override def onNext(value: v0.SubscriptionResponse): Unit = items += StreamNext(value)
    override def onError(t: Throwable): Unit = items += StreamError(t)
    override def onCompleted(): Unit = items += StreamComplete
  }

  class MockSubscription extends CloseNotification with AutoCloseable {
    override def close(): Unit = {}
  }

  private val participant = DefaultTestIdentities.participant1
  private val unauthenticatedMember = UnauthenticatedMemberId.tryCreate(participant.uid.namespace)

  class Environment(member: Member) extends Matchers {
    val sequencer: Sequencer = mock[Sequencer]
    when(sequencer.sendAsync(any[SubmissionRequest])(anyTraceContext))
      .thenReturn(EitherT.rightT[Future, SendAsyncError](()))
    val cryptoApi: DomainSyncCryptoClient =
      TestingIdentityFactory(loggerFactory).forOwnerAndDomain(member)
    val subscriptionPool: SubscriptionPool[Subscription] =
      mock[SubscriptionPool[GrpcManagedSubscription]]
    val sequencerSubscriptionFactory = mock[DirectSequencerSubscriptionFactory]

    val service =
      new GrpcSequencerService(
        sequencer,
        DomainTestMetrics.sequencer,
        loggerFactory,
        ParticipantAuditor.noop,
        new AuthenticationCheck.MatchesAuthenticatedMember {
          override def lookupCurrentMember(): Option[Member] = member.some
        },
        subscriptionPool,
        sequencerSubscriptionFactory,
        NonNegativeInt.tryCreate(5),
        NonNegativeInt.tryCreate(1000),
        timeouts,
      )
  }

  override type FixtureParam = Environment

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Environment(participant)
    withFixture(test.toNoArgAsyncTest(env))
  }

  def mkSubmissionRequest(
      batch: Batch[ClosedEnvelope],
      sender: Member,
  ): SubmissionRequest = {
    val id = MessageId.tryCreate("messageId")
    SubmissionRequest(
      sender,
      id,
      isRequest = true,
      batch,
      CantonTimestamp.MaxValue,
      None,
    )
  }

  "send" should {

    val content = ByteString.copyFromUtf8("123")
    val defaultRequest: SubmissionRequest = {
      val sender: Member = participant
      val recipient = DefaultTestIdentities.participant2
      mkSubmissionRequest(Batch(List(ClosedEnvelope(content, Recipients.cc(recipient)))), sender)
    }

    def sendAndSucceed(requestP: v0.SubmissionRequest)(implicit
        env: Environment
    ): Future[Assertion] =
      env.service.sendAsync(requestP).map { responseP =>
        val response = SendAsyncResponse.fromProtoV0(responseP)
        response.value.error shouldBe None
      }

    def sendAndCheckError(
        requestP: v0.SubmissionRequest,
        assertion: PartialFunction[SendAsyncError, Assertion],
        authenticated: Boolean = true,
    )(implicit env: Environment): Future[Assertion] = {
      (if (authenticated) env.service.sendAsync(requestP)
       else env.service.sendAsyncUnauthenticated(requestP)).map { responseP =>
        val response = SendAsyncResponse.fromProtoV0(responseP).value
        assertion(response.error.value)
      }
    }

    "reject empty request" in { implicit env =>
      val requestP = v0.SubmissionRequest("", "", false, None, None, None)
      loggerFactory.assertLogs(
        {
          sendAndCheckError(
            requestP,
            { case SendAsyncError.RequestInvalid(message) =>
              message should startWith("Unable to parse sender:")
            },
          )
        },
        _.warningMessage should startWith(
          "Request '' from '[sender-not-set]' is invalid: Unable to parse sender:"
        ),
      )
    }

    "reject envelopes with empty content" in { implicit env =>
      val request = defaultRequest
        .focus(_.batch.envelopes)
        .modify(_.map(_.focus(_.bytes).replace(ByteString.EMPTY)))

      loggerFactory.assertLogs(
        sendAndCheckError(
          request.toProtoV0(ProtocolVersion.latestForTest),
          { case SendAsyncError.RequestInvalid(message) =>
            message shouldBe "Batch contains envelope without content."
          },
        ),
        _.warningMessage should endWith("is invalid: Batch contains envelope without content."),
      )
    }

    "reject envelopes with invalid sender" in { implicit env =>
      val requestP =
        defaultRequest.toProtoV0(ProtocolVersion.latestForTest).focus(_.sender).modify {
          case "" => fail("sender should be set")
          case _sender =>
            "THISWILLFAIL"
        }

      loggerFactory.assertLogs(
        {
          sendAndCheckError(
            requestP,
            { case SendAsyncError.RequestInvalid(message) =>
              message should startWith("Unable to parse sender:")
            },
          )
        },
        _.warningMessage should startWith(
          "Request 'messageId' from 'THISWILLFAIL' is invalid: Unable to parse sender:"
        ),
      )
    }

    "reject large messages" in { implicit env =>
      val bigEnvelope =
        ClosedEnvelope(
          ByteString.copyFromUtf8(scala.util.Random.nextString(5000)),
          Recipients.cc(participant),
        )
      val request = defaultRequest.focus(_.batch.envelopes).replace(List(bigEnvelope))

      loggerFactory.assertLogs(
        {
          sendAndCheckError(
            request.toProtoV0(ProtocolVersion.latestForTest),
            { case SendAsyncError.RequestRefused(message) =>
              message should fullyMatch regex "Request from '.*' of size \\(.* bytes\\) is exceeding maximum size \\(1000 bytes\\)\\."
            },
          )
        },
        _.warningMessage should include regex "Request from '.*' of size \\(.* bytes\\) is exceeding maximum size \\(1000 bytes\\)\\.",
      )
    }

    "reject unauthorized authenticated participant" in { implicit env =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(DefaultTestIdentities.participant2)

      loggerFactory.assertLogs(
        {
          sendAndCheckError(
            request.toProtoV0(ProtocolVersion.latestForTest),
            { case SendAsyncError.RequestRefused(message) =>
              message should (include("is not authorized to send:")
                and include("just tried to use sequencer on behalf of"))
            },
          )
        },
        _.warningMessage should (include("is not authorized to send:")
          and include("just tried to use sequencer on behalf of")),
      )
    }

    "reject authenticated member that uses unauthenticated send" in { implicit env =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(DefaultTestIdentities.participant1)

      loggerFactory.assertLogs(
        {
          sendAndCheckError(
            request.toProtoV0(ProtocolVersion.latestForTest),
            { case SendAsyncError.RequestRefused(message) =>
              message should include("needs to use authenticated send operation")
            },
            authenticated = false,
          )
        },
        _.warningMessage should include("needs to use authenticated send operation"),
      )
    }

    "reject unauthenticated member that uses authenticated send" in { _ =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(unauthenticatedMember)

      loggerFactory.assertLogs(
        {
          sendAndCheckError(
            request.toProtoV0(ProtocolVersion.latestForTest),
            { case SendAsyncError.RequestRefused(message) =>
              message should include("needs to use unauthenticated send operation")
            },
            authenticated = true,
          )(new Environment(unauthenticatedMember))
        },
        _.warningMessage should include("needs to use unauthenticated send operation"),
      )
    }

    "reject non domain manager authenticated member sending message to unauthenticated member" in {
      implicit env =>
        val request = defaultRequest
          .focus(_.batch)
          .replace(Batch(List(ClosedEnvelope(content, Recipients.cc(unauthenticatedMember)))))
        loggerFactory.assertLogs(
          {
            sendAndCheckError(
              request.toProtoV0(ProtocolVersion.latestForTest),
              { case SendAsyncError.RequestRefused(message) =>
                message should include("Member is trying to send message to unauthenticated")
              },
              authenticated = true,
            )
          },
          _.warningMessage should include("Member is trying to send message to unauthenticated"),
        )
    }

    "succeed authenticated domain manager sending message to unauthenticated member" in { _ =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(DefaultTestIdentities.domainManager)
        .focus(_.batch)
        .replace(Batch(List(ClosedEnvelope(content, Recipients.cc(unauthenticatedMember)))))
      val domEnvironment = new Environment(DefaultTestIdentities.domainManager)
      sendAndSucceed(request.toProtoV0(ProtocolVersion.latestForTest))(domEnvironment)
    }

    "reject unauthenticated member sending message to non domain manager member" in { _ =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(unauthenticatedMember)
      loggerFactory.assertLogs(
        {
          sendAndCheckError(
            request.toProtoV0(ProtocolVersion.latestForTest),
            { case SendAsyncError.RequestRefused(message) =>
              message should include(
                "Unauthenticated member is trying to send message to members other than the domain manager"
              )
            },
            authenticated = false,
          )(new Environment(unauthenticatedMember))
        },
        _.warningMessage should include(
          "Unauthenticated member is trying to send message to members other than the domain manager"
        ),
      )
    }

    "succeed unauthenticated member sending message to domain manager" in { _ =>
      val request = defaultRequest
        .focus(_.sender)
        .replace(unauthenticatedMember)
        .focus(_.batch)
        .replace(
          Batch(List(ClosedEnvelope(content, Recipients.cc(DefaultTestIdentities.domainManager))))
        )
      new Environment(unauthenticatedMember).service
        .sendAsyncUnauthenticated(request.toProtoV0(ProtocolVersion.latestForTest))
        .map { responseP =>
          val response = SendAsyncResponse.fromProtoV0(responseP)
          response.value.error shouldBe None
        }
    }

    "reject on rate excess" in { implicit env =>
      def expectSuccess(): Future[Assertion] = {
        sendAndSucceed(defaultRequest.toProtoV0(ProtocolVersion.latestForTest))
      }

      def expectOverloaded(): Future[Assertion] = {
        loggerFactory.assertLogs(
          {
            sendAndCheckError(
              defaultRequest.toProtoV0(ProtocolVersion.latestForTest),
              { case SendAsyncError.Overloaded(message) =>
                message should endWith("Submission rate exceeds rate limit of 5/s.")
              },
            )
          },
          _.warningMessage should include("refused: Submission rate exceeds rate limit of 5/s."),
        )
      }

      for {
        _ <- expectSuccess() // push us beyond the max rate
        // Don't submit as we don't know when the current cycle ends
        _ = Threading.sleep(1000) // recover
        _ <- expectSuccess() // good again
        _ <- expectOverloaded() // resource exhausted
        _ = Threading.sleep(1000)
        _ <- expectSuccess() // good again
        _ <- expectOverloaded() // exhausted again
      } yield succeed
    }

    "reject sending to multiple mediators iff the sender is a participant" in { _ =>
      val mediator1: Member = DefaultTestIdentities.mediator
      val mediator2: Member = MediatorId(UniqueIdentifier.tryCreate("another", "mediator"))
      val differentEnvelopes = Batch.fromClosed(
        ClosedEnvelope(
          ByteString.copyFromUtf8("message to first mediator"),
          Recipients.cc(mediator1),
        ),
        ClosedEnvelope(
          ByteString.copyFromUtf8("message to second mediator"),
          Recipients.cc(mediator2),
        ),
      )
      val sameEnvelope = Batch.fromClosed(
        ClosedEnvelope(
          ByteString.copyFromUtf8("message to two mediators and the participant"),
          Recipients(
            NonEmptyList.of(
              RecipientsTree(
                NonEmptySet.of(participant),
                List(
                  RecipientsTree(NonEmptySet.of(mediator1), List.empty),
                  RecipientsTree(NonEmptySet.of(mediator2), List.empty),
                ),
              )
            )
          ),
        )
      )

      val domainManager: Member = DefaultTestIdentities.domainManager

      val batches = Seq(differentEnvelopes, sameEnvelope)
      val badRequests = batches.map(batch => mkSubmissionRequest(batch, participant))
      val goodRequests = batches.map(batch =>
        mkSubmissionRequest(batch, mediator1) -> mediator1
      ) ++ batches.map(batch =>
        mkSubmissionRequest(
          batch,
          domainManager,
        ) -> domainManager
      )
      for {
        _ <- MonadUtil.sequentialTraverse_(badRequests.zipWithIndex) { case (badRequest, index) =>
          withClue(s"bad request #$index") {
            // create a fresh environment for each request such that the rate limiter does not complain
            val participantEnv = new Environment(participant)
            loggerFactory.assertLogs(
              {
                sendAndCheckError(
                  badRequest.toProtoV0(ProtocolVersion.latestForTest),
                  { case SendAsyncError.RequestRefused(message) =>
                    message shouldBe "Batch from participant contains multiple mediators as recipients."
                  },
                )(participantEnv)
              },
              _.warningMessage should include(
                "refused: Batch from participant contains multiple mediators as recipients."
              ),
            )
          }
        }
        // We don't need log suppression for the good requests so we can run them in parallel
        _ <- goodRequests.zipWithIndex.traverse_ { case ((goodRequest, sender), index) =>
          withClue(s"good request #$index") {
            val senderEnv = new Environment(sender)
            sendAndSucceed(goodRequest.toProtoV0(ProtocolVersion.latestForTest))(senderEnv)
          }
        }
      } yield succeed
    }
  }

  "subscribe" should {
    "return error if called with observer not capable of observing server calls" in { env =>
      val observer = new MockStreamObserver()
      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribe(v0.SubscriptionRequest(member = "", counter = 0L), observer)
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == INTERNAL =>
      }
    }

    "return error if request cannot be deserialized" in { env =>
      val observer = new MockServerStreamObserver()
      env.service.subscribe(v0.SubscriptionRequest(member = "", counter = 0L), observer)

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == INVALID_ARGUMENT =>
      }
    }

    "return error if pool registration fails" in { env =>
      val observer = new MockServerStreamObserver()
      val requestP =
        SubscriptionRequest(
          participant,
          GenesisSequencerCounter,
        ).toProtoV0

      Mockito
        .when(
          env.subscriptionPool.create(
            ArgumentMatchers.any[() => Subscription](),
            ArgumentMatchers.any[Member](),
          )(anyTraceContext)
        )
        .thenReturn(Left(PoolClosed))

      env.service.subscribe(requestP, observer)

      inside(observer.items.loneElement) { case StreamError(ex: StatusException) =>
        ex.getStatus.getCode shouldBe UNAVAILABLE
        ex.getStatus.getDescription shouldBe "Subscription pool is closed."
      }
    }

    "return error if sending request with member that is not authenticated" in { env =>
      val observer = new MockServerStreamObserver()
      val requestP =
        SubscriptionRequest(
          ParticipantId("Wrong participant"),
          GenesisSequencerCounter,
        ).toProtoV0

      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribe(requestP, observer)
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == PERMISSION_DENIED =>
      }
    }

    "return error if authenticated member sending request unauthenticated endpoint" in { env =>
      val observer = new MockServerStreamObserver()
      val requestP =
        SubscriptionRequest(
          participant,
          GenesisSequencerCounter,
        ).toProtoV0

      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribeUnauthenticated(requestP, observer)
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == PERMISSION_DENIED =>
      }
    }

    "return error if unauthenticated member sending request authenticated endpoint" in { env =>
      val observer = new MockServerStreamObserver()
      val requestP =
        SubscriptionRequest(
          unauthenticatedMember,
          GenesisSequencerCounter,
        ).toProtoV0

      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribe(requestP, observer)
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == PERMISSION_DENIED =>
      }
    }
  }
}
