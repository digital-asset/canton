// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.governance.ParticipantAuditor
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.service.SubscriptionPool.PoolClosed
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  TestDomainParameters,
  v0 as protocolV0,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.{ProtocolVersion, UntypedVersionedMessage}
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import com.google.protobuf.ByteString
import io.grpc.Status.Code.*
import io.grpc.StatusException
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import monocle.macros.syntax.lens.*
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

  private lazy val participant = DefaultTestIdentities.participant1
  private lazy val crypto = new SymbolicPureCrypto
  private lazy val unauthenticatedMember =
    UnauthenticatedMemberId.tryCreate(participant.uid.namespace)(crypto)

  class Environment(member: Member) extends Matchers {
    val sequencer: Sequencer = mock[Sequencer]
    when(sequencer.sendAsync(any[SubmissionRequest])(anyTraceContext))
      .thenReturn(EitherT.rightT[Future, SendAsyncError](()))
    when(sequencer.sendAsyncSigned(any[SignedContent[SubmissionRequest]])(anyTraceContext))
      .thenReturn(EitherT.rightT[Future, SendAsyncError](()))
    val cryptoApi: DomainSyncCryptoClient =
      TestingIdentityFactory(loggerFactory).forOwnerAndDomain(member)
    val subscriptionPool: SubscriptionPool[Subscription] =
      mock[SubscriptionPool[GrpcManagedSubscription]]

    private val maxRatePerParticipant = NonNegativeInt.tryCreate(5)
    val sequencerSubscriptionFactory = mock[DirectSequencerSubscriptionFactory]
    private val topologyClient = mock[DomainTopologyClient]
    private val mockTopologySnapshot = mock[TopologySnapshot]
    when(topologyClient.currentSnapshotApproximation(any[TraceContext]))
      .thenReturn(mockTopologySnapshot)
    when(
      mockTopologySnapshot.findDynamicDomainParametersOrDefault(any[ProtocolVersion], anyBoolean)(
        any[TraceContext]
      )
    )
      .thenReturn(
        Future.successful(
          TestDomainParameters.defaultDynamic(maxRatePerParticipant = maxRatePerParticipant)
        )
      )

    private val maxRatePerParticipantLookup: DomainParametersLookup[NonNegativeInt] =
      DomainParametersLookup.forMaxRatePerParticipant(
        BaseTest.defaultStaticDomainParametersWith(maxRatePerParticipant =
          maxRatePerParticipant.unwrap
        ),
        topologyClient,
        FutureSupervisor.Noop,
        loggerFactory,
      )
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
        maxRatePerParticipantLookup,
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
      testedProtocolVersion,
    )
  }

  Seq(("sendAsync", false), ("sendAsyncSigned", true)).foreach { case (name, useSignedSend) =>
    name should {
      val content = ByteString.copyFromUtf8("123")
      val defaultRequest: SubmissionRequest = {
        val sender: Member = participant
        val recipient = DefaultTestIdentities.participant2
        mkSubmissionRequest(
          Batch(List(ClosedEnvelope(content, Recipients.cc(recipient))), testedProtocolVersion),
          sender,
        )
      }

      def signedContent(requestP: v0.SubmissionRequest): protocolV0.SignedContent =
        protocolV0.SignedContent(
          Some(
            UntypedVersionedMessage(
              UntypedVersionedMessage.Wrapper.Data(requestP.toByteString),
              0,
            ).toByteString
          ),
          Some(Signature.noSignature.toProtoV0),
          None,
        )

      def sendAndCheckSucceed(requestP: v0.SubmissionRequest)(implicit
          env: Environment
      ): Future[Assertion] = {
        (if (useSignedSend) {
           val response = env.service.sendAsyncSigned(signedContent(requestP))
           response.map(SendAsyncResponse.fromSendAsyncSignedResponseProto)
         } else {
           val response = env.service.sendAsync(requestP)
           response.map(SendAsyncResponse.fromSendAsyncResponseProto)
         }).map { responseP =>
          responseP.value.error shouldBe None
        }
      }

      def sendAndCheckError(
          requestP: v0.SubmissionRequest,
          assertion: PartialFunction[SendAsyncError, Assertion],
          authenticated: Boolean = true,
      )(implicit env: Environment): Future[Assertion] = {
        (if (!authenticated) {
           val response = env.service.sendAsyncUnauthenticated(requestP)
           response.map(SendAsyncResponse.fromSendAsyncResponseProto)
         } else if (useSignedSend) {
           val response = env.service.sendAsyncSigned(signedContent(requestP))
           response.map(SendAsyncResponse.fromSendAsyncSignedResponseProto)
         } else {
           val response = env.service.sendAsync(requestP)
           response.map(SendAsyncResponse.fromSendAsyncResponseProto)
         }).map { responseP =>
          assertion(responseP.value.error.value)
        }
      }

      "reject empty request" in { implicit env =>
        val requestP = v0.SubmissionRequest("", "", false, None, None, None)
        loggerFactory.assertLogs(
          {
            sendAndCheckError(
              requestP,
              { case SendAsyncError.RequestInvalid(message) =>
                message should startWith("ValueConversionError(sender,Invalid keyOwner ``")
              },
            )
          },
          _.warningMessage should startWith(
            "ValueConversionError(sender,Invalid keyOwner ``"
          ),
        )
      }

      "reject envelopes with empty content" in { implicit env =>
        val request = defaultRequest
          .focus(_.batch.envelopes)
          .modify(_.map(_.focus(_.bytes).replace(ByteString.EMPTY)))

        loggerFactory.assertLogs(
          sendAndCheckError(
            request.toProtoV0,
            { case SendAsyncError.RequestInvalid(message) =>
              message shouldBe "Batch contains envelope without content."
            },
          ),
          _.warningMessage should endWith("is invalid: Batch contains envelope without content."),
        )
      }

      "reject envelopes with invalid sender" in { implicit env =>
        val requestP = defaultRequest.toProtoV0.focus(_.sender).modify {
          case "" => fail("sender should be set")
          case _sender => "THISWILLFAIL"
        }

        loggerFactory.assertLogs(
          {
            sendAndCheckError(
              requestP,
              { case SendAsyncError.RequestInvalid(message) =>
                message should startWith(
                  "ValueConversionError(sender,Expected delimiter :: after three letter code of `THISWILLFAIL`)"
                )
              },
            )
          },
          _.warningMessage should startWith(
            "ValueConversionError(sender,Expected delimiter :: after three letter code of `THISWILLFAIL`)"
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

        val alarmMsg = s"Max bytes to decompress is exceeded. The limit is 1000 bytes."
        loggerFactory.assertLogs(
          {
            sendAndCheckError(
              request.toProtoV0,
              { case SendAsyncError.RequestInvalid(message) =>
                message should include(alarmMsg)
              },
            )
          },
          _.shouldBeCantonError(
            SequencerError.MaxRequestSizeExceeded,
            _ shouldBe alarmMsg,
          ),
        )
      }

      "reject unauthorized authenticated participant" in { implicit env =>
        val request = defaultRequest
          .focus(_.sender)
          .replace(DefaultTestIdentities.participant2)

        loggerFactory.assertLogs(
          {
            sendAndCheckError(
              request.toProtoV0,
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
              request.toProtoV0,
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
              request.toProtoV0,
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
            .replace(
              Batch(
                List(ClosedEnvelope(content, Recipients.cc(unauthenticatedMember))),
                testedProtocolVersion,
              )
            )
          loggerFactory.assertLogs(
            {
              sendAndCheckError(
                request.toProtoV0,
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
          .replace(
            Batch(
              List(ClosedEnvelope(content, Recipients.cc(unauthenticatedMember))),
              testedProtocolVersion,
            )
          )
        val domEnvironment = new Environment(DefaultTestIdentities.domainManager)
        sendAndCheckSucceed(request.toProtoV0)(domEnvironment)
      }

      "reject unauthenticated member sending message to non domain manager member" in { _ =>
        val request = defaultRequest
          .focus(_.sender)
          .replace(unauthenticatedMember)
        loggerFactory.assertLogs(
          {
            sendAndCheckError(
              request.toProtoV0,
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
            Batch(
              List(ClosedEnvelope(content, Recipients.cc(DefaultTestIdentities.domainManager))),
              testedProtocolVersion,
            )
          )
        new Environment(unauthenticatedMember).service
          .sendAsyncUnauthenticated(request.toProtoV0)
          .map { responseP =>
            val response = SendAsyncResponse.fromSendAsyncResponseProto(responseP)
            response.value.error shouldBe None
          }
      }

      "reject on rate excess" in { implicit env =>
        def expectSuccess(): Future[Assertion] = {
          sendAndCheckSucceed(defaultRequest.toProtoV0)
        }

        def expectOverloaded(): Future[Assertion] = {
          sendAndCheckError(
            defaultRequest.toProtoV0,
            { case SendAsyncError.Overloaded(message) =>
              message should endWith("Submission rate exceeds rate limit of 5/s.")
            },
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
          testedProtocolVersion,
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
          testedProtocolVersion,
          ClosedEnvelope(
            ByteString.copyFromUtf8("message to two mediators and the participant"),
            Recipients(
              NonEmpty(
                Seq,
                RecipientsTree(
                  NonEmpty.mk(Set, participant),
                  Seq(
                    RecipientsTree.leaf(NonEmpty.mk(Set, mediator1)),
                    RecipientsTree.leaf(NonEmpty.mk(Set, mediator2)),
                  ),
                ),
              )
            ),
          ),
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
                    badRequest.toProtoV0,
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
              sendAndCheckSucceed(goodRequest.toProtoV0)(senderEnv)
            }
          }
        } yield succeed
      }

      "reject requests from unauthenticated senders with a signing key timestamp" in { _ =>
        val request = defaultRequest
          .focus(_.sender)
          .replace(unauthenticatedMember)
          .focus(_.timestampOfSigningKey)
          .replace(Some(CantonTimestamp.Epoch))
          .focus(_.batch)
          .replace(
            Batch(
              List(ClosedEnvelope(content, Recipients.cc(DefaultTestIdentities.domainManager))),
              testedProtocolVersion,
            )
          )

        loggerFactory.assertLogs(
          sendAndCheckError(
            request.toProtoV0,
            { case SendAsyncError.RequestRefused(message) =>
              message should include(
                "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key"
              )
            },
            authenticated = false,
          )(new Environment(unauthenticatedMember)),
          _.warningMessage should include(
            "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key"
          ),
        )
      }

      "reject requests to unauthenticated members with a signing key timestamps" in {
        implicit env =>
          val request = defaultRequest
            .focus(_.timestampOfSigningKey)
            .replace(Some(CantonTimestamp.ofEpochSecond(1)))
            .focus(_.batch)
            .replace(
              Batch(
                List(ClosedEnvelope(content, Recipients.cc(unauthenticatedMember))),
                testedProtocolVersion,
              )
            )

          loggerFactory.assertLogs(
            sendAndCheckError(
              request.toProtoV0,
              { case SendAsyncError.RequestRefused(message) =>
                message should include(
                  "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key"
                )
              },
            ),
            _.warningMessage should include(
              "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key"
            ),
          )
      }
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
          SequencerCounter.Genesis,
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
          SequencerCounter.Genesis,
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
          SequencerCounter.Genesis,
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
          SequencerCounter.Genesis,
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
