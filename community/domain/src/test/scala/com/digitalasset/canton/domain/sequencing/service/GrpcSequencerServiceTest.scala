// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble, PositiveInt}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.governance.ParticipantAuditor
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.domain.sequencing.SequencerParameters
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.service.SubscriptionPool.PoolClosed
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  TestDomainParameters,
  v0 as protocolV0,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.BytestringWithCryptographicEvidence
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{DomainTopologyClient, TopologySnapshot}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.{ProtocolVersion, VersionedMessage}
import com.digitalasset.canton.{
  BaseTest,
  ProtocolVersionChecksFixtureAsyncWordSpec,
  SequencerCounter,
}
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
class GrpcSequencerServiceTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksFixtureAsyncWordSpec {
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
    when(sequencer.acknowledge(any[Member], any[CantonTimestamp])(anyTraceContext))
      .thenReturn(Future.unit)
    when(sequencer.acknowledgeSigned(any[SignedContent[AcknowledgeRequest]])(anyTraceContext))
      .thenReturn(EitherT.rightT(()))
    val cryptoApi: DomainSyncCryptoClient =
      TestingIdentityFactory(loggerFactory).forOwnerAndDomain(member)
    val subscriptionPool: SubscriptionPool[Subscription] =
      mock[SubscriptionPool[GrpcManagedSubscription]]

    private val maxRatePerParticipant = NonNegativeInt.tryCreate(5)
    private val maxRequestSize = NonNegativeInt.tryCreate(1000)
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
          TestDomainParameters.defaultDynamic(
            maxRatePerParticipant = maxRatePerParticipant,
            maxRequestSize = MaxRequestSize(maxRequestSize),
          )
        )
      )

    private val domainParamLookup: DomainParametersLookup[SequencerDomainParameters] =
      DomainParametersLookup.forSequencerDomainParameters(
        BaseTest.defaultStaticDomainParametersWith(
          maxRatePerParticipant = maxRatePerParticipant.unwrap,
          maxRequestSize = maxRequestSize.unwrap,
        ),
        None,
        topologyClient,
        FutureSupervisor.Noop,
        loggerFactory,
      )
    private val params = new SequencerParameters {
      override def maxBurstFactor: PositiveDouble = PositiveDouble.tryCreate(1e-6)
      override def processingTimeouts: ProcessingTimeout = timeouts
    }
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
        domainParamLookup,
        params,
        None,
        BaseTest.testedProtocolVersion,
        enableMediatorUnauthenticatedMessages = false,
      )
  }

  override type FixtureParam = Environment

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Environment(participant)
    withFixture(test.toNoArgAsyncTest(env))
  }

  private def mkSubmissionRequest(
      batch: Batch[ClosedEnvelope],
      sender: Member,
  ): SubmissionRequest = {
    val id = MessageId.tryCreate("messageId")
    SubmissionRequest.tryCreate(
      sender,
      id,
      isRequest = true,
      batch,
      CantonTimestamp.MaxValue,
      None,
      None,
      testedProtocolVersion,
    )
  }

  private def signedContent(bytes: ByteString): SignedContent[BytestringWithCryptographicEvidence] =
    SignedContent(
      BytestringWithCryptographicEvidence(bytes),
      Signature.noSignature,
      None,
      testedProtocolVersion,
    )

  private lazy val content = ByteString.copyFromUtf8("123")
  private lazy val defaultRequest: SubmissionRequest = {
    val sender: Member = participant
    val recipient = DefaultTestIdentities.participant2
    mkSubmissionRequest(
      Batch(
        List(
          ClosedEnvelope
            .tryCreate(content, Recipients.cc(recipient), Seq.empty, testedProtocolVersion)
        ),
        testedProtocolVersion,
      ),
      sender,
    )
  }

  Seq(("send unsigned", false), ("send signed", true)).foreach { case (name, useSignedSend) =>
    name should {
      val versioned = SubmissionRequest.usingVersionedSubmissionRequest(testedProtocolVersion)

      def signedSubmissionReq(
          request: SubmissionRequest
      ): SignedContent[BytestringWithCryptographicEvidence] =
        signedContent(request.toByteString)

      def sendProto(
          requestV0: protocolV0.SubmissionRequest,
          signedRequestV0: protocolV0.SignedContent,
          versionedRequest: ByteString,
          versionedSignedRequest: ByteString,
          authenticated: Boolean,
      )(implicit env: Environment): Future[ParsingResult[SendAsyncResponse]] = {
        import env.*

        if (!authenticated) {
          val response =
            if (versioned) {
              val requestP = v0.SendAsyncUnauthenticatedVersionedRequest(versionedRequest)
              service.sendAsyncUnauthenticatedVersioned(requestP)
            } else
              service.sendAsyncUnauthenticated(requestV0)
          response.map(SendAsyncResponse.fromSendAsyncResponseProto)
        } else if (useSignedSend) {
          val response =
            if (versioned) {
              val requestP = v0.SendAsyncVersionedRequest(versionedSignedRequest)
              service.sendAsyncVersioned(requestP)
            } else service.sendAsyncSigned(signedRequestV0)
          response.map(SendAsyncResponse.fromSendAsyncSignedResponseProto)
        } else {
          val response = service.sendAsync(requestV0)
          response.map(SendAsyncResponse.fromSendAsyncResponseProto)
        }
      }

      def send(request: SubmissionRequest, authenticated: Boolean)(implicit
          env: Environment
      ): Future[ParsingResult[SendAsyncResponse]] = {
        val signedRequest = signedSubmissionReq(request)
        sendProto(
          request.toProtoV0,
          signedRequest.toProtoV0,
          request.toByteString,
          signedRequest.toByteString,
          authenticated,
        )
      }

      def sendAndCheckSucceed(request: SubmissionRequest)(implicit
          env: Environment
      ): Future[Assertion] =
        send(request, authenticated = true).map { responseP =>
          responseP.value.error shouldBe None
        }

      def sendAndCheckError(
          request: SubmissionRequest,
          authenticated: Boolean = true,
      )(assertion: PartialFunction[SendAsyncError, Assertion])(implicit
          env: Environment
      ): Future[Assertion] =
        send(request, authenticated).map { responseP =>
          assertion(responseP.value.error.value)
        }

      def sendProtoAndCheckError(
          requestV0: protocolV0.SubmissionRequest,
          signedRequestV0: protocolV0.SignedContent,
          versionedRequest: ByteString,
          versionedSignedRequest: ByteString,
          assertion: PartialFunction[SendAsyncError, Assertion],
          authenticated: Boolean = true,
      )(implicit env: Environment): Future[Assertion] =
        sendProto(
          requestV0,
          signedRequestV0,
          versionedRequest,
          versionedSignedRequest,
          authenticated,
        ).map { responseP =>
          assertion(responseP.value.error.value)
        }

      if (SubmissionRequest.usingSignedSubmissionRequest(testedProtocolVersion) == useSignedSend) {
        "reject empty request" in { implicit env =>
          val requestV0 = protocolV0.SubmissionRequest("", "", false, None, None, None)
          val signedRequestV0 = signedContent(
            VersionedMessage[SubmissionRequest](requestV0.toByteString, 0).toByteString
          )

          loggerFactory.assertLogs(
            sendProtoAndCheckError(
              requestV0,
              signedRequestV0.toProtoV0,
              VersionedMessage(requestV0.toByteString, 0).toByteString,
              signedRequestV0.toByteString,
              { case SendAsyncError.RequestInvalid(message) =>
                message should startWith("ValueConversionError(sender,Invalid keyOwner ``")
              },
            ),
            _.warningMessage should startWith("ValueConversionError(sender,Invalid keyOwner ``"),
          )
        }

        "reject envelopes with empty content" in { implicit env =>
          val request = defaultRequest
            .focus(_.batch.envelopes)
            .modify(_.map(_.focus(_.bytes).replace(ByteString.EMPTY)))

          loggerFactory.assertLogs(
            sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
              message shouldBe "Batch contains envelope without content."
            },
            _.warningMessage should endWith(
              "is invalid: Batch contains envelope without content."
            ),
          )
        }

        "reject envelopes with invalid sender" in { implicit env =>
          val requestV0 = defaultRequest.toProtoV0.focus(_.sender).modify {
            case "" => fail("sender should be set")
            case _sender => "THISWILLFAIL"
          }
          val signedRequestV0 = signedContent(
            VersionedMessage[SubmissionRequest](requestV0.toByteString, 0).toByteString
          )
          loggerFactory.assertLogs(
            sendProtoAndCheckError(
              requestV0,
              signedRequestV0.toProtoV0,
              VersionedMessage(requestV0.toByteString, 0).toByteString,
              signedRequestV0.toByteString,
              { case SendAsyncError.RequestInvalid(message) =>
                message should startWith(
                  "ValueConversionError(sender,Expected delimiter :: after three letter code of `THISWILLFAIL`)"
                )
              },
            ),
            _.warningMessage should startWith(
              "ValueConversionError(sender,Expected delimiter :: after three letter code of `THISWILLFAIL`)"
            ),
          )
        }

        "reject large messages" in { implicit env =>
          val bigEnvelope =
            ClosedEnvelope.tryCreate(
              ByteString.copyFromUtf8(scala.util.Random.nextString(5000)),
              Recipients.cc(participant),
              Seq.empty,
              testedProtocolVersion,
            )
          val request = defaultRequest.focus(_.batch.envelopes).replace(List(bigEnvelope))

          val alarmMsg = s"Max bytes to decompress is exceeded. The limit is 1000 bytes."
          loggerFactory.assertLogs(
            sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
              message should include(alarmMsg)
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
            sendAndCheckError(request) { case SendAsyncError.RequestRefused(message) =>
              message should (include("is not authorized to send:")
                and include("just tried to use sequencer on behalf of"))
            },
            _.warningMessage should (include("is not authorized to send:")
              and include("just tried to use sequencer on behalf of")),
          )
        }

        "reject unauthenticated member that uses authenticated send" in { _ =>
          val request = defaultRequest
            .focus(_.sender)
            .replace(unauthenticatedMember)

          loggerFactory.assertLogs(
            sendAndCheckError(request, authenticated = true) {
              case SendAsyncError.RequestRefused(message) =>
                message should include("needs to use unauthenticated send operation")
            }(new Environment(unauthenticatedMember)),
            _.warningMessage should include("needs to use unauthenticated send operation"),
          )
        }

        "reject non domain manager authenticated member sending message to unauthenticated member" in {
          implicit env =>
            val request = defaultRequest
              .focus(_.batch)
              .replace(
                Batch(
                  List(
                    ClosedEnvelope.tryCreate(
                      content,
                      Recipients.cc(unauthenticatedMember),
                      Seq.empty,
                      testedProtocolVersion,
                    )
                  ),
                  testedProtocolVersion,
                )
              )
            loggerFactory.assertLogs(
              sendAndCheckError(request, authenticated = true) {
                case SendAsyncError.RequestRefused(message) =>
                  message should include("Member is trying to send message to unauthenticated")
              },
              _.warningMessage should include(
                "Member is trying to send message to unauthenticated"
              ),
            )
        }

        "succeed authenticated domain manager sending message to unauthenticated member" in { _ =>
          val request = defaultRequest
            .focus(_.sender)
            .replace(DefaultTestIdentities.domainManager)
            .focus(_.batch)
            .replace(
              Batch(
                List(
                  ClosedEnvelope.tryCreate(
                    content,
                    Recipients.cc(unauthenticatedMember),
                    Seq.empty,
                    testedProtocolVersion,
                  )
                ),
                testedProtocolVersion,
              )
            )
          val domEnvironment = new Environment(DefaultTestIdentities.domainManager)
          sendAndCheckSucceed(request)(domEnvironment)
        }

        "succeed unauthenticated member sending message to domain manager" in { _ =>
          val request = defaultRequest
            .focus(_.sender)
            .replace(unauthenticatedMember)
            .focus(_.batch)
            .replace(
              Batch(
                List(
                  ClosedEnvelope.tryCreate(
                    content,
                    Recipients.cc(DefaultTestIdentities.domainManager),
                    Seq.empty,
                    testedProtocolVersion,
                  )
                ),
                testedProtocolVersion,
              )
            )
          val newEnv = new Environment(unauthenticatedMember).service
          val responseF =
            if (versioned)
              newEnv.sendAsyncUnauthenticatedVersioned(
                v0.SendAsyncUnauthenticatedVersionedRequest(request.toByteString)
              )
            else
              newEnv.sendAsyncUnauthenticated(request.toProtoV0)
          responseF
            .map { responseP =>
              val response = SendAsyncResponse.fromSendAsyncResponseProto(responseP)
              response.value.error shouldBe None
            }
        }

        "reject on rate excess" in { implicit env =>
          def expectSuccess(): Future[Assertion] = {
            sendAndCheckSucceed(defaultRequest)
          }

          def expectOverloaded(): Future[Assertion] = {
            sendAndCheckError(defaultRequest) { case SendAsyncError.Overloaded(message) =>
              message should endWith("Submission rate exceeds rate limit of 5/s.")
            }
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

        def multipleMediatorTestCase(
            mediator1: RecipientsTree,
            mediator2: RecipientsTree,
        ): (FixtureParam => Future[Assertion]) = { _ =>
          val differentEnvelopes = Batch.fromClosed(
            testedProtocolVersion,
            ClosedEnvelope.tryCreate(
              ByteString.copyFromUtf8("message to first mediator"),
              Recipients(NonEmpty.mk(Seq, mediator1)),
              Seq.empty,
              testedProtocolVersion,
            ),
            ClosedEnvelope.tryCreate(
              ByteString.copyFromUtf8("message to second mediator"),
              Recipients(NonEmpty.mk(Seq, mediator2)),
              Seq.empty,
              testedProtocolVersion,
            ),
          )
          val sameEnvelope = Batch.fromClosed(
            testedProtocolVersion,
            ClosedEnvelope.tryCreate(
              ByteString.copyFromUtf8("message to two mediators and the participant"),
              Recipients(
                NonEmpty(
                  Seq,
                  RecipientsTree.ofMembers(
                    NonEmpty.mk(Set, participant),
                    Seq(
                      mediator1,
                      mediator2,
                    ),
                  ),
                )
              ),
              Seq.empty,
              testedProtocolVersion,
            ),
          )

          val domainManager: Member = DefaultTestIdentities.domainManager

          val batches = Seq(differentEnvelopes, sameEnvelope)
          val badRequests = batches.map(batch => mkSubmissionRequest(batch, participant))
          val goodRequests = batches.map(batch =>
            mkSubmissionRequest(
              batch,
              DefaultTestIdentities.mediator,
            ) -> DefaultTestIdentities.mediator
          ) ++ batches.map(batch =>
            mkSubmissionRequest(
              batch,
              domainManager,
            ) -> domainManager
          )
          for {
            _ <- MonadUtil.sequentialTraverse_(badRequests.zipWithIndex) {
              case (badRequest, index) =>
                withClue(s"bad request #$index") {
                  // create a fresh environment for each request such that the rate limiter does not complain
                  val participantEnv = new Environment(participant)
                  loggerFactory.assertLogs(
                    sendAndCheckError(badRequest) { case SendAsyncError.RequestRefused(message) =>
                      message shouldBe "Batch from participant contains multiple mediators as recipients."
                    }(participantEnv),
                    _.warningMessage should include(
                      "refused: Batch from participant contains multiple mediators as recipients."
                    ),
                  )
                }
            }
            // We don't need log suppression for the good requests so we can run them in parallel
            _ <- goodRequests.zipWithIndex.parTraverse_ { case ((goodRequest, sender), index) =>
              withClue(s"good request #$index") {
                val senderEnv = new Environment(sender)
                sendAndCheckSucceed(goodRequest)(senderEnv)
              }
            }
          } yield succeed
        }

        "reject sending to multiple mediators iff the sender is a participant" in multipleMediatorTestCase(
          RecipientsTree.leaf(NonEmpty.mk(Set, DefaultTestIdentities.mediator)),
          RecipientsTree.leaf(
            NonEmpty.mk(Set, MediatorId(UniqueIdentifier.tryCreate("another", "mediator")))
          ),
        )

        // TODO(#12373) Adapt when releasing BFT
        "reject sending to multiple mediator groups iff the sender is a participant" onlyRunWithOrGreaterThan (ProtocolVersion.dev) in multipleMediatorTestCase(
          RecipientsTree(
            NonEmpty.mk(
              Set,
              MediatorsOfDomain(NonNegativeInt.tryCreate(1)),
            ),
            Seq.empty,
          ),
          RecipientsTree(
            NonEmpty.mk(
              Set,
              MediatorsOfDomain(NonNegativeInt.tryCreate(2)),
            ),
            Seq.empty,
          ),
        )

        "reject requests to unauthenticated members with a signing key timestamps" in {
          implicit env =>
            val request = defaultRequest
              .focus(_.timestampOfSigningKey)
              .replace(Some(CantonTimestamp.ofEpochSecond(1)))
              .focus(_.batch)
              .replace(
                Batch(
                  List(
                    ClosedEnvelope.tryCreate(
                      content,
                      Recipients.cc(unauthenticatedMember),
                      Seq.empty,
                      testedProtocolVersion,
                    )
                  ),
                  testedProtocolVersion,
                )
              )

            loggerFactory.assertLogs(
              sendAndCheckError(request) { case SendAsyncError.RequestRefused(message) =>
                message should include(
                  "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key"
                )
              },
              _.warningMessage should include(
                "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key"
              ),
            )
        }

        // TODO(#12373) Adapt when releasing BFT
        "reject unauthenticated eligible members in aggregation rule" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
          implicit env =>
            val request = defaultRequest
              .focus(_.aggregationRule)
              .replace(
                Some(
                  AggregationRule(
                    eligibleMembers = NonEmpty(Seq, participant, unauthenticatedMember),
                    threshold = PositiveInt.tryCreate(1),
                    testedProtocolVersion,
                  )
                )
              )
            loggerFactory.assertLogs(
              sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
                message should include(
                  "Eligible senders in aggregation rule must be authenticated, but found unauthenticated members"
                )
              },
              _.warningMessage should include(
                "Eligible senders in aggregation rule must be authenticated, but found unauthenticated members"
              ),
            )
        }

        // TODO(#12373) Adapt when releasing BFT
        "reject unachievable threshold in aggregation rule" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
          implicit env =>
            val request = defaultRequest
              .focus(_.aggregationRule)
              .replace(
                Some(
                  AggregationRule(
                    eligibleMembers = NonEmpty(Seq, participant, participant),
                    threshold = PositiveInt.tryCreate(2),
                    testedProtocolVersion,
                  )
                )
              )
            loggerFactory.assertLogs(
              sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
                message should include("Threshold 2 cannot be reached")
              },
              _.warningMessage should include("Threshold 2 cannot be reached"),
            )
        }

        // TODO(#12373) Adapt when releasing BFT
        "reject uneligible sender in aggregation rule" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
          implicit env =>
            val request = defaultRequest
              .focus(_.aggregationRule)
              .replace(
                Some(
                  AggregationRule(
                    eligibleMembers = NonEmpty(Seq, DefaultTestIdentities.participant2),
                    threshold = PositiveInt.tryCreate(1),
                    testedProtocolVersion,
                  )
                )
              )
            loggerFactory.assertLogs(
              sendAndCheckError(request) { case SendAsyncError.RequestInvalid(message) =>
                message should include("Sender is not eligible according to the aggregation rule")
              },
              _.warningMessage should include(
                "Sender is not eligible according to the aggregation rule"
              ),
            )
        }
      } else {
        "reject the request" in { implicit env =>
          send(defaultRequest, authenticated = true).failed.map { error =>
            error.getMessage should (include("UNIMPLEMENTED") and include(
              s"send endpoints must be used with protocol version $testedProtocolVersion"
            ))
          }

        }
      }

      "reject unauthenticated member sending message to non domain manager member" in { _ =>
        val request = defaultRequest
          .focus(_.sender)
          .replace(unauthenticatedMember)
        loggerFactory.assertLogs(
          sendAndCheckError(request, authenticated = false) {
            case SendAsyncError.RequestRefused(message) =>
              message should include(
                "Unauthenticated member is trying to send message to members other than the domain manager"
              )
          }(new Environment(unauthenticatedMember)),
          _.warningMessage should include(
            "Unauthenticated member is trying to send message to members other than the domain manager"
          ),
        )
      }

      "reject authenticated member that uses unauthenticated send" in { implicit env =>
        val request = defaultRequest
          .focus(_.sender)
          .replace(DefaultTestIdentities.participant1)

        loggerFactory.assertLogs(
          sendAndCheckError(request, authenticated = false) {
            case SendAsyncError.RequestRefused(message) =>
              message should include("needs to use authenticated send operation")
          },
          _.warningMessage should include("needs to use authenticated send operation"),
        )
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
              List(
                ClosedEnvelope.tryCreate(
                  content,
                  Recipients.cc(DefaultTestIdentities.domainManager),
                  Seq.empty,
                  testedProtocolVersion,
                )
              ),
              testedProtocolVersion,
            )
          )

        loggerFactory.assertLogs(
          sendAndCheckError(request, authenticated = false) {
            case SendAsyncError.RequestRefused(message) =>
              message should include(
                "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key"
              )
          }(new Environment(unauthenticatedMember)),
          _.warningMessage should include(
            "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key"
          ),
        )
      }

      if (SubmissionRequest.usingVersionedSubmissionRequest(testedProtocolVersion)) {
        "reject unversioned authenticated endpoint" in { implicit env =>
          val responseF =
            if (useSignedSend)
              env.service.sendAsyncSigned(signedContent(defaultRequest.toByteString).toProtoV0)
            else env.service.sendAsync(defaultRequest.toProtoV0)

          responseF.failed
            .map { error =>
              error.getMessage should (include("UNIMPLEMENTED") and include(
                s"send endpoints must be used with protocol version $testedProtocolVersion"
              ))
            }
        }
      } else {
        "reject versioned authenticated endpoint" in { implicit env =>
          env.service
            .sendAsyncVersioned(
              v0.SendAsyncVersionedRequest(signedContent(defaultRequest.toByteString).toByteString)
            )
            .failed
            .map { error =>
              error.getMessage should (include("UNIMPLEMENTED") and include(
                s"send endpoints must be used with protocol version $testedProtocolVersion"
              ))
            }
        }
      }
    }
  }

  if (SubmissionRequest.usingVersionedSubmissionRequest(testedProtocolVersion)) {
    "reject unversioned unauthenticated endpoint" in { implicit env =>
      val responseF = env.service.sendAsyncUnauthenticated(defaultRequest.toProtoV0)
      responseF.failed
        .map { error =>
          error.getMessage should (include("UNIMPLEMENTED") and include(
            s"send endpoints must be used with protocol version $testedProtocolVersion"
          ))
        }
    }
  } else {
    "reject versioned unauthenticated endpoint" in { implicit env =>
      env.service
        .sendAsyncUnauthenticatedVersioned(
          v0.SendAsyncUnauthenticatedVersionedRequest(defaultRequest.toByteString)
        )
        .failed
        .map { error =>
          error.getMessage should (include("UNIMPLEMENTED") and include(
            s"send endpoints must be used with protocol version $testedProtocolVersion"
          ))
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
          testedProtocolVersion,
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
          testedProtocolVersion,
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
          testedProtocolVersion,
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
          testedProtocolVersion,
        ).toProtoV0

      loggerFactory.suppressWarningsAndErrors {
        env.service.subscribe(requestP, observer)
      }

      observer.items.toSeq should matchPattern {
        case Seq(StreamError(err: StatusException)) if err.getStatus.getCode == PERMISSION_DENIED =>
      }
    }
  }

  Seq(("acknowledge", false), ("acknowledgeSigned", true)).foreach { case (name, useSignedAck) =>
    def performAcknowledgeRequest(env: Environment)(request: AcknowledgeRequest) =
      if (useSignedAck) {
        env.service.acknowledgeSigned(signedAcknowledgeReq(request.toProtoV0))
      } else
        env.service.acknowledge(request.toProtoV0)

    def signedAcknowledgeReq(requestP: v0.AcknowledgeRequest): protocolV0.SignedContent =
      signedContent(VersionedMessage(requestP.toByteString, 0).toByteString).toProtoV0

    name should {
      if (SubmissionRequest.usingSignedSubmissionRequest(testedProtocolVersion) == useSignedAck) {
        "reject unauthorized authenticated participant" in { implicit env =>
          val unauthorizedParticipant = DefaultTestIdentities.participant2
          val req =
            AcknowledgeRequest(
              unauthorizedParticipant,
              CantonTimestamp.Epoch,
              testedProtocolVersion,
            )

          loggerFactory.assertLogs(
            performAcknowledgeRequest(env)(req).failed.map(error =>
              error.getMessage should include("PERMISSION_DENIED")
            ),
            _.warningMessage should (include("Authentication check failed:")
              and include("just tried to use sequencer on behalf of")),
          )
        }

        "succeed with correct participant" in { implicit env =>
          val req = AcknowledgeRequest(participant, CantonTimestamp.Epoch, testedProtocolVersion)
          performAcknowledgeRequest(env)(req).map(_ => succeed)
        }
      } else {
        "reject the acknowledgement" in { implicit env =>
          val req = AcknowledgeRequest(participant, CantonTimestamp.Epoch, testedProtocolVersion)
          performAcknowledgeRequest(env)(req).failed.map { error =>
            error.getMessage should (include("UNIMPLEMENTED") and include(
              s"acknowledgement endpoints must be used with protocol version $testedProtocolVersion"
            ))
          }
        }
      }
    }
  }
}
