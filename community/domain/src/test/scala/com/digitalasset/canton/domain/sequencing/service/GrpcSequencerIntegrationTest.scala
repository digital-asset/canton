// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import akka.NotUsed
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton._
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{
  DefaultProcessingTimeouts,
  LoggingConfig,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.Nonce
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.api.v0.SequencerAuthenticationServiceGrpc.SequencerAuthenticationService
import com.digitalasset.canton.domain.governance.ParticipantAuditor
import com.digitalasset.canton.domain.metrics.DomainTestMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.topology._
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, Lifecycle, SyncCloseable}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.protocol.messages.ProtocolMessage
import com.digitalasset.canton.protocol.v0.EnvelopeContent.SomeEnvelopeContent
import com.digitalasset.canton.protocol.v0.{EnvelopeContent, SignedProtocolMessage}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.client._
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.sequencing.{
  ApplicationHandler,
  GrpcSequencerConnection,
  OrdinaryApplicationHandler,
  SerializedEventHandler,
}
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.time.{DomainTimeTracker, SimClock}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.AkkaUtil
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}
import io.grpc.netty.NettyServerBuilder
import io.opentelemetry.api.trace.Tracer
import org.mockito.ArgumentMatchersSugar
import org.scalatest.Outcome
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}

case class Env(loggerFactory: NamedLoggerFactory)(implicit
    ec: ExecutionContextExecutor,
    tracer: Tracer,
    traceContext: TraceContext,
) extends AutoCloseable
    with NamedLogging
    with org.mockito.MockitoSugar
    with ArgumentMatchersSugar
    with Matchers {
  implicit val actorSystem = AkkaUtil.createActorSystem("GrpcSequencerIntegrationTest")
  val sequencer = mock[Sequencer]
  private val participant = ParticipantId("testing")
  private val domainId = DefaultTestIdentities.domainId
  private val cryptoApi =
    TestingTopology().withParticipants(participant).build().forOwnerAndDomain(participant, domainId)
  private val clock = new SimClock(loggerFactory = loggerFactory)
  private val sequencerSubscriptionFactory = mock[DirectSequencerSubscriptionFactory]
  def timeouts = DefaultProcessingTimeouts.testing
  private val service =
    new GrpcSequencerService(
      sequencer,
      DomainTestMetrics.sequencer,
      loggerFactory,
      ParticipantAuditor.noop,
      member =>
        Either.cond(
          member == participant,
          (),
          s"$participant attempted operation on behalf of $member",
        ),
      new SubscriptionPool[GrpcManagedSubscription](
        clock,
        DomainTestMetrics.sequencer,
        timeouts,
        loggerFactory,
      ),
      sequencerSubscriptionFactory,
      NonNegativeInt.tryCreate(100),
      NonNegativeInt.tryCreate(10000000),
      timeouts,
    )
  private val connectService = new GrpcSequencerConnectService(
    domainId = domainId,
    staticDomainParameters = TestDomainParameters.defaultStatic,
    cryptoApi = cryptoApi,
    agreementManager = None,
    loggerFactory = loggerFactory,
  )

  private val authService = new SequencerAuthenticationService {
    override def challenge(request: v0.Challenge.Request): Future[v0.Challenge.Response] =
      for {
        fingerprints <- cryptoApi.ips.currentSnapshotApproximation
          .signingKeys(participant)
          .map(_.map(_.fingerprint).toList)
      } yield v0.Challenge.Response(
        v0.Challenge.Response.Value
          .Success(
            v0.Challenge.Success(
              ReleaseVersion.current.toProtoPrimitive,
              Nonce.generate().toProtoPrimitive,
              fingerprints.map(_.unwrap),
            )
          )
      )
    override def authenticate(
        request: v0.Authentication.Request
    ): Future[v0.Authentication.Response] =
      Future.successful(
        v0.Authentication.Response(
          v0.Authentication.Response.Value
            .Success(
              v0.Authentication.Success(
                AuthenticationToken.generate().toProtoPrimitive,
                Some(clock.now.plusSeconds(100000).toProtoPrimitive),
              )
            )
        )
      )
  }
  private val serverPort = UniquePortGenerator.forDomainTests.next
  logger.debug(s"Using port ${serverPort} for integration test")
  private val server = NettyServerBuilder
    .forPort(serverPort.unwrap)
    .addService(v0.SequencerConnectServiceGrpc.bindService(connectService, ec))
    .addService(v0.SequencerServiceGrpc.bindService(service, ec))
    .addService(v0.SequencerAuthenticationServiceGrpc.bindService(authService, ec))
    .build()
    .start()

  private val sequencedEventStore = new InMemorySequencedEventStore(loggerFactory)
  private val sendTrackerStore = new InMemorySendTrackerStore()
  private val connection =
    GrpcSequencerConnection(NonEmpty(Seq, Endpoint("localhost", serverPort)), false, None)

  val client = Await
    .result(
      SequencerClient(
        connection,
        domainId,
        DefaultTestIdentities.sequencer,
        cryptoApi,
        cryptoApi.crypto,
        agreedAgreementId = None,
        SequencerClientConfig(),
        TracingConfig.Propagation.Disabled,
        TestingConfigInternal(),
        TestDomainParameters.defaultStatic,
        DefaultProcessingTimeouts.testing,
        clock,
        _ => None,
        _ => None,
        CommonMockMetrics.sequencerClient,
        FutureSupervisor.Noop,
        LoggingConfig(),
        loggerFactory,
        ProtocolVersion.supportedProtocolsParticipant(includeDevelopmentVersions = false),
        Some(ProtocolVersion.latestForTest),
      ).create(
        participant,
        sequencedEventStore,
        sendTrackerStore,
      ).value,
      10.seconds,
    )
    .fold(fail(_), Predef.identity)

  override def close(): Unit =
    Lifecycle.close(
      service,
      Lifecycle.toCloseableServer(server, logger, "test"),
      client,
      Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts),
    )(logger)

  def mockSubscription(
      subscribeCallback: Unit => Unit = _ => (),
      unsubscribeCallback: Unit => Unit = _ => (),
  ): Unit = {
    // when a subscription is made resolve the subscribe promise
    // return to caller a subscription that will resolve the unsubscribe promise on close
    when(
      sequencerSubscriptionFactory
        .create(
          any[SequencerCounter],
          any[String],
          any[Member],
          any[SerializedEventHandler[NotUsed]],
        )(any[TraceContext])
    )
      .thenAnswer {
        subscribeCallback(())
        EitherT.rightT[Future, CreateSubscriptionError] {
          new SequencerSubscription[NotUsed] {
            override protected def loggerFactory: NamedLoggerFactory = Env.this.loggerFactory
            override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
              SyncCloseable(
                "anonymous-sequencer-subscription",
                unsubscribeCallback(()),
              )
            )
            override protected def timeouts: ProcessingTimeout = Env.this.timeouts
          }
        }
      }
  }
}

class GrpcSequencerIntegrationTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext {
  override type FixtureParam = Env

  override def withFixture(test: OneArgTest): Outcome = {
    val env = Env(loggerFactory)
    try super.withFixture(test.toNoArgTest(env))
    finally env.close()
  }

  "GRPC Sequencer" should {
    "cancel the sequencer subscription when the client connection is cancelled" in { env =>
      val subscribePromise = Promise[Unit]()
      val unsubscribePromise = Promise[Unit]()

      // when a subscription is made resolve the subscribe promise
      // return to caller a subscription that will resolve the unsubscribe promise on close
      env.mockSubscription(_ => subscribePromise.success(()), _ => unsubscribePromise.success(()))

      val domainTimeTracker = mock[DomainTimeTracker]
      when(domainTimeTracker.wrapHandler(any[OrdinaryApplicationHandler[Any]]))
        .thenAnswer(Predef.identity[OrdinaryApplicationHandler[Any]] _)

      // kick of subscription
      val initF = env.client.subscribeAfter(
        CantonTimestamp.MinValue,
        ApplicationHandler.success(),
        domainTimeTracker,
      )

      val result = for {
        _ <- initF
        _ <- subscribePromise.future
        _ = env.client.close()
        _ <- unsubscribePromise.future
      } yield succeed // just getting here is good enough

      result.futureValue
    }

    "send from the client gets a message to the sequencer" in { env =>
      import cats.implicits._

      val anotherParticipant = ParticipantId("another")

      when(env.sequencer.sendAsync(any[SubmissionRequest])(anyTraceContext))
        .thenReturn(EitherT.pure[Future, SendAsyncError](()))

      val result = for {
        response <- env.client
          .sendAsync(
            Batch.of((MockProtocolMessage, Recipients.cc(anotherParticipant))),
            SendType.Other,
            None,
          )
          .value
      } yield {
        response shouldBe Right(())
      }

      result.futureValue
    }
  }

  private case object MockProtocolMessage extends ProtocolMessage {
    override def domainId: DomainId = DefaultTestIdentities.domainId
    // no significance to this payload, just need anything valid and this was the easiest to construct
    override def toProtoEnvelopeContentV0(version: ProtocolVersion): EnvelopeContent =
      EnvelopeContent(
        SomeEnvelopeContent.SignedMessage(
          SignedProtocolMessage(None, SignedProtocolMessage.SomeSignedProtocolMessage.Empty)
        )
      )
  }
}
