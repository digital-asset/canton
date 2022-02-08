// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.authentication.grpc

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0.{Hello, HelloServiceGrpc}
import com.digitalasset.canton.domain.governance.ParticipantAuditor
import com.digitalasset.canton.domain.sequencing.authentication.{
  InMemoryMemberAuthenticationStore,
  MemberAuthenticationService,
  StoredAuthenticationToken,
}
import com.digitalasset.canton.sequencing.authentication.grpc.{
  AuthenticationTokenManagerTest,
  AuthenticationTokenWithExpiry,
  SequencerClientNoAuthentication,
  SequencerClientTokenAuthentication,
}
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{ParticipantId, UnauthenticatedMemberId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, DomainId, HasExecutionContext}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ServerInterceptors, Status}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration => JDuration}
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
class SequencerAuthenticationServerInterceptorTest
    extends AnyWordSpec
    with BaseTest
    with BeforeAndAfterEach
    with HasExecutionContext {
  class GrpcHelloService extends HelloServiceGrpc.HelloService {
    override def hello(request: Hello.Request): Future[Hello.Response] =
      Future.successful(Hello.Response("hello back"))
    override def helloStreamed(
        request: Hello.Request,
        responseObserver: StreamObserver[Hello.Response],
    ): Unit = ???
  }
  val service = new GrpcHelloService()

  val store = new InMemoryMemberAuthenticationStore()
  val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("popo::pipi"))
  val authService = new MemberAuthenticationService(
    domainId,
    null,
    store,
    None,
    new SimClock(loggerFactory = loggerFactory),
    JDuration.ofMinutes(1),
    JDuration.ofHours(1),
    _ => (),
    Future.unit,
    loggerFactory,
    ParticipantAuditor.noop,
  ) {
    override protected def isParticipantActive(participant: ParticipantId)(implicit
        traceContext: TraceContext
    ): Future[Boolean] = Future.successful(true)
  }
  val serverInterceptor = new SequencerAuthenticationServerInterceptor(authService, loggerFactory)

  val channelName = InProcessServerBuilder.generateName()
  val server = InProcessServerBuilder
    .forName(channelName)
    .addService(
      ServerInterceptors.intercept(
        HelloServiceGrpc.bindService(service, parallelExecutionContext),
        serverInterceptor,
      )
    )
    .build()
    .start()

  val participantId =
    UniqueIdentifier.fromProtoPrimitive_("p1::default").map(new ParticipantId(_)).value
  val unauthenticatedMemberId =
    UniqueIdentifier.fromProtoPrimitive_("unm1::default").map(new UnauthenticatedMemberId(_)).value
  val neverExpire = CantonTimestamp.MaxValue
  val token = AuthenticationTokenWithExpiry(AuthenticationToken.generate(), neverExpire)
  val incorrectToken = AuthenticationTokenWithExpiry(AuthenticationToken.generate(), neverExpire)

  require(token != incorrectToken, "The generated tokens must be different")

  override def beforeEach(): Unit = {
    store.saveToken(StoredAuthenticationToken(participantId, neverExpire, token.token)).futureValue
  }

  var channel: ManagedChannel = _
  override def afterEach(): Unit = {
    channel.shutdown()
    channel.awaitTermination(2, TimeUnit.SECONDS)
    channel.shutdownNow()
  }

  "Authentication interceptors" should {
    "fail request if client does not use interceptor to add auth metadata" in
      loggerFactory.suppressWarningsAndErrors {
        channel = InProcessChannelBuilder.forName(channelName).build()
        val client = HelloServiceGrpc.stub(channel)

        inside(client.hello(Hello.Request("hi")).failed.futureValue) {
          case status: io.grpc.StatusRuntimeException =>
            status.getStatus.getCode shouldBe io.grpc.Status.UNAUTHENTICATED.getCode
        }
      }

    "succeed request if participant use interceptor with correct token information" in {
      val clientAuthentication =
        SequencerClientTokenAuthentication(
          domainId,
          participantId,
          () => EitherT.pure[Future, Status](token),
          isClosed = false,
          AuthenticationTokenManagerConfig(),
          AuthenticationTokenManagerTest.mockClock,
          loggerFactory,
        )
      channel = InProcessChannelBuilder
        .forName(channelName)
        .build()
      val client = clientAuthentication(HelloServiceGrpc.stub(channel))
      client.hello(Hello.Request("hi")).futureValue.msg shouldBe "hello back"
    }

    "succeed request if client does not need authentication" in {
      val clientAuthentication =
        new SequencerClientNoAuthentication(domainId, unauthenticatedMemberId)
      channel = InProcessChannelBuilder
        .forName(channelName)
        .build()
      val client = clientAuthentication(HelloServiceGrpc.stub(channel))
      client.hello(Hello.Request("hi")).futureValue.msg shouldBe "hello back"
    }

    "fail request if participant use interceptor with incorrect token information" in {
      val clientAuthentication =
        SequencerClientTokenAuthentication(
          domainId,
          participantId,
          () => EitherT.pure[Future, Status](incorrectToken),
          isClosed = false,
          AuthenticationTokenManagerConfig(),
          AuthenticationTokenManagerTest.mockClock,
          loggerFactory,
        )
      channel = InProcessChannelBuilder
        .forName(channelName)
        .build()
      val client = clientAuthentication(HelloServiceGrpc.stub(channel))

      inside(client.hello(Hello.Request("hi")).failed.futureValue) {
        case status: io.grpc.StatusRuntimeException =>
          status.getStatus.getCode shouldBe io.grpc.Status.UNAUTHENTICATED.getCode
      }
    }
  }
}
