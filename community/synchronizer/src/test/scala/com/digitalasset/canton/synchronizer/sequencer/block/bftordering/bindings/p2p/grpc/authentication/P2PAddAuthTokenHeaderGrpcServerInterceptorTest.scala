// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Fingerprint, SynchronizerCrypto, SynchronizerCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.sequencer.api.v30.SequencerAuthentication.ChallengeResponse
import com.digitalasset.canton.sequencer.api.v31.{
  SequencerAuthenticateRequest,
  SequencerAuthenticateResponse,
  SequencerAuthenticationServiceGrpc,
  SequencerChallengeRequest,
  SequencerChallengeResponse,
}
import com.digitalasset.canton.sequencing.authentication.grpc.SequencerClientTokenAuthentication
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationToken,
  AuthenticationTokenManagerConfig,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.AuthenticationServices
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  AuthenticationInitialState,
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.{
  P2PGrpcBftOrderingService,
  P2PGrpcConnectionManager,
  P2PGrpcConnectionState,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  P2PConnectionManagementConfig,
  P2PTransportMetricsConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.memory.InMemoryP2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
  P2PConnectionEventListener,
}
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.{
  IdentityContextHelper,
  SequencerAuthenticationServerInterceptor,
}
import com.digitalasset.canton.synchronizer.sequencing.authentication.{
  MemberAuthenticationService,
  StoredAuthenticationToken,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.synchronizer.sequencing.service.GrpcSequencerAuthenticationService
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.stub.{ServerCalls, StreamObserver}
import io.grpc.{Context, Metadata, MethodDescriptor, ServerCall, ServerCallHandler, Status}
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.Executors
import scala.concurrent.Future

class P2PAddAuthTokenHeaderGrpcServerInterceptorTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with MockitoSugar {

  private def withInterceptor(
      test: P2PAddAuthTokenHeaderGrpcServerInterceptor => Unit
  ): Unit = withInterceptorAt(testedProtocolVersion)(test)

  private def withInterceptorAt(version: ProtocolVersion)(
      test: P2PAddAuthTokenHeaderGrpcServerInterceptor => Unit
  ): Unit = {
    val crypto =
      SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
    val interceptor = new P2PAddAuthTokenHeaderGrpcServerInterceptor(
      DefaultTestIdentities.physicalSynchronizerId.copy(protocolVersion = version),
      DefaultTestIdentities.sequencerId,
      SynchronizerCrypto(crypto, defaultStaticSynchronizerParameters),
      Seq(version),
      AuthenticationTokenManagerConfig(),
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )
    try test(interceptor)
    finally {
      interceptor.close()
      crypto.close()
    }
  }

  private def withMember(member: Option[Member])(test: => Unit): Unit =
    Context
      .current()
      .withValue(IdentityContextHelper.storedMemberContextKey, member)
      .run(() => test)

  private def malformedEndpointHeaders(): Metadata = {
    val headers = new Metadata()
    // Decoding this header would fail, so rejection also proves the endpoint was never parsed.
    headers.put(
      Metadata.Key.of(
        P2PAddEndpointHeaderGrpcClientInterceptor.ENDPOINT_METADATA_KEY.name(),
        Metadata.BINARY_BYTE_MARSHALLER,
      ),
      Array[Byte](0),
    )
    headers
  }

  "P2P reverse authentication" should {
    Seq(
      ("the callback verifier differs", 38, Status.UNAUTHENTICATED, false),
      ("the callback RPC fails", 38, Status.PERMISSION_DENIED, false),
      ("reverse authentication shuts down on PV37", 37, Status.UNAVAILABLE, true),
      ("reverse authentication shuts down on PV38", 38, Status.UNAVAILABLE, true),
    ).foreach { case (failure, pv, expectedStatus, shutdownBeforeOpener) =>
      s"abort receiver setup without poisoning an honest endpoint when $failure" in {
        implicit val metricsContext: MetricsContext = MetricsContext.Empty
        val version = ProtocolVersion.parseUnchecked(pv)
        val psid = DefaultTestIdentities.physicalSynchronizerId.copy(protocolVersion = version)
        val sequencer1 = DefaultTestIdentities.sequencerId
        val honestSequencer2 = DefaultTestIdentities.daSequencerId
        val maliciousSequencerM = com.digitalasset.canton.topology.SequencerId(
          com.digitalasset.canton.topology.UniqueIdentifier
            .tryCreate("M", DefaultTestIdentities.namespace)
        )
        val authenticateCalls = new java.util.concurrent.atomic.AtomicInteger()
        val honestAuthenticationService =
          new SequencerAuthenticationServiceGrpc.SequencerAuthenticationService {
            override def sequencerChallenge(
                request: SequencerChallengeRequest
            ): Future[SequencerChallengeResponse] =
              if (expectedStatus.getCode == Status.Code.UNAUTHENTICATED)
                Future.successful(
                  SequencerChallengeResponse(
                    challenge = Some(ChallengeResponse.defaultInstance),
                    verifier = honestSequencer2.toProtoPrimitive,
                  )
                )
              else Future.failed(expectedStatus.asRuntimeException())

            override def sequencerAuthenticate(
                request: SequencerAuthenticateRequest
            ): Future[SequencerAuthenticateResponse] = {
              authenticateCalls.incrementAndGet().discard
              Future.failed(Status.INTERNAL.asRuntimeException())
            }
          }
        val callbackServer = NettyServerBuilder
          .forPort(0)
          .addService(
            SequencerAuthenticationServiceGrpc.bindService(
              honestAuthenticationService,
              parallelExecutionContext,
            )
          )
          .build()
          .start()
        val honestEndpoint =
          PlainTextP2PEndpoint("127.0.0.1", Port.tryCreate(callbackServer.getPort))
        val thisNode = SequencerNodeId.toBftNodeId(sequencer1)
        val honestNode = SequencerNodeId.toBftNodeId(honestSequencer2)
        val attackerNode = SequencerNodeId.toBftNodeId(maliciousSequencerM)
        val state = new P2PGrpcConnectionState(thisNode, loggerFactory)
        val store = new InMemoryP2PEndpointsStore(Set(honestEndpoint))
        val executor = Executors.newSingleThreadExecutor()
        val inputModule = mock[ModuleRef[BftOrderingMessage]]
        val events = mock[P2PConnectionEventListener]
        val clock = new SimClock(loggerFactory = loggerFactory)
        val manager = new P2PGrpcConnectionManager(
          thisNode,
          P2PConnectionManagementConfig(),
          state,
          store,
          Some(
            AuthenticationInitialState(
              psid,
              sequencer1,
              AuthenticationServices(
                mock[SynchronizerCryptoClient],
                mock[MemberAuthenticationService],
                mock[GrpcSequencerAuthenticationService],
                mock[SequencerAuthenticationServerInterceptor],
              ),
              AuthenticationTokenManagerConfig(),
              None,
              clock,
            )
          ),
          None,
          events,
          inputModule,
          SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
          clock,
          P2PTransportMetricsConfig(enabled = false),
          executor,
          timeouts,
          loggerFactory,
        )
        try {
          state.associateP2PEndpointIdToBftNodeId(honestEndpoint.id, honestNode) shouldBe Right(())
          store
            .associate(honestEndpoint, honestNode)
            .futureUnlessShutdown()
            .futureValueUS shouldBe true
          val call = mock[ServerCall[BftOrderingMessage, BftOrderingMessage]]
          val marshaller = mock[MethodDescriptor.Marshaller[BftOrderingMessage]]
          when(call.getMethodDescriptor).thenReturn(
            MethodDescriptor
              .newBuilder[BftOrderingMessage, BftOrderingMessage]()
              .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName("test/receive")
              .setRequestMarshaller(marshaller)
              .setResponseMarshaller(marshaller)
              .build()
          )
          val service = new P2PGrpcBftOrderingService(
            (observer, tc) =>
              manager.createServerSidePeerReceiver(inputModule, observer)(metricsContext, tc),
            loggerFactory,
          )
          // Use gRPC's real response observer: sending the manager's opener invokes sendHeaders.
          val next =
            ServerCalls.asyncBidiStreamingCall((observer: StreamObserver[BftOrderingMessage]) =>
              service.receive(observer)
            )
          val token = AuthenticationToken.tryFromProtoPrimitive(
            ByteString.copyFrom(new Array[Byte](AuthenticationToken.length))
          )
          val tokenValidator = mock[MemberAuthenticationService]
          when(tokenValidator.validateToken(psid, maliciousSequencerM, token)).thenReturn(
            Right(
              StoredAuthenticationToken(
                maliciousSequencerM,
                CantonTimestamp.MaxValue,
                token,
                Fingerprint.tryFromString("test-key"),
              )
            )
          )
          val incomingAuth =
            new SequencerAuthenticationServerInterceptor(tokenValidator, loggerFactory)
          val headers =
            SequencerClientTokenAuthentication.authenticationMetadata(
              psid,
              maliciousSequencerM,
              token,
            )
          headers.put(
            P2PAddEndpointHeaderGrpcClientInterceptor.ENDPOINT_METADATA_KEY,
            honestEndpoint,
          )
          withInterceptorAt(version) { interceptor =>
            val afterChannelCreation: ServerCallHandler[BftOrderingMessage, BftOrderingMessage] =
              (wrappedCall, requestHeaders) => {
                if (shutdownBeforeOpener) interceptor.close()
                next.startCall(wrappedCall, requestHeaders)
              }
            def startCall(): Unit =
              incomingAuth
                .interceptCall[BftOrderingMessage, BftOrderingMessage](
                  call,
                  headers,
                  (authenticatedCall, requestHeaders) =>
                    interceptor
                      .interceptCall(authenticatedCall, requestHeaders, afterChannelCreation),
                )
                .discard

            if (shutdownBeforeOpener) startCall()
            else
              loggerFactory.suppressWarningsAndErrors {
                startCall()
              }
          }
          verify(call).close(
            argThat[Status](_.getCode == expectedStatus.getCode),
            any[Metadata],
          )
          verify(call, times(1)).close(any[Status], any[Metadata])
          verify(call, never).sendHeaders(any[Metadata])
          verify(call, never).sendMessage(any[BftOrderingMessage])
          authenticateCalls.get() shouldBe 0
          state.getBftNodeId(honestEndpoint.id) shouldBe Some(honestNode)
          state.isConnected(Right(attackerNode)) shouldBe false
          state.connections should contain only (Some(honestEndpoint.id) -> Some(honestNode))
          store.listEndpoints().futureUnlessShutdown().futureValueUS should contain only (
            honestEndpoint -> Some(honestNode)
          )
          verify(events, never).onNodeId(any[BftNodeId], any[Option[P2PEndpoint]])(
            any[TraceContext]
          )
          verifyNoInteractions(inputModule)
        } finally {
          manager.close()
          store.close()
          executor.shutdown()
          callbackServer.shutdownNow().discard
        }
      }
    }

    "reject receiver setup when the interceptor is shutting down" in withInterceptor {
      interceptor =>
        interceptor.close()
        val call = mock[ServerCall[String, String]]
        val next = mock[ServerCallHandler[String, String]]
        val headers = new Metadata()
        headers.put(
          P2PAddEndpointHeaderGrpcClientInterceptor.ENDPOINT_METADATA_KEY,
          PlainTextP2PEndpoint("127.0.0.1", Port.tryCreate(1)),
        )
        withMember(Some(DefaultTestIdentities.sequencerId)) {
          interceptor.interceptCall(call, headers, next)
        }
        verify(call).close(
          argThat[Status](_.getCode == Status.Code.UNAVAILABLE),
          any[Metadata],
        )
        verifyNoInteractions(next)
    }

    Seq(
      "participant" -> DefaultTestIdentities.participant1,
      "mediator" -> DefaultTestIdentities.mediatorId,
    ).foreach { case (role, member) =>
      s"reject an authenticated $role before accessing the callback endpoint" in withInterceptor {
        interceptor =>
          val call = mock[ServerCall[String, String]]
          val next = mock[ServerCallHandler[String, String]]
          val headers = malformedEndpointHeaders()

          withMember(Some(member)) {
            interceptor.interceptCall(call, headers, next)
          }

          verify(call).close(
            argThat[Status](_.getCode == Status.Code.PERMISSION_DENIED),
            argThat[Metadata](_.keys().isEmpty),
          )
          verifyNoInteractions(next)
      }
    }

    "reject a missing authenticated identity before accessing the callback endpoint" in withInterceptor {
      interceptor =>
        val call = mock[ServerCall[String, String]]
        val next = mock[ServerCallHandler[String, String]]
        val headers = malformedEndpointHeaders()

        withMember(None) {
          interceptor.interceptCall(call, headers, next)
        }

        verify(call).close(
          argThat[Status](_.getCode == Status.Code.UNAUTHENTICATED),
          argThat[Metadata](_.keys().isEmpty),
        )
        verifyNoInteractions(next)
    }

    "allow an authenticated sequencer to reach the callback path" in withInterceptor {
      interceptor =>
        val call = mock[ServerCall[String, String]]
        val endpoint = PlainTextP2PEndpoint("127.0.0.1", Port.tryCreate(1))
        val headers = new Metadata()
        headers.put(P2PAddEndpointHeaderGrpcClientInterceptor.ENDPOINT_METADATA_KEY, endpoint)
        val listener = new ServerCall.Listener[String] {}
        val next = new ServerCallHandler[String, String] {
          override def startCall(
              wrappedCall: ServerCall[String, String],
              requestHeaders: Metadata,
          ): ServerCall.Listener[String] = {
            requestHeaders shouldBe headers
            P2PAddAuthTokenHeaderGrpcServerInterceptor.peerEndpointContextKey.get() shouldBe Some(
              endpoint
            )
            wrappedCall.close(Status.OK, new Metadata())
            listener
          }
        }

        withMember(Some(DefaultTestIdentities.sequencerId)) {
          interceptor.interceptCall(call, headers, next)
        }

        // Only next.startCall closes with OK, so this also proves the handler was invoked.
        verify(call).close(eqTo(Status.OK), argThat[Metadata](_.keys().isEmpty))
    }
  }
}
