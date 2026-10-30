// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.PlainTextP2PEndpoint
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.{Context, Metadata, ServerCall, ServerCallHandler, Status}
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec

class P2PAddAuthTokenHeaderGrpcServerInterceptorTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with MockitoSugar {

  private def withInterceptor(
      test: P2PAddAuthTokenHeaderGrpcServerInterceptor => Unit
  ): Unit = {
    val crypto =
      SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
    val interceptor = new P2PAddAuthTokenHeaderGrpcServerInterceptor(
      DefaultTestIdentities.physicalSynchronizerId,
      DefaultTestIdentities.sequencerId,
      SynchronizerCrypto(crypto, defaultStaticSynchronizerParameters),
      Seq(testedProtocolVersion),
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
