// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service.channel

import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.api.v30.{
  ConnectToSequencerChannelRequest,
  ConnectToSequencerChannelResponse,
  PingRequest,
  PingResponse,
}
import com.digitalasset.canton.sequencing.authentication.AuthenticationToken
import com.digitalasset.canton.sequencing.channel
import com.digitalasset.canton.sequencing.channel.errors.SequencerChannelErrors
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelId,
  SequencerChannelMetadata,
}
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.synchronizer.sequencing.authentication.{
  MemberAuthenticationStore,
  StoredAuthenticationToken,
}
import com.digitalasset.canton.synchronizer.sequencing.service.AuthenticationCheck
import com.digitalasset.canton.time.{PositiveFiniteDuration, SimClock}
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.tracing.SerializableTraceContext
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{Context, Status, StatusException, StatusRuntimeException}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.wordspec.AnyWordSpec

class GrpcSequencerChannelServiceTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private class Env(authenticationCheck: AuthenticationCheck) extends AutoCloseable {
    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory = loggerFactory)
    val timeout = ProcessingTimeout()
    val member1 = ParticipantId("member-1")
    val member2 = ParticipantId("member-2")
    val channelId = SequencerChannelId.tryCreate("channel")
    val crypto = new SymbolicPureCrypto
    val authenticationStore = new MemberAuthenticationStore(PositiveInt.two, loggerFactory)
    val pool = new GrpcSequencerChannelPool(
      clock,
      authenticationStore,
      testedProtocolVersion,
      timeout,
      PositiveInt.one,
      PositiveFiniteDuration.tryOfMinutes(1),
      PositiveFiniteDuration.tryOfSeconds(10),
      loggerFactory,
    )

    val service = new GrpcSequencerChannelService(
      authenticationCheck,
      pool,
      timeout,
      loggerFactory,
    )

    def createToken(member: Member): StoredAuthenticationToken = {
      val token = StoredAuthenticationToken(
        member,
        clock.now.plusSeconds(30),
        AuthenticationToken.generate(crypto),
        member.fingerprint,
      )
      authenticationStore.saveToken(token)
      token
    }

    def openConnection(tokenO: Option[StoredAuthenticationToken] = None): (
        StreamObserver[ConnectToSequencerChannelRequest],
        ServerCallStreamObserver[ConnectToSequencerChannelResponse],
    ) = {
      val respObserver = mock[ServerCallStreamObserver[ConnectToSequencerChannelResponse]]
      val context = Context
        .current()
        .withValue(IdentityContextHelper.storedAuthenticationTokenContextKey, tokenO)
        .withValue(IdentityContextHelper.storedMemberContextKey, tokenO.map(_.member))
      (context.call(() => service.connectToSequencerChannel(respObserver)), respObserver)
    }

    override def close(): Unit = pool.close()
  }

  private def withEnv[A](
      authenticationCheck: AuthenticationCheck = AuthenticationCheck.Disabled
  )(test: Env => A): A =
    ResourceUtil
      .withResource(new Env(authenticationCheck))(test)

  "GrpcSequencerChannelService.connectToSequencerChannel" should {

    "disconnect only a revoked member's pending connections" in
      withEnv(AuthenticationCheck.AuthenticationToken) { env =>
        import env.*

        val (_, response1) = openConnection(Some(createToken(member1)))
        val (_, response2) = openConnection(Some(createToken(member2)))

        authenticationStore.invalidateMember(member1)
        service.disconnectMember(member1)

        verify(response1).onCompleted()
        verifyNoMoreInteractions(response1, response2)
        pool.numOfPendingConnections shouldBe 1
        pool.numOfAvailableSlots shouldBe 1
        clock.numberOfScheduledTasks shouldBe 1
      }

    "reject a revoked token for both new and pending connections" in
      withEnv(AuthenticationCheck.AuthenticationToken) { env =>
        import env.*

        val token = createToken(member1)
        val (request, response) = openConnection(Some(token))
        authenticationStore.invalidateMember(member1)

        loggerFactory.assertLogs(
          {
            val error = intercept[StatusException](openConnection(Some(token)))
            error.getStatus.getCode shouldBe Status.Code.PERMISSION_DENIED
          },
          _.warningMessage should include("Authentication token has expired or been revoked"),
        )
        pool.numOfPendingConnections shouldBe 1
        pool.numOfAvailableSlots shouldBe 1

        val metadata = SequencerChannelMetadata(channelId, member1, member2, testedProtocolVersion)
        loggerFactory.assertLogs(
          request.onNext(
            ConnectToSequencerChannelRequest(
              channel.ConnectToSequencerChannelRequest.Metadata(metadata).toProtoV30,
              Some(SerializableTraceContext(traceContext).toProtoV30),
            )
          ),
          _.warningMessage should include("Authentication token has expired or been revoked"),
        )

        verify(response).onError(
          argThat[StatusRuntimeException](_.getStatus.getCode == Status.Code.PERMISSION_DENIED)
        )
        verifyNoMoreInteractions(response)
        pool.size shouldBe 0
        pool.numOfPendingConnections shouldBe 0
        pool.numOfAvailableSlots shouldBe 2
        clock.numberOfScheduledTasks shouldBe 0
      }
  }

  "GrpcSequencerChannelService.ping" should {

    "return successful response" in withEnv() { env =>
      import env.*

      assert(pool.numOfAvailableSlots > 0)
      service.ping(PingRequest()).futureValue shouldBe PingResponse()
    }

    "fail with the PoolCapacityExhausted canton error when the pool is full" in withEnv() { env =>
      import env.*

      openConnection()
      openConnection()

      eventually(2.seconds) {
        pool.numOfAvailableSlots shouldBe 0
      }

      inside(service.ping(PingRequest()).failed.futureValue) { case err: StatusRuntimeException =>
        err.getStatus.getCode shouldBe Status.Code.ABORTED
        DecodedCantonError
          .fromStatusRuntimeException(err)
          .value
          .code
          .id shouldBe SequencerChannelErrors.PoolCapacityExhausted.id
      }
    }
  }

}
