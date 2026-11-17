// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service.channel

import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.api.v30.{
  ConnectToSequencerChannelRequest,
  ConnectToSequencerChannelResponse,
  PingRequest,
  PingResponse,
}
import com.digitalasset.canton.sequencing.channel.errors.SequencerChannelErrors
import com.digitalasset.canton.synchronizer.sequencing.service.AuthenticationCheck
import com.digitalasset.canton.time.{PositiveFiniteDuration, SimClock}
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.wordspec.AnyWordSpec

class GrpcSequencerChannelServiceTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  class Env extends AutoCloseable {
    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory = loggerFactory)
    val timeout = ProcessingTimeout()
    val pool = new GrpcSequencerChannelPool(
      clock,
      testedProtocolVersion,
      timeout,
      PositiveInt.one,
      PositiveFiniteDuration.tryOfMinutes(1),
      PositiveFiniteDuration.tryOfSeconds(10),
      loggerFactory,
    )

    val service = new GrpcSequencerChannelService(
      AuthenticationCheck.Disabled,
      pool,
      timeout,
      loggerFactory,
    )

    def openConnection(): (
        StreamObserver[ConnectToSequencerChannelRequest],
        ServerCallStreamObserver[ConnectToSequencerChannelResponse],
    ) = {
      val respObserver = mock[ServerCallStreamObserver[ConnectToSequencerChannelResponse]]
      (service.connectToSequencerChannel(respObserver), respObserver)
    }

    override def close(): Unit = pool.close()
  }

  def withEnv[A](test: Env => A): A =
    ResourceUtil
      .withResource(new Env())(test)

  "GrpcSequencerChannelService.ping" should {

    "return successful response" in withEnv { env =>
      import env.*

      assert(pool.numOfAvailableSlots > 0)
      service.ping(PingRequest()).futureValue shouldBe PingResponse()
    }

    "fail with the PoolCapacityExhausted canton error when the pool is full" in withEnv { env =>
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
